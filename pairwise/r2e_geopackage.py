#!/usr/bin/env python3
"""Four real owner processes; declared upstream, Gateway and identity fixtures."""
import os,json,time,secrets,pathlib,subprocess,threading,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlparse,parse_qs
root=pathlib.Path(__file__).resolve().parents[1]
onboarding=pathlib.Path(os.environ['ONBOARDING_ROOT']).resolve();udp=pathlib.Path(os.environ['UDP_ROOT']).resolve()
semantic=pathlib.Path(os.environ['SEMANTIC_ROOT']).resolve()
evidence=root/'target/r2e-evidence';evidence.mkdir(parents=True,exist_ok=True)
token=secrets.token_hex(32);human=secrets.token_hex(32);tokenfile=evidence/'workload.token';tokenfile.write_text(token);tokenfile.chmod(0o600)
processes=[];logs=[];checks=[];ack_allowed=True;calls={'handoff':0,'nondurableResponse':0,'duplicateAck':0,'semantic':0}
def check(name,condition):
 if not condition:raise AssertionError(name)
 checks.append(name)
def sql(query):return subprocess.check_output(['psql','-h','127.0.0.1','-U','ouf','-d','ouf','-At','-v','ON_ERROR_STOP=1','-c',query],text=True).strip()
def http(url,credential=token):
 with urllib.request.urlopen(urllib.request.Request(url,headers={'Authorization':'Bearer '+credential} if credential else {}),timeout=5) as r:return json.load(r)
def wait(predicate,seconds=120):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  if any(p.poll() is not None for p in processes):raise RuntimeError('Application process exited; inspect logs')
  try:
   if predicate():return
  except (OSError,ValueError,subprocess.CalledProcessError):pass
  time.sleep(.5)
 raise TimeoutError('Bounded process integration wait expired')
def launch(name,args,env):
 f=open(evidence/(name+'.log'),'w');logs.append(f);p=subprocess.Popen(args,cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT);processes.append(p);return p
class Gateway(BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def reply(self,status,body):
  self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def do_GET(self):self.route()
 def do_POST(self):self.route()
 def route(self):
  auth=self.headers.get('Authorization')
  if auth not in ('Bearer '+token,'Bearer '+human):self.send_error(403);return
  path=urlparse(self.path);size=int(self.headers.get('Content-Length','0'))
  if size>67_000_000:self.send_error(413);return
  data=self.rfile.read(size) if self.command=='POST' else None
  try:
   if path.path.startswith('/api/onboarding/v1/runtime/publications'):upstream='http://127.0.0.1:18131'+self.path
   elif path.path.startswith('/api/internal/v1/') or path.path.startswith('/api/udp/v1/'):upstream='http://127.0.0.1:18134'+self.path
   elif path.path=='/api/semantic/v1/references:resolve':
    calls['semantic']+=1;upstream='http://127.0.0.1:18135'+self.path
   elif path.path=='/internal/object-storage/v1/content':
    refs={'object://r2e/cameras.gpkg':os.environ['OUF_R2E_INPUT'],'object://r2e/cabinets.gpkg':os.environ['OUF_R2E_INPUT'],'object://r2e/cameras-reloaded.gpkg':os.environ['OUF_R2E_INPUT_RELOADED']}
    ref=parse_qs(path.query).get('ref',[''])[0]
    if auth!='Bearer '+token or ref not in refs:self.send_error(403);return
    self.reply(200,pathlib.Path(refs[ref]).read_bytes());return
   elif path.path=='/internal/sources/v1/fetch':
    if auth!='Bearer '+token or json.loads(data)['bindingRef']!='gateway://r2b/pull':self.send_error(403);return
    self.reply(200,json.dumps({'items':[{'id':'2','name':'Beta','secret':'classified-pull'}]}).encode());return
   else:self.send_error(404);return
   request=urllib.request.Request(upstream,data=data,method=self.command,headers={'Authorization':auth,'Content-Type':'application/json'})
   with urllib.request.urlopen(request,timeout=15) as r:body=r.read(2_097_153);status=r.status
   if path.path=='/api/internal/v1/handoffs' and self.command=='POST':
    calls['handoff']+=1;calls['duplicateAck']+=int(json.loads(body).get('duplicate',False))
    if not ack_allowed:status=202;calls['nondurableResponse']+=1
   self.reply(status,body)
  except urllib.error.HTTPError as error:self.reply(error.code,error.read(65536))
server=ThreadingHTTPServer(('127.0.0.1',18132),Gateway);threading.Thread(target=server.serve_forever,daemon=True).start()
try:
 env=dict(os.environ,OUF_PAIRWISE_TOKEN=token,OUF_PAIRWISE_HUMAN_TOKEN=human,OUF_ONB_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_ONB_DB_USER='ouf',OUF_ONB_DB_PASSWORD=os.environ['PGPASSWORD'],OUF_UDP_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_UDP_DB_USER='ouf',OUF_UDP_DB_PASSWORD=os.environ['PGPASSWORD'],SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE='3',SPRING_DATASOURCE_HIKARI_MINIMUM_IDLE='0')
 def classpath(path):return str(path/'target/test-classes')+':'+str(path/'target/classes')+':'+(path/'target/r2-classpath.txt').read_text().strip()
 env.update(OUF_SEM_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_SEM_DB_USER='ouf',OUF_SEM_DB_PASSWORD=os.environ['PGPASSWORD'],OUF_DISCOVERY_WORKER_ENABLED='false',OUF_R2C_SEMANTIC_RESULT=str(evidence/'semantic-publication.json'))
 launch('semantic',['java','-cp',classpath(semantic),'it.comune.trieste.ouf.pairwise.GovernedPublicationFixture','--server.port=18135'],env)
 wait(lambda:(evidence/'semantic-publication.json').exists())
 semantic_result=json.loads((evidence/'semantic-publication.json').read_text());checks.extend(semantic_result['checks'])
 binding=semantic_result['binding']
 exact='http://127.0.0.1:18132/api/semantic/v1/references:resolve?'+urllib.parse.urlencode({k:binding[k] for k in ('semanticId','revisionId','publicationSetId')})
 check('real_registry_reference_active',http(exact)['status']=='ACTIVE')
 launch('onboarding',['java','-cp',classpath(onboarding),'it.comune.trieste.ouf.pairwise.ServingPublisherFixture','--server.port=18131','--ouf.runtime-publications.tenant-id=tenant-a'],env)
 wait(lambda:len(http('http://127.0.0.1:18131/api/onboarding/v1/runtime/publications')['items'])==1)
 udp_args=['java','-cp',classpath(udp),'it.comune.trieste.ouf.pairwise.ServingConsumerFixture','--server.port=18134','--ouf.udp.execution.enabled=true','--ouf.udp.execution.gateway-url=http://127.0.0.1:18132','--ouf.udp.execution.token-file='+str(tokenfile),'--ouf.udp.lake.tenant-id=tenant-a','--ouf.udp.lake.required=true','--ouf.udp.lake.raw-retention-days=30','--ouf.udp.lake.raw-retention-class=OPERATIONAL','--ouf.udp.lake.raw-access-label=RESTRICTED','--ouf.udp.lake.s3.bucket=r2b-lake','--ouf.udp.lake.s3.endpoint=http://127.0.0.1:9000','--ouf.udp.lake.s3.path-style=true']
 consumer=launch('udp',udp_args,env);wait(lambda:http('http://127.0.0.1:18134/actuator/health')['status']=='UP')
 env.update(OUF_ING_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_ING_DB_USER='ouf',OUF_ING_DB_PASSWORD=os.environ['PGPASSWORD'])
 args=['java','-jar',str(next((root/'target').glob('ingestion-runtime-*.jar'))),'--server.port=18133','--ouf.ingestion.activation.enabled=true','--ouf.ingestion.execution.enabled=true','--ouf.ingestion.activation.gateway-url=http://127.0.0.1:18132','--ouf.ingestion.activation.token-file='+str(tokenfile),'--ouf.ingestion.activation.tenant-id=tenant-a','--ouf.ingestion.activation.poll-ms=500']
 runtime=launch('ingestion',args,env)
 wait(lambda:sql("select count(*) from ouf_ingestion.ing_run where state='SUCCEEDED'")=='1',180)
 wait(lambda:sql("select count(*) from ouf_udp.urban_object_current_state")=='2')
 check('one_object_per_camera_feature',sql("select count(*) from ouf_udp.urban_object where canonical_type='https://example.org/Camera'")=='2')
 camera=sql("select urban_object_id from ouf_udp.urban_object where canonical_key='cam-1'")
 if not camera:camera=sql("select urban_object_id from ouf_udp.urban_object where canonical_key='CAM-1'")
 check('camera_identity_exists',bool(camera))
 check('unresolved_reference_is_quarantined',sql("select count(*) from ouf_udp.relationship_issue where state='OPEN' and reason_code='NO_MATCH'")=='1')
 def post(path):
  request=urllib.request.Request('http://127.0.0.1:18131/fixture/r2e/'+path,data=b'',method='POST',headers={'Authorization':'Bearer '+human})
  with urllib.request.urlopen(request,timeout=20) as r:return json.load(r)
 post('cabinets')
 wait(lambda:sql("select count(*) from ouf_udp.urban_relationship where status='ACTIVE'")=='1',120)
 check('late_cabinet_reconciled',True)
 relation=http('http://127.0.0.1:18132/api/udp/v1/objects/'+camera+'/relationships',human)
 check('human_sees_governed_relation','https://example.org/connectedTo' in json.dumps(relation))
 cabinet=sql("select urban_object_id from ouf_udp.urban_object where canonical_type='https://example.org/Cabinet'")
 incoming=http('http://127.0.0.1:18132/api/udp/v1/objects/'+cabinet+'/relationships?direction=INBOUND',human)
 check('cabinet_navigates_back_to_camera',camera in json.dumps(incoming))
 before=http('http://127.0.0.1:18132/api/udp/v1/objects/'+camera,human)
 check('original_and_canonical_geometry_available','originalGeometry' in before and 'canonicalGeometry' in before)
 old_publication=http('http://127.0.0.1:18131/api/onboarding/v1/runtime/publications/r2e-cameras/active')
 old_ids=sql("select string_agg(urban_object_id::text,',' order by urban_object_id) from ouf_udp.urban_object where canonical_type='https://example.org/Camera'")
 post('reload')
 wait(lambda:sql("select count(*) from ouf_ingestion.ing_run where source_id='r2e-cameras' and state='SUCCEEDED'")=='2',180)
 wait(lambda:sql("select count(*) from ouf_udp.relationship_issue where state='OPEN' and reason_code='NO_MATCH'")=='1')
 wait(lambda:sql("select count(*) from ouf_udp.urban_relationship where status='ACTIVE'")=='0')
 check('reload_does_not_duplicate_feature_objects',sql("select string_agg(urban_object_id::text,',' order by urban_object_id) from ouf_udp.urban_object where canonical_type='https://example.org/Camera'")==old_ids)
 check('changed_reference_retires_old_edge',True)
 after=http('http://127.0.0.1:18132/api/udp/v1/objects/'+camera,human)
 check('updated_geometry_visible','13.79' in json.dumps(after))
 check('old_geometry_has_history',int(sql("select count(*) from ouf_udp.urban_geometry where urban_object_id='"+camera+"'"))>=2)
 old=old_publication['bundle'];ref=old['bundleId']+':'+old['bundleVersion']+':'+old['checksum']
 check('original_profile_still_resolves',http('http://127.0.0.1:18131/api/onboarding/v1/runtime/publications/resolve?'+urllib.parse.urlencode({'bundleRef':ref}))['bundle']==old)
 lineage=http('http://127.0.0.1:18132/api/udp/v1/objects/'+camera+'/lineage',human)
 check('lineage_pins_original_contracts','contractRefs' in json.dumps(lineage))
 try:http('http://127.0.0.1:18132/api/udp/v1/objects/'+camera,token);raise AssertionError('writer implicitly reads')
 except urllib.error.HTTPError as e:check('workload_write_does_not_grant_serving',e.code==403)
 try:http('http://127.0.0.1:18132/api/udp/v1/objects/'+camera,'');raise AssertionError('anonymous read allowed')
 except urllib.error.HTTPError as e:check('anonymous_serving_denied',e.code==403)
 count=sql('select count(*) from ouf_udp.relationship_revision')
 consumer.kill();consumer.wait(timeout=10);processes.remove(consumer);launch('udp-restart',udp_args,env)
 wait(lambda:http('http://127.0.0.1:18134/actuator/health')['status']=='UP')
 check('restart_preserves_history',sql('select count(*) from ouf_udp.relationship_revision')==count)
 check('raw_feature_references_verified',sql("select count(*) from ouf_udp.handoff_intake h join ouf_udp.lake_object l on l.lake_object_id=h.source_raw_lake_object_id where l.state='VERIFIED'")=='5')
 (evidence/'human-scenario.json').write_text(json.dumps({'cameraBefore':before,'relationBefore':relation,'cameraAfter':after,'lineage':lineage},indent=2))
 (evidence/'summary.json').write_text(json.dumps({'status':'PASS','checks':checks,'calls':calls,'publisherCommit':os.environ['R2B_PUBLISHER_SHA'],'udpCommit':os.environ['R2B_UDP_SHA'],'runtimeCommit':os.environ.get('GITHUB_SHA'),'semanticCommit':os.environ['R2C_SEMANTIC_SHA'],'level':'FOUR_REAL_OWNER_JVM_PROCESSES_POSTGRES_MINIO_WITH_DECLARED_GATEWAY_IDENTITY_FIXTURES','limitations':['2D simple features only; 10 MiB files and 10000 features per layer','No territorial IGM certification; R2d limits unchanged','Browser, production IAM and Gateway deployment remain separate acceptance gates']},indent=2))
finally:
 for p in reversed(processes):
  p.terminate()
  try:p.wait(timeout=20)
  except subprocess.TimeoutExpired:p.kill();p.wait(timeout=5)
 for f in logs:f.close()
 server.shutdown();server.server_close();tokenfile.unlink(missing_ok=True)
