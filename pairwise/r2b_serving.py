#!/usr/bin/env python3
"""Real Onboarding/Ingestion/UDP and MinIO; explicit Gateway, source, Semantic and identity seams."""
import os,json,time,secrets,pathlib,subprocess,threading,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlparse,parse_qs
root=pathlib.Path(__file__).resolve().parents[1]
onboarding=pathlib.Path(os.environ['ONBOARDING_ROOT']).resolve();udp=pathlib.Path(os.environ['UDP_ROOT']).resolve()
evidence=root/'target/r2b-evidence';evidence.mkdir(parents=True,exist_ok=True)
token=secrets.token_hex(32);human=secrets.token_hex(32);tokenfile=evidence/'workload.token';tokenfile.write_text(token);tokenfile.chmod(0o600)
processes=[];logs=[];checks=[];ack_allowed=False;calls={'handoff':0,'nondurableResponse':0,'duplicateAck':0,'semantic':0}
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
    q=parse_qs(path.query);calls['semantic']+=1
    if q!={'semanticId':['core'],'revisionId':['00000000-0000-0000-0000-000000000001'],'publicationSetId':['00000000-0000-0000-0000-000000000002']}:self.send_error(404);return
    self.reply(200,json.dumps({'semantic_id':'core','semantic_version':'1','revision_id':q['revisionId'][0],'publication_set_id':q['publicationSetId'][0],'status':'ACTIVE'}).encode());return
   elif path.path=='/internal/object-storage/v1/content':
    if auth!='Bearer '+token or parse_qs(path.query)!={'ref':['object://r2b/input.csv']}:self.send_error(403);return
    self.reply(200,b'id,name\n1,Alpha\n');return
   elif path.path=='/internal/sources/v1/fetch':
    if auth!='Bearer '+token or json.loads(data)['bindingRef']!='gateway://r2b/pull':self.send_error(403);return
    self.reply(200,json.dumps({'items':[{'id':'2','name':'Beta'}]}).encode());return
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
 launch('onboarding',['java','-cp',classpath(onboarding),'it.comune.trieste.ouf.pairwise.ServingPublisherFixture','--server.port=18131','--ouf.runtime-publications.tenant-id=tenant-a'],env)
 wait(lambda:len(http('http://127.0.0.1:18131/api/onboarding/v1/runtime/publications')['items'])==2)
 udp_args=['java','-cp',classpath(udp),'it.comune.trieste.ouf.pairwise.ServingConsumerFixture','--server.port=18134','--ouf.udp.execution.enabled=true','--ouf.udp.execution.gateway-url=http://127.0.0.1:18132','--ouf.udp.execution.token-file='+str(tokenfile),'--ouf.udp.lake.tenant-id=tenant-a','--ouf.udp.lake.required=true','--ouf.udp.lake.raw-retention-days=30','--ouf.udp.lake.raw-retention-class=OPERATIONAL','--ouf.udp.lake.raw-access-label=RESTRICTED','--ouf.udp.lake.s3.bucket=r2b-lake','--ouf.udp.lake.s3.endpoint=http://127.0.0.1:9000','--ouf.udp.lake.s3.path-style=true']
 consumer=launch('udp',udp_args,env);wait(lambda:http('http://127.0.0.1:18134/actuator/health')['status']=='UP')
 env.update(OUF_ING_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_ING_DB_USER='ouf',OUF_ING_DB_PASSWORD=os.environ['PGPASSWORD'])
 args=['java','-jar',str(next((root/'target').glob('ingestion-runtime-*.jar'))),'--server.port=18133','--ouf.ingestion.activation.enabled=true','--ouf.ingestion.execution.enabled=true','--ouf.ingestion.activation.gateway-url=http://127.0.0.1:18132','--ouf.ingestion.activation.token-file='+str(tokenfile),'--ouf.ingestion.activation.tenant-id=tenant-a','--ouf.ingestion.activation.poll-ms=500']
 runtime=launch('ingestion',args,env)
 wait(lambda:calls['nondurableResponse']>=1)
 check('http_202_does_not_commit_watermark',sql('select count(*) from ouf_ingestion.ing_watermark')=='0')
 check('udp_durable_before_lost_ack',int(sql('select count(*) from ouf_udp.handoff_intake'))>=1)
 runtime.kill();runtime.wait(timeout=10);processes.remove(runtime)
 ack_allowed=True;runtime=launch('ingestion-restart',args,env)
 wait(lambda:sql("select count(*) from ouf_ingestion.ing_run where source_id in ('r2b-file','r2b-pull') and state='SUCCEEDED'")=='2',180)
 check('file_and_pull_completed',True);check('duplicate_delivery_returns_same_ack',calls['duplicateAck']>=1)
 check('one_handoff_per_source_record',sql('select count(*) from ouf_udp.handoff_intake')=='2')
 check('committed_watermarks_after_durable_ack',sql('select count(*) from ouf_ingestion.ing_watermark')=='2')
 wait(lambda:sql("select count(*) from ouf_udp.urban_object_current_state")=='2')
 filepage=http('http://127.0.0.1:18132/api/udp/v1/objects?type=https%3A%2F%2Fexample.org%2FR2bFile',human)
 pullpage=http('http://127.0.0.1:18132/api/udp/v1/objects?type=https%3A%2F%2Fexample.org%2FR2bPull',human)
 check('authorized_file_read_contains_alpha','Alpha' in json.dumps(filepage));check('authorized_pull_read_contains_beta','Beta' in json.dumps(pullpage))
 objectid=sql("select urban_object_id from ouf_udp.urban_object where canonical_type='https://example.org/R2bFile'")
 lineage=http('http://127.0.0.1:18132/api/udp/v1/objects/'+objectid+'/lineage',human)
 check('human_can_verify_lineage',bool(lineage) and 'contractRefs' in json.dumps(lineage))
 try:http('http://127.0.0.1:18132/api/udp/v1/objects/'+objectid,'');raise AssertionError('anonymous read allowed')
 except urllib.error.HTTPError as e:check('anonymous_serving_denied',e.code==403)
 try:http('http://127.0.0.1:18132/api/udp/v1/objects/'+objectid,token);raise AssertionError('writer implicitly reads')
 except urllib.error.HTTPError as e:check('workload_write_does_not_grant_serving',e.code==403)
 check('raw_source_reference_retained',sql("select count(*) from ouf_udp.handoff_intake h join ouf_udp.lake_object l on l.lake_object_id=h.source_raw_lake_object_id where l.state='VERIFIED' and l.blocking_lineage_refs>0")=='2')
 check('all_three_lake_zones_verified',sql("select count(distinct tier) from ouf_udp.lake_object where state='VERIFIED' and tier in('RAW','NORMALIZED','CURATED')")=='3')
 before=sql('select count(*) from ouf_udp.object_revision')
 consumer.kill();consumer.wait(timeout=10);processes.remove(consumer);launch('udp-restart',udp_args,env)
 wait(lambda:http('http://127.0.0.1:18134/actuator/health')['status']=='UP')
 check('restart_preserves_serving','Alpha' in json.dumps(http('http://127.0.0.1:18132/api/udp/v1/objects/'+objectid,human)))
 check('restart_no_duplicate_revisions',sql('select count(*) from ouf_udp.object_revision')==before)
 (evidence/'human-scenario.json').write_text(json.dumps({'fileResult':filepage,'pullResult':pullpage,'fileLineage':lineage},indent=2))
 (evidence/'summary.json').write_text(json.dumps({'status':'PASS','checks':checks,'calls':calls,'publisherCommit':os.environ['R2B_PUBLISHER_SHA'],'udpCommit':os.environ['R2B_UDP_SHA'],'runtimeCommit':os.environ.get('GITHUB_SHA'),'level':'THREE_REAL_JVM_PROCESSES_POSTGRES_MINIO_WITH_DECLARED_GATEWAY_SOURCE_SEMANTIC_IDENTITY_FIXTURES'},indent=2))
finally:
 for p in reversed(processes):
  p.terminate()
  try:p.wait(timeout=20)
  except subprocess.TimeoutExpired:p.kill();p.wait(timeout=5)
 for f in logs:f.close()
 server.shutdown();server.server_close();tokenfile.unlink(missing_ok=True)
