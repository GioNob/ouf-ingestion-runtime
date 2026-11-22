#!/usr/bin/env python3
"""Real publisher/runtime processes; explicit laboratory Gateway and Semantic seam."""
import os,sys,json,time,secrets,pathlib,subprocess,threading,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlparse,parse_qs
root=pathlib.Path(__file__).resolve().parents[1]
onboarding=pathlib.Path(os.environ['ONBOARDING_ROOT']).resolve()
evidence=root/'target/r2a-evidence';evidence.mkdir(parents=True,exist_ok=True)
token=secrets.token_hex(32);tokenfile=evidence/'workload.token';tokenfile.write_text(token);tokenfile.chmod(0o600)
processes=[];logs=[];calls={'publications':0,'semantic':0};checks=[]
def check(name,condition):
 if not condition:raise AssertionError(name)
 checks.append(name)
def sql(query):
 return subprocess.check_output(['psql','-h','127.0.0.1','-U','ouf','-d','ouf','-At','-v','ON_ERROR_STOP=1','-c',query],text=True).strip()
def http(url,auth=False):
 headers={'Authorization':'Bearer '+token} if auth else {}
 with urllib.request.urlopen(urllib.request.Request(url,headers=headers),timeout=3) as r:return json.load(r)
def wait(predicate,seconds=90):
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
 def do_GET(self):
  if self.headers.get('Authorization')!='Bearer '+token:self.send_error(403);return
  path=urlparse(self.path)
  try:
   if path.path.startswith('/api/onboarding/v1/runtime/publications'):
    calls['publications']+=1
    request=urllib.request.Request('http://127.0.0.1:18131'+self.path,headers={'Authorization':'Bearer '+token})
    with urllib.request.urlopen(request,timeout=3) as r:body=r.read(2097153);status=r.status
   elif path.path=='/api/semantic/v1/references:resolve':
    calls['semantic']+=1;q=parse_qs(path.query)
    if q!={'semanticId':['core'],'revisionId':['00000000-0000-0000-0000-000000000001'],'publicationSetId':['00000000-0000-0000-0000-000000000002']}:self.send_error(404);return
    body=json.dumps({'semantic_id':'core','semantic_version':'1','revision_id':q['revisionId'][0],'publication_set_id':q['publicationSetId'][0],'status':'ACTIVE'}).encode();status=200
   else:self.send_error(404);return
   self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
  except urllib.error.HTTPError as error:self.send_error(error.code)
server=ThreadingHTTPServer(('127.0.0.1',18132),Gateway);threading.Thread(target=server.serve_forever,daemon=True).start()
try:
 env=dict(os.environ,OUF_PAIRWISE_TOKEN=token,OUF_ONB_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_ONB_DB_USER='ouf',OUF_ONB_DB_PASSWORD=os.environ['PGPASSWORD'])
 cp=str(onboarding/'target/test-classes')+':'+str(onboarding/'target/classes')+':'+(onboarding/'target/r2-classpath.txt').read_text().strip()
 launch('onboarding',['java','-cp',cp,'it.comune.trieste.ouf.pairwise.ActivationPublisherFixture','--server.port=18131','--ouf.runtime-publications.tenant-id=tenant-a'],env)
 wait(lambda:len(http('http://127.0.0.1:18131/api/onboarding/v1/runtime/publications',True)['items'])==2)
 try:http('http://127.0.0.1:18132/api/onboarding/v1/runtime/publications');raise AssertionError('anonymous feed allowed')
 except urllib.error.HTTPError as e:check('anonymous_gateway_denied',e.code==403)
 env.update(OUF_ING_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_ING_DB_USER='ouf',OUF_ING_DB_PASSWORD=os.environ['PGPASSWORD'])
 jar=next((root/'target').glob('ingestion-runtime-*.jar'))
 args=['java','-jar',str(jar),'--server.port=18133','--ouf.ingestion.activation.enabled=true','--ouf.ingestion.activation.gateway-url=http://127.0.0.1:18132','--ouf.ingestion.activation.token-file='+str(tokenfile),'--ouf.ingestion.activation.tenant-id=tenant-a','--ouf.ingestion.activation.poll-ms=500']
 runtime=launch('ingestion',args,env)
 wait(lambda:sql("select count(*) from ouf_ingestion.ing_run where source_id in ('r2a-file','r2a-pull') and state='RUNNING'")=='2')
 check('automatic_file_and_pull_preflight',True)
 check('managed_once_disabled',sql("select state||':'||interval_seconds from ouf_ingestion.ing_schedule where source_id='r2a-file'")=='DISABLED:0')
 check('versioned_pull_cadence',sql("select state||':'||interval_seconds||':'||(sync_profile->'operationalPolicy'->>'timeZone') from ouf_ingestion.ing_schedule where source_id='r2a-pull'")=='ACTIVE:60:Europe/Rome')
 check('published_checksum_pinned',sql("select count(*) from ouf_ingestion.runtime_configuration_snapshot s join ouf_ingestion.ing_run r using(run_id) where s.snapshot_json->'configuration'->'publishedBundle'->>'checksum'=r.bundle_checksum")=='2')
 check('managed_asset_profile_pinned',sql("select count(*) from ouf_ingestion.runtime_configuration_snapshot s join ouf_ingestion.ing_run r using(run_id) where r.source_id='r2a-file' and s.snapshot_json->'configuration'->'publishedBundle'->'extractionProfile'->'runtime'->>'stagingRef'='object://r2a/input.csv'")=='1')
 runtime.terminate();runtime.wait(timeout=20);processes.remove(runtime)
 launch('ingestion-restart',args,env);wait(lambda:http('http://127.0.0.1:18133/actuator/health')['status']=='UP');time.sleep(2)
 check('restart_no_duplicate_runs',sql("select count(*) from ouf_ingestion.ing_run where source_id in ('r2a-file','r2a-pull')")=='2')
 check('gateway_exact_preflight_used',calls['semantic']>=2 and calls['publications']>=4)
 (evidence/'summary.json').write_text(json.dumps({'status':'PASS','checks':checks,'gatewayCalls':calls,'publisherCommit':os.environ['R2A_PUBLISHER_SHA'],'runtimeCommit':os.environ.get('GITHUB_SHA'),'level':'TWO_REAL_JVM_PROCESSES_WITH_GATEWAY_SEMANTIC_IDENTITY_FIXTURES','scope':'R2a automatic run admission and immutable preflight; no source acquisition/DataLake/UDP ACK claim'},indent=2))
finally:
 for p in reversed(processes):
  p.terminate()
  try:p.wait(timeout=20)
  except subprocess.TimeoutExpired:p.kill();p.wait(timeout=5)
 for f in logs:f.close()
 server.shutdown();server.server_close();tokenfile.unlink(missing_ok=True)
