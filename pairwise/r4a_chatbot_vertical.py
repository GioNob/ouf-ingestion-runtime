#!/usr/bin/env python3
"""R2c infrastructure reused with real MCP + four owner JVMs; explicit disposable IAM/Gateway/file-transport seams."""
import base64,csv,datetime,hashlib,hmac,io,json,os,pathlib,secrets,subprocess,threading,time,urllib.error,urllib.parse,urllib.request,uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
root=pathlib.Path(__file__).resolve().parents[1]
repos={k:pathlib.Path(os.environ[k+'_ROOT']).resolve() for k in ('ONBOARDING','SEMANTIC','UDP','MCP')}
evidence=root/'target/r4a-chatbot-evidence';evidence.mkdir(parents=True,exist_ok=True)
token,human,key=(secrets.token_hex(32) for _ in range(3));tokenfile=evidence/'workload.token';tokenfile.write_text(token);tokenfile.chmod(0o600)
keyfile=evidence/'owner-receipt.key';keyfile.write_text(key);keyfile.chmod(0o600)
processes=[];logs=[];checks=[];calls={};captures=[]
proposal=json.loads((repos['SEMANTIC']/'src/test/resources/teatri-reuse-semantic-proposal-20261014.json').read_text())
fields=proposal['mappingBlueprint']['fields']

def check(name,condition):
 if not condition:raise AssertionError(name)
 checks.append(name)
def compact(value):return json.dumps(value,separators=(',',':'),ensure_ascii=False).encode()
def sql(query):return subprocess.check_output(['psql','-h','127.0.0.1','-U','ouf','-d','ouf','-At','-v','ON_ERROR_STOP=1','-c',query],text=True).strip()
def request(url,body=None,credential=token,extra=None):
 headers={'Accept':'application/json','Authorization':'Bearer '+credential,'Content-Type':'application/json'}
 if extra:headers.update(extra)
 req=urllib.request.Request(url,data=None if body is None else compact(body),headers=headers)
 try:
  with urllib.request.urlopen(req,timeout=30) as response:
   raw=response.read(2_097_153)
   if not raw:return None
   if response.headers.get('Content-Type','').startswith('text/event-stream'):
    events=[x[6:] for x in raw.decode().splitlines() if x.startswith('data: ')];return json.loads(events[-1])
   return json.loads(raw)
 except urllib.error.HTTPError as e:
  diagnostic=e.read(4096).decode(errors='replace');raise RuntimeError('HTTP '+str(e.code)+' '+urllib.parse.urlparse(url).path+' '+diagnostic) from e

def wait(predicate,seconds=120):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  if any(p.poll() is not None for p in processes):raise RuntimeError('Application process exited; inspect bounded logs')
  try:
   if predicate():return
  except (OSError,ValueError,RuntimeError,subprocess.CalledProcessError):pass
  time.sleep(.5)
 raise TimeoutError('Bounded integration wait expired')
def launch(name,args,env):
 log=open(evidence/(name+'.log'),'w');logs.append(log)
 p=subprocess.Popen(args,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT);processes.append(p);return p

def classpath(path):return str(path/'target/test-classes')+':'+str(path/'target/classes')+':'+(path/'target/r2-classpath.txt').read_text().strip()
def timestamp(delta=0):return (datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=delta)).isoformat().replace('+00:00','Z')
capabilities={'ouf.managed-source.onboarding.create':'COMMAND','ouf.managed-source.onboarding.review.prepare':'COMMAND','ouf.managed-source.onboarding.read':'READ','urban.object.search':'SEARCH'}
bundle={'bundleId':'vertical-mcp-fixture','version':1,'publishedAt':timestamp(),'capabilities':[{'capabilityId':cap,'operation':operation,'requiredScope':cap,'allowedActors':['HUMAN']} for cap,operation in capabilities.items()],'grants':[{'grantId':'fixture:'+cap,'capabilityId':cap,'tenantId':'tenant-a','subjectId':'fixture-human','validFrom':timestamp(-60),'validUntil':timestamp(3600)} for cap in capabilities]}
policy={'contentHash':hashlib.sha256(compact(bundle)).hexdigest(),'bundleId':bundle['bundleId'],'bundleVersion':1,'activatedAt':timestamp(),'bundle':bundle}

def receipt(raw,path,cap,idempotency):
 now=int(time.time());item={'v':1,'purpose':'managed-file-owner','method':'POST','path':path,'bodyHash':hashlib.sha256(raw).hexdigest(),'capability':cap,'iat':now,'exp':now+25,'issuer':'fixture-issuer','audience':'fixture-audience','workload':'ouf-mcp-server','subject':'fixture-human','tenant':'tenant-a','acr':'fixture-authentication','roles':'','scope':cap,'idempotencyKey':idempotency}
 encoded=base64.urlsafe_b64encode(compact(item)).rstrip(b'=')
 mac=hmac.new(key.encode(),b'ouf-managed-file-owner-v1.'+encoded,hashlib.sha256).digest()
 return (encoded+b'.'+base64.urlsafe_b64encode(mac).rstrip(b'=')).decode()
class Gateway(BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def reply(self,status,body,content='application/json'):
  self.send_response(status);self.send_header('Content-Type',content);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def do_GET(self):self.route()
 def do_POST(self):self.route()
 def route(self):
  auth=self.headers.get('Authorization');path=urllib.parse.urlparse(self.path);size=int(self.headers.get('Content-Length','0'))
  if auth not in ('Bearer '+token,'Bearer '+human):self.send_error(403);return
  if size>67_000_000:self.send_error(413);return
  raw=self.rfile.read(size) if self.command=='POST' else None;headers={'Authorization':auth,'Content-Type':'application/json'}
  try:
   if path.path=='/fixture/policy':self.reply(200,compact(policy));return
   if path.path=='/internal/object-storage/v1/content':
    ref=urllib.parse.parse_qs(path.query).get('ref',[None])[0]
    if ref not in ('object://r4a/baseline.csv','object://r4a/incoming.csv'):self.send_error(404);return
    self.reply(200,(evidence/(ref.rsplit('/',1)[-1])).read_bytes(),'text/csv');return
   if path.path.startswith('/api/onboarding/v1/') or path.path.startswith('/api/internal/v1/onboarding/') or path.path.startswith('/api/trusted-human/v1/approval-challenges'):
    upstream='http://127.0.0.1:18131'+self.path
   elif path.path.startswith('/api/udp/v1/') or path.path.startswith('/api/internal/v1/'):
    upstream='http://127.0.0.1:18134'+self.path
   elif path.path.startswith('/api/semantic/v1/'):
    upstream='http://127.0.0.1:18135'+self.path
   elif path.path.startswith('/internal/capabilities/v1/execute/'):
    if auth!='Bearer '+token or self.headers.get('X-OUF-Delegation')!='fixture-human-delegation':self.send_error(403);return
    envelope=json.loads(raw);cap=envelope['CapabilityID'];calls[cap]=calls.get(cap,0)+1
    check_scope=envelope['Identity'];
    if check_scope.get('PrincipalID')!='fixture-human' or check_scope.get('TenantID')!='tenant-a' or check_scope.get('ActorType')!='HUMAN':self.send_error(403);return
    if cap=='urban.object.search':
     upstream='http://127.0.0.1:18134/api/udp/v1/objects/search';raw=compact(envelope['Arguments']);headers['Authorization']='Bearer '+human
    else:
     modes={'ouf.managed-source.onboarding.create':'create','ouf.managed-source.onboarding.review.prepare':'review-prepare','ouf.managed-source.onboarding.read':'review-read'}
     if cap not in modes:self.send_error(404);return
     owner='/api/internal/v1/onboarding/managed-file-mcp/'+modes[cap]
     if not path.path.endswith('/managed.file/'+modes[cap]):self.send_error(400);return
     upstream='http://127.0.0.1:18131'+owner
     headers['X-OUF-Managed-File-Receipt']=receipt(raw,owner,cap,envelope['IdempotencyKey']);headers['Authorization']='Bearer '+human
   else:self.send_error(404);return
   req=urllib.request.Request(upstream,data=raw,method=self.command,headers=headers)
   with urllib.request.urlopen(req,timeout=25) as response:self.reply(response.status,response.read(2_097_153))
  except urllib.error.HTTPError as e:self.reply(e.code,e.read(65536))
  except Exception as e:self.reply(502,compact({'code':'FIXTURE_GATEWAY_FAILURE','detail':str(e)[:512]}))
server=ThreadingHTTPServer(('127.0.0.1',18132),Gateway);threading.Thread(target=server.serve_forever,daemon=True).start()

def tool(name,args):
 headers={'Mcp-Protocol-Version':'2026-07-28','Mcp-Method':'tools/call','Accept':'application/json, text/event-stream','X-OUF-Gateway-Verified':'true','X-OUF-Delegation':'fixture-human-delegation','X-OUF-Service-Principal':'ouf-chatgpt','X-OUF-Principal-ID':'fixture-human','X-OUF-Tenant-ID':'tenant-a','X-OUF-Actor-Type':'HUMAN','X-OUF-Authentication-Context-Ref':'1','X-OUF-Token-Issuer':'fixture-issuer','X-OUF-Token-Audience':'fixture-audience','X-OUF-Granted-Scopes':' '.join(capabilities),'X-Correlation-ID':str(uuid.uuid4()),'Idempotency-Key':'fixture:'+str(uuid.uuid4())}
 result=request('http://127.0.0.1:18136/mcp',{'jsonrpc':'2.0','id':str(uuid.uuid4()),'method':'tools/call','params':{'name':name,'arguments':args}},human,headers)
 if 'error' in result:raise RuntimeError('MCP '+name+' '+json.dumps(result['error']))
 content=result['result']
 if content.get('isError'):raise RuntimeError('MCP '+name+' '+json.dumps(content))
 if 'structuredContent' in content:out=content['structuredContent']
 else:out=json.loads(next(item['text'] for item in content['content'] if item['type']=='text'))
 captures.append({'tool':name,'result':out});return out

def csv_bytes(rows,exclude=()):
 output=io.StringIO(newline='');writer=csv.DictWriter(output,fieldnames=[f['fieldName'] for f in fields if f['fieldName'] not in exclude]);writer.writeheader();writer.writerows([{k:v for k,v in row.items() if k not in exclude} for row in rows]);return output.getvalue().encode()
def row(name,note='reviewed extension',distinct=False):
 prefix='second-' if distinct else 'fixture-'
 data={f['fieldName']:prefix+f['fieldName'] for f in fields};data.update(nome_teatro=name,cap='00234' if distinct else '00123',toponimo='Piazza' if distinct else 'Via',nome_indirizzo='Secondo' if distinct else 'Teatro',civico='2' if distinct else '1',citta='Other City' if distinct else 'Fixture City',provincia='YY' if distinct else 'ZZ',note=note);return data
try:
 check('twenty_field_semantic_proposal_loaded',len(fields)==20)
 (evidence/'baseline.csv').write_bytes(csv_bytes([row('Fixture Teatro')],exclude=('note',)))
 (evidence/'incoming.csv').write_bytes(csv_bytes([row('Fixture Teatro'),row('Fixture Secondo','distinct fixture',True)]))
 env=dict(os.environ,OUF_PAIRWISE_TOKEN=token,OUF_PAIRWISE_HUMAN_TOKEN=human,SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE='3',SPRING_DATASOURCE_HIKARI_MINIMUM_IDLE='0')
 for prefix in ('ONB','SEM','UDP','ING'):env.update({f'OUF_{prefix}_DB_URL':'jdbc:postgresql://127.0.0.1:5432/ouf',f'OUF_{prefix}_DB_USER':'ouf',f'OUF_{prefix}_DB_PASSWORD':os.environ['PGPASSWORD']})
 env.update(OUF_DISCOVERY_WORKER_ENABLED='false',OUF_R2C_SEMANTIC_RESULT=str(evidence/'semantic-publication.json'),OUF_R4A_IMPORT_PROPOSAL=str(repos['SEMANTIC']/'src/test/resources/teatri-reuse-semantic-proposal-20261014.json'),OUF_R4A_INPUT_DIR=str(evidence),OUF_R4A_ASSETS_RESULT=str(evidence/'assets.json'))
 launch('semantic',['java','-cp',classpath(repos['SEMANTIC']),'it.comune.trieste.ouf.pairwise.GovernedPublicationFixture','--server.port=18135'],env)
 wait(lambda:(evidence/'semantic-publication.json').exists());semantic=json.loads((evidence/'semantic-publication.json').read_text());binding=semantic['binding'];checks.extend(semantic['checks'])
 launch('onboarding',['java','-cp',classpath(repos['ONBOARDING']),'it.comune.trieste.ouf.pairwise.ChatbotVerticalPublisherFixture','--server.port=18131','--ouf.runtime-publications.tenant-id=tenant-a','--ouf.authorization.ths.public-origin=https://ths.fixture.invalid','--ouf.authorization.delegation.key-file='+str(keyfile),'--ouf.iam.issuer=fixture-issuer','--ouf.iam.audience=fixture-audience','--ouf.onboarding.udp-identity.gateway-url=http://127.0.0.1:18132','--ouf.onboarding.udp-identity.token-file='+str(tokenfile)],env)
 wait(lambda:(evidence/'assets.json').exists());assets=json.loads((evidence/'assets.json').read_text())['assets']
 launch('udp',['java','-cp',classpath(repos['UDP']),'it.comune.trieste.ouf.pairwise.ServingConsumerFixture','--server.port=18134','--ouf.udp.execution.enabled=true','--ouf.udp.execution.gateway-url=http://127.0.0.1:18132','--ouf.udp.execution.token-file='+str(tokenfile),'--ouf.udp.lake.tenant-id=tenant-a','--ouf.udp.lake.required=true','--ouf.udp.lake.raw-retention-days=30','--ouf.udp.lake.raw-retention-class=OPERATIONAL','--ouf.udp.lake.raw-access-label=RESTRICTED','--ouf.udp.lake.s3.bucket=r2b-lake','--ouf.udp.lake.s3.endpoint=http://127.0.0.1:9000','--ouf.udp.lake.s3.path-style=true'],env)
 wait(lambda:request('http://127.0.0.1:18134/actuator/health')['status']=='UP')
 mcp_env=dict(env,MCP_DATABASE_URL='postgres://ouf:'+os.environ['PGPASSWORD']+'@127.0.0.1:5432/ouf?sslmode=disable',MCP_WORKLOAD_TOKEN=token,MCP_FINGERPRINT_KEY=secrets.token_hex(32),MCP_AUTHORIZATION_BUNDLE_ENDPOINT='http://127.0.0.1:18132/fixture/policy',MCP_GATEWAY_ENDPOINT='http://127.0.0.1:18132/internal/capabilities/v1/execute')
 mcp_bin=str(repos['MCP']/'build/ouf-mcp');subprocess.run([mcp_bin,'migrate'],env=mcp_env,check=True,timeout=60)
 launch('mcp',[mcp_bin,'server','-http','127.0.0.1:18136'],mcp_env)
 wait(lambda:request('http://127.0.0.1:18136/health/ready') is None)
 jar=str(next((root/'target').glob('ingestion-runtime-*.jar')))
 launch('ingestion',['java','-jar',jar,'--server.port=18133','--ouf.ingestion.activation.enabled=true','--ouf.ingestion.execution.enabled=true','--ouf.ingestion.activation.gateway-url=http://127.0.0.1:18132','--ouf.ingestion.activation.token-file='+str(tokenfile),'--ouf.ingestion.activation.tenant-id=tenant-a','--ouf.ingestion.activation.poll-ms=500'],env)
 probe_props=evidence/'probe.properties';probe_props.write_text('ouf.ingestion.activation.gateway-url=http://127.0.0.1:18132\nouf.ingestion.activation.token-file='+str(tokenfile)+'\nouf.ingestion.activation.tenant-id=tenant-a\n')
 class_iri='https://schema.org/PerformingArtsTheater';public_props={f['fieldName']:f['targetPropertyIri'] for f in fields}
 def onboard(name,rows):
  source='r4a-'+name;asset=assets[name];selected=[f for f in fields if name!='baseline' or f['fieldName']!='note']
  decisions=[{'fieldName':f['fieldName'],'targetPropertyIri':f['targetPropertyIri'],'extractionDecision':'INCLUDE','dataAccessLabel':'OPEN' if f['fieldName'] in ('nome_teatro','cap','note') else 'RESTRICTED','transform':'IDENTITY'} for f in selected]
  draft=tool('source.onboarding.create',{'assetId':str(asset['assetId']),'profileId':str(asset['profileId']),'sourceId':source,'name':'Disposable fixture '+name,'owner':'fixture-owner','targetClassIri':class_iri,'semanticRefs':[binding['semanticId']+'@'+binding['semanticVersion']],'sourceObjectKeyFields':['nome_teatro'],'fields':decisions})
  version=str(draft['onboardingVersionId']);check(name+'_mcp_created_only_draft',draft['state']=='DRAFT')
  draft_state=tool('source.onboarding.read',{'sourceId':source,'onboardingVersionId':version})
  policy_ref='urn:fixture:identity:'+source
  udp={'resolution':{'strategyId':'GOVERNED_IDENTITY','strategyVersion':'1','policyRef':policy_ref,'governedIdentity':{'ref':policy_ref,'version':'1','tenantId':'tenant-a','canonicalClass':class_iri,'sourceId':source,'maxCandidates':100,'allowAutoNew':True,'signals':[{'id':f['targetPropertyIri'],'semanticRef':f['targetPropertyIri']+'@'+binding['publicationSetId'],'comparator':'TEXT_V1','excludesOnDisagreement':False,'uniqueWithinScope':False,'assertionRef':'urn:fixture:explicit-human-comparison'} for f in selected],'sufficientRules':[{'id':'fixture-name-address','signalIds':[public_props[n] for n in ('nome_teatro','toponimo','nome_indirizzo','civico','cap','citta')],'assertionRef':'urn:fixture:explicit-human-rule'}]}},'materialization':{'policyRef':'urn:fixture:authority-order-v1','properties':[{'sourceField':f['targetPropertyIri'],'propertyIri':f['targetPropertyIri'],'datatype':'http://www.w3.org/2001/XMLSchema#string','accessLabel':f['dataAccessLabel'],'authorityOrder':['r4a-incoming','r4a-baseline']} for f in decisions],'bitemporalProperties':[],'checkpointInterval':1}}
  review_args={'sourceId':source,'onboardingVersionId':version,'expectedVersion':draft_state['lockVersion'],'commandKey':'review-'+source,'executionProposal':{'semanticBindings':[binding],'semanticPublicationSetRef':binding['publicationSetId'],'udp':udp,'limits':{'maxRows':100,'maxColumns':20,'maxCellChars':4096}}}
  review=tool('source.onboarding.review.prepare',review_args)
  check(name+'_review_frozen_without_activation',review['state']=='IN_REVIEW' and review['humanApprovalRequired'])
  current=request('http://127.0.0.1:18132/api/onboarding/v1/sources/'+source+'/onboarding-versions/'+version,credential=human)
  candidate={'sourceId':source,'onboardingVersionId':version,'configurationHash':current['configuration_hash'],'configuration':current['configuration'],'state':current['state']}
  proof=subprocess.run(['java','-Dloader.main=it.comune.trieste.ouf.ingestion.FrozenConfigurationProbeMain','-cp',jar,'org.springframework.boot.loader.launch.PropertiesLauncher','--properties',str(probe_props)],input=compact(candidate),stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,timeout=45)
  (evidence/(name+'-probe.json')).write_bytes(proof.stdout);check(name+'_real_frozen_probe_exit',proof.returncode==0)
  observed=json.loads(proof.stdout);check(name+'_all_rows_validated_without_attestation',observed['status']=='PASS' and observed['validatedRows']==rows and not observed['attestationSubmitted'])
  request('http://127.0.0.1:18132/api/internal/v1/onboarding/compatibility/ingestion-runtime',{'sourceId':source,'onboardingVersionId':version,'compatible':True,'detail':'Disposable SERVICE submission after real all-row probe hash '+observed['configurationHash']})
  preflight=request('http://127.0.0.1:18132/api/udp/v1/governance/identity/preflight',{'sourceId':source,'configurationHash':current['configuration_hash'],'configuration':current['configuration']},human)
  check(name+'_real_identity_coverage',preflight['valid'] and preflight['indexedObjects']==(0 if name=='baseline' else 1))
  review=tool('source.onboarding.read',{'sourceId':source,'onboardingVersionId':version})
  challenge=review['challengeId'];action='http://127.0.0.1:18132/api/trusted-human/v1/approval-challenges/'+challenge+'/review/'
  confirmed=request(action+'confirm',{'configurationHash':review['configurationHash'],'expectedVersion':review['lockVersion']},human)
  check(name+'_explicit_fixture_human_confirmed',confirmed['state']=='APPROVED')
  active=request(action+'activate',{'configurationHash':confirmed['configurationHash'],'expectedVersion':confirmed['lockVersion']},human)
  check(name+'_explicit_fixture_human_activated',active['state']=='ACTIVE')
  wait(lambda:sql("select count(*) from ouf_ingestion.ing_run where source_id='"+source+"' and state='SUCCEEDED'")=='1',180)
  wait(lambda:sql("select count(*) from ouf_udp.materialization_job j join ouf_udp.handoff_intake h using(handoff_id) where h.source_id='"+source+"' and j.state='SUCCEEDED'")==str(rows),180)
  return {'sourceId':source,'onboardingVersionId':version,'configurationHash':current['configuration_hash'],'coverage':preflight}
 baseline=onboard('baseline',1)
 baseline_id=sql("select urban_object_id from ouf_udp.urban_object where canonical_type='"+class_iri+"'")
 check('baseline_object_materialized',len(baseline_id)==36)
 baseline_revisions=sql("select count(*) from ouf_udp.object_revision where urban_object_id='"+baseline_id+"'")
 incoming=onboard('incoming',2)
 check('two_input_rows_match_one_existing_and_create_one_new',sql('select count(*) from ouf_udp.urban_object')=='2')
 check('baseline_object_identity_retained',sql("select count(*) from ouf_udp.source_binding where urban_object_id='"+baseline_id+"'")=='2')
 check('existing_object_commit_has_new_revision',int(sql("select count(*) from ouf_udp.object_revision where urban_object_id='"+baseline_id+"'"))>int(baseline_revisions))
 check('exactly_three_durable_handoffs',sql('select count(*) from ouf_udp.handoff_intake')=='3')
 search=tool('urban.object.search',{'type':class_iri,'pageSize':100})
 check('real_mcp_search_returns_both_objects','Fixture Teatro' in json.dumps(search) and 'Fixture Secondo' in json.dumps(search))
 check('lexical_postal_code_leading_zero_retained','00123' in json.dumps(search))
 check('new_property_committed_on_existing_object','reviewed extension' in json.dumps(search))
 check('restricted_source_facts_not_returned','fixture-fonte_indirizzo' not in json.dumps(search))
 lineage=request('http://127.0.0.1:18132/api/udp/v1/objects/'+baseline_id+'/lineage',credential=human)
 check('lineage_preserves_both_real_sources','r4a-baseline' in json.dumps(lineage) and 'r4a-incoming' in json.dumps(lineage))
 check('mcp_created_and_prepared_both_sources',calls.get('ouf.managed-source.onboarding.create')==2 and calls.get('ouf.managed-source.onboarding.review.prepare')==2 and calls.get('urban.object.search')==1)
 (evidence/'mcp-transcript.json').write_text(json.dumps(captures,indent=2));(evidence/'lineage.json').write_text(json.dumps(lineage,indent=2))
 (evidence/'summary.json').write_text(json.dumps({'status':'PASS','checks':checks,'calls':calls,'baseline':baseline,'incoming':incoming,'baselineObjectId':baseline_id,'cohort':{k:os.environ.get(k+'_SHA') for k in ('R4A_ONBOARDING','R4A_SEMANTIC','R4A_UDP','R4A_MCP')}|{'ingestion':os.environ.get('GITHUB_SHA')},'level':'FOUR_REAL_OWNER_JVMS_REAL_MCP_POSTGRES_MINIO','limitations':['Disposable synthetic CSV rows with the real 20-field semantic proposal; not the user VPS asset or its 13 rows','Gateway mediation, browser sign-in and IAM are explicitly declared test fixtures','Raw CSV registration/profile uses real owner services behind a fixture transport; host chat attachment/picker tested separately','No production authority or runtime acceptance issued']},indent=2))
finally:
 for p in reversed(processes):
  p.terminate()
  try:p.wait(timeout=15)
  except subprocess.TimeoutExpired:p.kill();p.wait(timeout=5)
 for log in logs:log.close()
 server.shutdown();server.server_close();tokenfile.unlink(missing_ok=True);keyfile.unlink(missing_ok=True)
