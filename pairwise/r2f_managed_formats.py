#!/usr/bin/env python3
"""Four real owner processes; declared upstream, Gateway and identity fixtures."""
import os,json,time,secrets,pathlib,subprocess,threading,urllib.request,urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlparse,parse_qs
root=pathlib.Path(__file__).resolve().parents[1]
onboarding=pathlib.Path(os.environ['ONBOARDING_ROOT']).resolve();udp=pathlib.Path(os.environ['UDP_ROOT']).resolve()
semantic=pathlib.Path(os.environ['SEMANTIC_ROOT']).resolve()
evidence=root/'target/r2f-evidence';evidence.mkdir(parents=True,exist_ok=True)
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
 def reply(self,status,body,cookie=None):
  self.send_response(status);
  if cookie:self.send_header('Set-Cookie',cookie)
  self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
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
    refs={'object://r2f/'+name:str(pathlib.Path(os.environ['OUF_R2F_INPUT_DIR'])/name) for name in ('assets.zip','assets.mdb','assets.accdb','assets.gpkg','assets-shift.zip')}
    ref=parse_qs(path.query).get('ref',[''])[0]
    if auth!='Bearer '+token or ref not in refs:self.send_error(403);return
    self.reply(200,pathlib.Path(refs[ref]).read_bytes());return
   elif path.path=='/internal/sources/v1/fetch':
    if auth!='Bearer '+token or json.loads(data)['bindingRef']!='gateway://r2b/pull':self.send_error(403);return
    self.reply(200,json.dumps({'items':[{'id':'2','name':'Beta','secret':'classified-pull'}]}).encode());return
   else:self.send_error(404);return
   headers={'Authorization':auth,'Content-Type':'application/json'}
   for key in ('Cookie','X-OUF-CSRF','Origin'):
    if self.headers.get(key):headers[key]=self.headers[key]
   request=urllib.request.Request(upstream,data=data,method=self.command,headers=headers)
   with urllib.request.urlopen(request,timeout=15) as r:body=r.read(2_097_153);status=r.status;cookie=r.headers.get('Set-Cookie')
   if path.path=='/api/internal/v1/handoffs' and self.command=='POST':
    calls['handoff']+=1;calls['duplicateAck']+=int(json.loads(body).get('duplicate',False))
    if not ack_allowed:status=202;calls['nondurableResponse']+=1
   self.reply(status,body,cookie)
  except urllib.error.HTTPError as error:self.reply(error.code,error.read(65536))
server=ThreadingHTTPServer(('127.0.0.1',18132),Gateway);threading.Thread(target=server.serve_forever,daemon=True).start()
try:
 env=dict(os.environ,OUF_PAIRWISE_TOKEN=token,OUF_PAIRWISE_HUMAN_TOKEN=human,OUF_ONB_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_ONB_DB_USER='ouf',OUF_ONB_DB_PASSWORD=os.environ['PGPASSWORD'],OUF_UDP_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_UDP_DB_USER='ouf',OUF_UDP_DB_PASSWORD=os.environ['PGPASSWORD'],SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE='3',SPRING_DATASOURCE_HIKARI_MINIMUM_IDLE='0')
 def classpath(path):return str(path/'target/test-classes')+':'+str(path/'target/classes')+':'+(path/'target/r2-classpath.txt').read_text().strip()
 from r2f_fixtures import shapefile,geopackage
 shapefile(os.environ['OUF_R2F_INPUT_DIR']);shapefile(os.environ['OUF_R2F_INPUT_DIR'],'assets-shift.zip',13.7701);geopackage(os.environ['OUF_R2F_INPUT_DIR'])
 subprocess.run(['java','-cp',classpath(onboarding),'it.comune.trieste.ouf.pairwise.ManagedFormatsPublisherFixture',os.environ['OUF_R2F_INPUT_DIR']],check=True,timeout=60)
 env.update(OUF_SEM_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_SEM_DB_USER='ouf',OUF_SEM_DB_PASSWORD=os.environ['PGPASSWORD'],OUF_DISCOVERY_WORKER_ENABLED='false',OUF_R2C_SEMANTIC_RESULT=str(evidence/'semantic-publication.json'))
 launch('semantic',['java','-cp',classpath(semantic),'it.comune.trieste.ouf.pairwise.GovernedPublicationFixture','--server.port=18135'],env)
 wait(lambda:(evidence/'semantic-publication.json').exists())
 semantic_result=json.loads((evidence/'semantic-publication.json').read_text());checks.extend(semantic_result['checks'])
 binding=semantic_result['binding']
 exact='http://127.0.0.1:18132/api/semantic/v1/references:resolve?'+urllib.parse.urlencode({k:binding[k] for k in ('semanticId','revisionId','publicationSetId')})
 check('real_registry_reference_active',http(exact)['status']=='ACTIVE')
 launch('onboarding',['java','-cp',classpath(onboarding),'it.comune.trieste.ouf.pairwise.ServingPublisherFixture','--server.port=18131','--ouf.runtime-publications.tenant-id=tenant-a'],env)
 wait(lambda:len(http('http://127.0.0.1:18131/api/onboarding/v1/runtime/publications')['items'])==1)
 udp_args=['java','-cp',classpath(udp),'it.comune.trieste.ouf.pairwise.ServingConsumerFixture','--server.port=18134','--ouf.ths.origin=https://fixture.ouf.test','--ouf.udp.execution.enabled=true','--ouf.udp.execution.gateway-url=http://127.0.0.1:18132','--ouf.udp.execution.token-file='+str(tokenfile),'--ouf.udp.lake.tenant-id=tenant-a','--ouf.udp.lake.required=true','--ouf.udp.lake.raw-retention-days=30','--ouf.udp.lake.raw-retention-class=OPERATIONAL','--ouf.udp.lake.raw-access-label=RESTRICTED','--ouf.udp.lake.s3.bucket=r2b-lake','--ouf.udp.lake.s3.endpoint=http://127.0.0.1:9000','--ouf.udp.lake.s3.path-style=true']
 consumer=launch('udp',udp_args,env);wait(lambda:http('http://127.0.0.1:18134/actuator/health')['status']=='UP')
 env.update(OUF_ING_DB_URL='jdbc:postgresql://127.0.0.1:5432/ouf',OUF_ING_DB_USER='ouf',OUF_ING_DB_PASSWORD=os.environ['PGPASSWORD'])
 args=['java','-jar',str(next((root/'target').glob('ingestion-runtime-*.jar'))),'--server.port=18133','--ouf.ingestion.activation.enabled=true','--ouf.ingestion.execution.enabled=true','--ouf.ingestion.activation.gateway-url=http://127.0.0.1:18132','--ouf.ingestion.activation.token-file='+str(tokenfile),'--ouf.ingestion.activation.tenant-id=tenant-a','--ouf.ingestion.activation.poll-ms=500']
 runtime=launch('ingestion',args,env)
 wait(lambda:http('http://127.0.0.1:18133/actuator/health')['status']=='UP')
 wait(lambda:sql("select count(*) from ouf_ingestion.ing_run where state='SUCCEEDED'")=='1',180)
 wait(lambda:sql("select count(*) from ouf_udp.urban_object_current_state")=='1')
 asset=sql("select urban_object_id from ouf_udp.urban_object where canonical_type='https://example.org/Asset'")
 before=http('http://127.0.0.1:18132/api/udp/v1/objects/'+asset,human)
 check('shapefile_has_authorized_geometry','canonicalGeometry' in before)
 check('shapefile_preserves_leading_zero_key',before['properties']['https://example.org/CODE']=='001')
 def post(kind):
  request=urllib.request.Request('http://127.0.0.1:18131/fixture/r2f/'+kind,data=b'',method='POST',headers={'Authorization':'Bearer '+human})
  with urllib.request.urlopen(request,timeout=30) as r:return json.load(r)
 published=post('assets')
 metadata=published['schemaEvidence']['metadata']
 hints=metadata['semanticHints']
 relation_hints=[hint for hint in hints if hint['kind']=='RELATIONSHIP']
 check('access_declared_relationship_is_proposal',len(relation_hints)==1 and relation_hints[0]['status']=='PENDING_HUMAN_REVIEW' and 'relationIri' not in relation_hints[0])
 check('access_composite_foreign_key_preserved',relation_hints[0]['referencedColumns']==['DISTRICT','CODE'] and relation_hints[0]['referencingColumns']==['DISTRICT','ASSET_CODE'])
 check('access_composite_primary_key_preserved',any(hint.get('declaredPrimaryKey')==['DISTRICT','CODE'] for hint in hints))
 wait(lambda:sql("select count(*) from ouf_ingestion.ing_run where source_id='r2f-assets' and state='SUCCEEDED'")=='1',180)
 wait(lambda:sql("select count(*) from ouf_udp.materialization_observation m join ouf_udp.handoff_intake h on h.handoff_id=m.handoff_id where h.source_id='r2f-assets'")=='1')
 check('different_native_ids_reuse_same_urban_object',sql("select count(distinct urban_object_id) from ouf_udp.source_binding where source_id in ('r2f-shape','r2f-assets')")=='1' and sql("select count(*) from ouf_udp.source_binding where source_id in ('r2f-shape','r2f-assets')")=='2')
 check('weighted_typo_match_keeps_evidence',sql("select count(*) from ouf_udp.resolution_decision d join ouf_udp.handoff_intake h on h.handoff_id=d.handoff_id where h.source_id='r2f-assets' and d.outcome='MATCH' and d.confidence=0.9 and d.score_evidence->>'reason'='HIGH_CONFIDENCE_UNIQUE'")=='1')
 after=http('http://127.0.0.1:18132/api/udp/v1/objects/'+asset,human)
 check('identity_score_does_not_override_property_authority',after['properties']['https://example.org/NAME']=='Asset Roma')
 check('tabular_contribution_preserves_existing_geometry',after['canonicalGeometry']==before['canonicalGeometry'])
 post('children')
 wait(lambda:sql("select count(*) from ouf_udp.urban_relationship where status='ACTIVE'")=='2',180)
 children=sql("select string_agg(urban_object_id::text,',' order by urban_object_id) from ouf_udp.urban_object where canonical_type='https://example.org/Child'")
 relation=http('http://127.0.0.1:18132/api/udp/v1/objects/'+asset+'/relationships?direction=INBOUND',human)
 check('asset_navigates_to_both_access_children',all(child in json.dumps(relation) for child in children.split(',')))
 child=children.split(',')[0]
 outgoing=http('http://127.0.0.1:18132/api/udp/v1/objects/'+child+'/relationships',human)
 check('access_child_navigates_to_asset',asset in json.dumps(outgoing))
 old_publication=http('http://127.0.0.1:18131/api/onboarding/v1/runtime/publications/r2f-children/active')
 post('reload')
 wait(lambda:sql("select count(*) from ouf_ingestion.ing_run where source_id='r2f-children' and state='SUCCEEDED'")=='2',180)
 wait(lambda:sql("select count(*) from ouf_udp.materialization_observation m join ouf_udp.handoff_intake h on h.handoff_id=m.handoff_id where h.source_id='r2f-children'")=='4')
 check('mdb_to_accdb_reordered_rows_keep_identities',sql("select string_agg(urban_object_id::text,',' order by urban_object_id) from ouf_udp.urban_object where canonical_type='https://example.org/Child'")==children)
 check('reimport_does_not_duplicate_active_relations',sql("select count(*) from ouf_udp.urban_relationship where status='ACTIVE'")=='2')
 check('three_canonical_objects_from_six_observations',sql("select count(*) from ouf_udp.urban_object")=='3' and sql("select count(*) from ouf_udp.materialization_observation")=='6')
 old=old_publication['bundle'];ref=old['bundleId']+':'+old['bundleVersion']+':'+old['checksum']
 check('original_access_profile_still_resolves',http('http://127.0.0.1:18131/api/onboarding/v1/runtime/publications/resolve?'+urllib.parse.urlencode({'bundleRef':ref}))['bundle']==old)
 lineage=http('http://127.0.0.1:18132/api/udp/v1/objects/'+asset+'/lineage',human)
 check('lineage_pins_original_contracts','contractRefs' in json.dumps(lineage))
 for credential,name in [(token,'workload'),('','anonymous')]:
  try:http('http://127.0.0.1:18132/api/udp/v1/objects/'+asset,credential);raise AssertionError(name+' reads object')
  except urllib.error.HTTPError as e:check(name+'_serving_denied',e.code==403)
 check('raw_file_records_are_verified',sql("select count(*) from ouf_udp.handoff_intake h join ouf_udp.lake_object l on l.lake_object_id=h.source_raw_lake_object_id where l.state='VERIFIED'")=='6')
 # Reuse the real Registry's published relation through the Access proposal lifecycle.
 check('access_relationship_proposals_resolved_to_published_semantics',sql("select count(*) from ouf_onboarding.semantic_gap g join ouf_onboarding.semantic_candidate c on c.candidate_id=g.selected_candidate_id where g.source_id in ('r2f-assets','r2f-children') and g.state='RESOLVED' and c.status='PUBLISHED' and c.evidence->>'direction'='REFERENCING_TO_REFERENCED' and c.evidence->'binding'->>'publicationSetId' is not null")=='2')
 post('gpkg')
 wait(lambda:sql("select count(*) from ouf_udp.materialization_observation m join ouf_udp.handoff_intake h on h.handoff_id=m.handoff_id where h.source_id='r2f-gpkg'")=='1',180)
 equivalent=http('http://127.0.0.1:18132/api/udp/v1/objects/'+asset,human)
 check('geopackage_and_shapefile_reuse_canonical_identity',sql("select urban_object_id from ouf_udp.source_binding where source_id='r2f-gpkg'")==asset)
 check('equivalent_formats_preserve_geometry_and_attributes',equivalent['canonicalGeometry']==before['canonicalGeometry'] and equivalent['properties']==before['properties'])
 check('equivalent_formats_preserve_existing_relationships',sql("select count(*) from ouf_udp.urban_relationship where status='ACTIVE'")=='2')
 # A new approved authority policy deliberately leaves NAME tied; exercise the real HTTP owner API.
 post('conflicts')
 wait(lambda:sql("select count(*) from ouf_udp.property_conflict where property_iri='https://example.org/NAME' and state='OPEN'")=='1',180)
 conflict=sql("select conflict_id from ouf_udp.property_conflict where property_iri='https://example.org/NAME' and state='OPEN'")
 url='http://127.0.0.1:18132/api/udp/v1/governance/properties/conflicts/'+conflict
 request=urllib.request.Request(url,headers={'Authorization':'Bearer '+human})
 with urllib.request.urlopen(request,timeout=10) as response:
  review=json.load(response);cookie=response.headers['Set-Cookie'].split(';',1)[0]
 chosen=next(c['contributionId'] for c in review['candidates'] if c['sourceId']=='r2f-assets')
 payload={'expectedCurrentRevision':review['currentRevision'],'chosenContribution':chosen,'reason':'Verified Access contribution in integration fixture'}
 def decide(body,csrf=True,origin='https://fixture.ouf.test'):
  headers={'Authorization':'Bearer '+human,'Content-Type':'application/json','Cookie':cookie,'Origin':origin}
  if csrf:headers['X-OUF-CSRF']=review['csrfToken']
  request=urllib.request.Request(url+'/decisions',data=json.dumps(body).encode(),headers=headers,method='POST')
  with urllib.request.urlopen(request,timeout=10) as response:return json.load(response)
 for name,body,csrf,origin,expected in [('missing_csrf',payload,False,'https://fixture.ouf.test',403),('cross_origin',payload,True,'https://other.test',403),('stale_revision',dict(payload,expectedCurrentRevision='00000000-0000-0000-0000-000000000000'),True,'https://fixture.ouf.test',409)]:
  try:decide(body,csrf,origin);raise AssertionError(name+' accepted')
  except urllib.error.HTTPError as error:check('human_property_'+name+'_denied',error.code==expected)
 decision=decide(payload)
 check('human_property_retry_returns_same_decision',decide(payload)==decision)
 current=http('http://127.0.0.1:18132/api/udp/v1/objects/'+asset,human)
 check('human_property_choice_updates_only_selected_property',current['properties']['https://example.org/NAME']=='Asset Romo' and current['properties']['https://example.org/CODE']=='001' and current['canonicalGeometry']==before['canonicalGeometry'])
 check('human_property_decision_persisted_once',sql("select count(*) from ouf_udp.human_property_decision")=='1')
 consumer.terminate();consumer.wait(timeout=20);processes.remove(consumer)
 consumer=launch('udp-restarted',udp_args,env);wait(lambda:http('http://127.0.0.1:18134/actuator/health')['status']=='UP')
 restored=http('http://127.0.0.1:18132/api/udp/v1/objects/'+asset,human)
 check('human_property_choice_survives_owner_restart',restored['properties']==current['properties'] and restored['revisionId']==current['revisionId'])
 post('geometry')
 wait(lambda:sql("select count(*) from ouf_udp.spatial_resolution_issue i join ouf_udp.handoff_intake h on h.handoff_id=i.handoff_id where h.source_id='r2f-geometry' and i.state='OPEN' and i.reason_code='SPATIAL_AUTHORITY_CONFLICT'")=='1',180)
 issue=sql("select issue_id from ouf_udp.spatial_resolution_issue i join ouf_udp.handoff_intake h on h.handoff_id=i.handoff_id where h.source_id='r2f-geometry' and i.state='OPEN' and i.reason_code='SPATIAL_AUTHORITY_CONFLICT'")
 url='http://127.0.0.1:18132/api/udp/v1/governance/geometry/issues/'+issue
 with urllib.request.urlopen(urllib.request.Request(url,headers={'Authorization':'Bearer '+human}),timeout=10) as response:
  review=json.load(response);cookie=response.headers['Set-Cookie'].split(';',1)[0]
 check('geometry_review_has_authorized_comparison_metrics',review['metrics']['minimum_distance_meters']>0 and not review['metrics']['topologically_equal'])
 payload={'expectedCurrentRevision':review['current']['revisionId'],'chosenRevision':review['candidate']['revisionId'],'reason':'Verified geometry candidate in integration fixture'}
 geometry_decision=decide(payload)
 check('geometry_human_decision_retry_is_idempotent',decide(payload)==geometry_decision)
 wait(lambda:sql("select count(*) from ouf_udp.materialization_job j join ouf_udp.handoff_intake h on h.handoff_id=j.handoff_id where h.source_id='r2f-geometry' and j.state='SUCCEEDED'")=='1')
 geometry_current=http('http://127.0.0.1:18132/api/udp/v1/objects/'+asset,human)
 check('geometry_human_choice_aligns_current_and_canonical_property',geometry_current['geometry']==review['candidate']['geometry'] and geometry_current['properties']['https://example.org/geom']['geoJson']==geometry_current['geometry'])
 check('geometry_choice_preserves_scalar_human_value',geometry_current['properties']['https://example.org/NAME']=='Asset Romo')
 check('geometry_decision_persisted_once',sql("select count(*) from ouf_udp.human_geometry_decision")=='1')

 (evidence/'human-scenario.json').write_text(json.dumps({'assetBefore':before,'assetAfter':after,'relation':relation,'lineage':lineage,'schemaEvidence':published['schemaEvidence']},indent=2))
 (evidence/'summary.json').write_text(json.dumps({'status':'PASS','checks':checks,'calls':calls,'publisherCommit':os.environ['R2B_PUBLISHER_SHA'],'udpCommit':os.environ['R2B_UDP_SHA'],'runtimeCommit':os.environ.get('GITHUB_SHA'),'semanticCommit':os.environ['R2C_SEMANTIC_SHA'],'level':'FOUR_REAL_OWNER_JVM_PROCESSES_POSTGRES_MINIO_WITH_DECLARED_GATEWAY_IDENTITY_FIXTURES','limitations':['Source data and human approvals use declared test fixtures','Registry publication and relation direction are explicitly selected by test actors; production IAM and APISIX remain separate acceptance gates','Geometry roles/validity and the browser use dedicated owner/browser suites; the live cross-owner human decisions cover scalar properties and geometry','Secure session cookie is explicitly forwarded by the HTTP test client; this is not production TLS/session acceptance']},indent=2))

finally:
 diagnostics={}
 for name,query in {'runs':"select coalesce(jsonb_agg(x),'[]'::jsonb) from (select source_id,state,failure_code from ouf_ingestion.ing_run limit 20)x",'udpJobs':"select coalesce(jsonb_agg(x),'[]'::jsonb) from (select state,safe_failure_code from ouf_udp.materialization_job limit 20)x",'checks':None}.items():
  if query is None:diagnostics[name]=checks;continue
  try:diagnostics[name]=json.loads(sql(query))
  except Exception:pass
 (evidence/'diagnostics.json').write_text(json.dumps(diagnostics,indent=2))
 for p in reversed(processes):
  p.terminate()
  try:p.wait(timeout=20)
  except subprocess.TimeoutExpired:p.kill();p.wait(timeout=5)
 for f in logs:f.close()
 server.shutdown();server.server_close();tokenfile.unlink(missing_ok=True)
