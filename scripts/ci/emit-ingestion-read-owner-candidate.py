"""Freeze public CI candidate bytes; no runtime installation or authority."""
import hashlib,json,os,zipfile
from pathlib import Path

def digest_file(path):
 h=hashlib.sha256()
 with path.open('rb')as stream:
  for block in iter(lambda:stream.read(1048576),b''):h.update(block)
 return h.hexdigest()
def inventory(root):
 return {str(p.relative_to(root)):digest_file(p)for p in sorted(root.rglob('V*__*.sql'))}
root=Path(os.environ['GITHUB_WORKSPACE']);target=root/'target';out=target/'owner-read-candidate'
image=json.loads((target/'owner-read-image-inspect.json').read_text())[0];commit=os.environ['GITHUB_SHA']
assert image['Config']['User']=='10002:10002'and image['Config']['Entrypoint']==['java','-jar','/app/app.jar']
assert image['Config']['Labels']['org.opencontainers.image.revision']==commit
before=inventory(root/'.pairwise/ingestion-base/src/main/resources/db/migration');after=inventory(root/'src/main/resources/db/migration');assert before and before==after
classes={}
with zipfile.ZipFile(target/'owner-read-image.jar')as jar:
 names=jar.namelist();assert not any(n.endswith('-tests.jar')for n in names)
 for n in ('GatewayReadReceiptVerifier','IngestionGatewayReadSecurityConfiguration','IngestionIamSecurityConfiguration','IngestionIamDisabledConfiguration'):
  p='BOOT-INF/classes/it/comune/trieste/ouf/ingestion/'+n+'.class';classes[n]=hashlib.sha256(jar.read(p)).hexdigest()
value={'schema':'ouf.ingestion-read-owner-candidate.v1','sourceCommit':commit,'imageId':image['Id'],
 'imageArchiveSha256':digest_file(out/'image.tar.gz'),'imageArchiveBytes':(out/'image.tar.gz').stat().st_size,
 'applicationJarSha256':digest_file(target/'owner-read-image.jar'),'applicationClasses':classes,
 'migrationInventory':after,'migrationChanges':0,'imageUser':'10002:10002','imageEntrypoint':['java','-jar','/app/app.jar'],
 'trivyReportSha256':digest_file(out/'trivy-image.json'),'highCriticalVulnerabilityGatePassed':True,
 'imageStartedByCandidateExport':False,'runtimeAuthorityIssued':False,'notReleaseAcceptance':True}
assert value['imageArchiveBytes']<=536870912
(out/'metadata.json').write_text(json.dumps(value,sort_keys=True,indent=2)+'\n')
print('OUF_INGESTION_READ_OWNER_CANDIDATE='+json.dumps(value,sort_keys=True,separators=(',',':')),flush=True)
