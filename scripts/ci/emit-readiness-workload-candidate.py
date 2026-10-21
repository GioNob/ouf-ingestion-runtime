"""Freeze a CI-only image for the exact two-source readiness contract patch."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import zipfile

SOURCE = '4165b9e149be467d5d7c6bed35e4e1af4e249db1'
BASELINE = '0fe0de60c6d980fe97610acf195d4bfd46d3777f'
CHANGED = {
    'java/it/comune/trieste/ouf/ingestion/PublicationGatewayClient.java',
    'java/it/comune/trieste/ouf/ingestion/OnboardingReadinessService.java',
}
CLASSES = ('PublicationGatewayClient', 'OnboardingReadinessService',
           'IngestionValidateOnlyStartup', 'IngestionReadinessHealthIndicator',
           'GatewayReadReceiptVerifier', 'IngestionGatewayReadSecurityConfiguration',
           'IngestionIamSecurityConfiguration', 'IngestionIamDisabledConfiguration')


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1048576), b''):
            h.update(chunk)
    return h.hexdigest()


def inventory(root):
    return {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob('*')) if p.is_file()}


def verify_sources(root, baseline):
    before, after = inventory(baseline / 'src/main'), inventory(root / 'src/main')
    changed = {p for p in before.keys() | after.keys() if before.get(p) != after.get(p)}
    assert changed == CHANGED, 'EXACT_TWO_PRODUCTION_SOURCE_CHANGES_REQUIRED'
    for name in ('contracts', 'vendor/authorization-sdk'):
        old, new = inventory(baseline / name), inventory(root / name)
        assert old and old == new, 'UNCHANGED_CONTRACT_AND_AUTHORIZATION_SDK_REQUIRED'
    for name in ('pom.xml', 'Dockerfile'):
        assert digest(root / name) == digest(baseline / name), 'UNCHANGED_BUILD_DESCRIPTOR_REQUIRED'
    migrations = inventory(root / 'src/main/resources/db/migration')
    assert migrations and migrations == inventory(baseline / 'src/main/resources/db/migration')
    return migrations


def verify_image(image, archive):
    config = image['Config']
    assert config['User'] == '10002:10002' and config['Entrypoint'] == ['java', '-jar', '/app/app.jar']
    assert config['Labels']['org.opencontainers.image.revision'] == SOURCE
    assert image['Os'] == 'linux' and image['Architecture'] == 'amd64'
    assert 0 < archive.stat().st_size <= 536870912
    with tarfile.open(archive, 'r:gz') as tar:
        manifest_file = tar.extractfile('manifest.json')
        raw = manifest_file.read(65537)
        assert len(raw) <= 65536
        rows = json.loads(raw)
        assert len(rows) == 1 and rows[0]['RepoTags'] == ['ouf-ingestion-readiness:' + SOURCE]
        member = tar.getmember(rows[0]['Config'])
        assert member.isfile() and 0 < member.size <= 1048576
        encoded = tar.extractfile(member).read(1048577)
        assert 'sha256:' + hashlib.sha256(encoded).hexdigest() == image['Id']


def verify_jar(jar_path, migrations):
    with zipfile.ZipFile(jar_path) as jar:
        names = jar.namelist()
        assert len(names) == len(set(names))
        assert not any(n.endswith('-tests.jar') or (n.startswith('BOOT-INF/classes/') and
                       ('Fixture' in n or n.endswith('Test.class'))) for n in names)
        actual = {n.removeprefix('BOOT-INF/classes/db/migration/'): hashlib.sha256(jar.read(n)).hexdigest()
                  for n in names if n.startswith('BOOT-INF/classes/db/migration/') and n.endswith('.sql')}
        assert actual == migrations, 'PACKAGED_MIGRATIONS_MUST_MATCH_INSTALLED_BASELINE'
        return {name: hashlib.sha256(jar.read('BOOT-INF/classes/it/comune/trieste/ouf/ingestion/'
                                             + name + '.class')).hexdigest() for name in CLASSES}


def emit(root, recipe):
    for path, expected in ((root, SOURCE), (root / '.baseline', BASELINE), (root / '.recipe', recipe)):
        assert subprocess.check_output(['git', '-C', str(path), 'rev-parse', 'HEAD'],
                                       text=True, timeout=10).strip() == expected
    out = root / 'target/readiness-candidate'
    image = json.loads((root / 'target/readiness-image-inspect.json').read_text())[0]
    migrations = verify_sources(root, root / '.baseline')
    verify_image(image, out / 'image.tar.gz')
    classes = verify_jar(root / 'target/readiness-image.jar', migrations)
    scan = json.loads((out / 'trivy-image.json').read_text())
    assert scan['ArtifactName'] == 'ouf-ingestion-readiness:' + SOURCE
    assert isinstance(scan['Results'], list) and scan['Results']
    assert not any(v.get('Severity') in ('HIGH', 'CRITICAL') for r in scan['Results']
                   for v in r.get('Vulnerabilities', [])), 'HIGH_CRITICAL_VULNERABILITY_GATE'
    value = {'schema': 'ouf.ingestion-readiness-workload-candidate.v1', 'sourceCommit': SOURCE,
             'baselineSourceCommit': BASELINE, 'buildRecipeCommit': recipe,
             'changedProductionSources': sorted(CHANGED), 'imageId': image['Id'],
             'imageArchiveSha256': digest(out / 'image.tar.gz'),
             'imageArchiveBytes': (out / 'image.tar.gz').stat().st_size,
             'imageInspectSha256': digest(root / 'target/readiness-image-inspect.json'),
             'applicationJarSha256': digest(root / 'target/readiness-image.jar'),
             'applicationClasses': classes, 'migrationInventory': migrations, 'migrationChanges': 0,
             'imageUser': '10002:10002', 'imageEntrypoint': ['java', '-jar', '/app/app.jar'],
             'architecture': 'amd64', 'os': 'linux',
             'trivyReportSha256': digest(out / 'trivy-image.json'),
             'highCriticalVulnerabilityGatePassed': True, 'imageStartedByCandidateExport': False,
             'runtimeAuthorityIssued': False, 'targetInstalled': False, 'notReleaseAcceptance': True}
    (out / 'metadata.json').write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')
    print('OUF_READINESS_WORKLOAD_CANDIDATE=' + json.dumps(value, sort_keys=True, separators=(',', ':')),
          flush=True)
    return value


if __name__ == '__main__':
    emit(Path(os.environ['GITHUB_WORKSPACE']), os.environ['BUILD_RECIPE_COMMIT'])
