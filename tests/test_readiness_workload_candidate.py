import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
import zipfile
import hashlib

spec = importlib.util.spec_from_file_location('emitter', Path(__file__).parents[1] / 'scripts/ci/emit-readiness-workload-candidate.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        for tree in (self.root, self.root / '.baseline'):
            for path in list(m.CHANGED) + ['resources/db/migration/V1__base.sql', 'java/Unchanged.java']:
                p = tree / 'src/main' / path
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text('original')
            for name in ('contracts/contract.json', 'vendor/authorization-sdk/pom.xml', 'pom.xml', 'Dockerfile'):
                p = tree / name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text('same')
        for path in m.CHANGED:
            (self.root / 'src/main' / path).write_text('fixed')

    def tearDown(self):
        self.tmp.cleanup()

    def test_exact_sources_and_unchanged_migrations(self):
        self.assertEqual(m.verify_sources(self.root, self.root / '.baseline'),
                         {'V1__base.sql': hashlib.sha256(b'original').hexdigest()})
        (self.root / 'src/main/resources/db/migration/V1__base.sql').write_text('changed')
        with self.assertRaises(AssertionError):
            m.verify_sources(self.root, self.root / '.baseline')

    def test_unexpected_java_and_dependency_drift_are_rejected(self):
        p = self.root / 'src/main/java/Unchanged.java'
        p.write_text('changed')
        with self.assertRaises(AssertionError):
            m.verify_sources(self.root, self.root / '.baseline')
        p.write_text('original')
        (self.root / 'pom.xml').write_text('changed dependency')
        with self.assertRaises(AssertionError):
            m.verify_sources(self.root, self.root / '.baseline')

    def test_archive_identity_and_nonroot_revision_are_enforced(self):
        config = b'{"fixture":"image-config"}'
        image = {'Id': 'sha256:' + hashlib.sha256(config).hexdigest(), 'Os': 'linux', 'Architecture': 'amd64',
                 'Config': {'User': '10002:10002', 'Entrypoint': ['java', '-jar', '/app/app.jar'],
                            'Labels': {'org.opencontainers.image.revision': m.SOURCE}}}
        archive = self.root / 'image.tar.gz'
        with tarfile.open(archive, 'w:gz') as tar:
            for name, data in [('manifest.json', json.dumps([{'RepoTags': ['ouf-ingestion-readiness:' + m.SOURCE],
                                                            'Config': 'config.json'}]).encode()),
                               ('config.json', config)]:
                info = tarfile.TarInfo(name);info.size = len(data);tar.addfile(info, io.BytesIO(data))
        m.verify_image(image, archive)
        image['Config']['User'] = '0'
        with self.assertRaises(AssertionError):
            m.verify_image(image, archive)
        image['Config']['User'] = '10002:10002'
        image['Id'] = 'sha256:' + '0' * 64
        with self.assertRaises(AssertionError):
            m.verify_image(image, archive)

    def test_packaged_migration_drift_and_test_fixture_rejected(self):
        jar = self.root / 'app.jar'
        with zipfile.ZipFile(jar, 'w') as z:
            z.writestr('BOOT-INF/classes/db/migration/V1__base.sql', b'original')
            for name in m.CLASSES:
                z.writestr('BOOT-INF/classes/it/comune/trieste/ouf/ingestion/' + name + '.class', b'class')
        migrations = {'V1__base.sql': hashlib.sha256(b'original').hexdigest()}
        self.assertEqual(set(m.verify_jar(jar, migrations)), set(m.CLASSES))
        with self.assertRaises(AssertionError):
            m.verify_jar(jar, {'V1__base.sql': '0' * 64})
        with zipfile.ZipFile(jar, 'a') as z:
            z.writestr('BOOT-INF/classes/DangerousFixture.class', b'fixture')
        with self.assertRaises(AssertionError):
            m.verify_jar(jar, migrations)


if __name__ == '__main__':
    unittest.main()
