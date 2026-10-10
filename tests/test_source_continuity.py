import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('continuity', Path(__file__).parents[1] / 'scripts/check-source-continuity.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ContinuityTests(unittest.TestCase):
    def setUp(self):
        self.path = 'src/main/resources/db/migration/V34__retained.sql'
        self.feature = 'src/main/java/owner/GovernedIdentityEngine.java'
        self.baseline = {'schema': 'ouf.source-continuity-baseline.v1', 'modules': {'udp': {
            'sourceCommit': 'a' * 40, 'migrationBlobs': {self.path: 'b' * 40},
            'requiredSourcePaths': [self.feature]}}}
        self.tree = {'truncated': False, 'tree': [
            {'path': self.path, 'type': 'blob', 'mode': '100644', 'sha': 'b' * 40},
            {'path': self.feature, 'type': 'blob', 'mode': '100644', 'sha': 'c' * 40}]}

    def run_check(self):
        return module.check(self.baseline, 'udp', self.tree)

    def test_retained_and_additive_sources_pass_without_authority(self):
        self.tree['tree'].append({'path': 'src/main/resources/db/migration/V35__additive.sql', 'type': 'blob', 'mode': '100644', 'sha': 'd' * 40})
        result = self.run_check()
        self.assertEqual(result['status'], 'STRUCTURAL_CONTINUITY_PASS')
        self.assertFalse(result['deploymentAuthorized'])
        self.assertFalse(result['behavioralCompatibilityProven'])

    def test_older_main_missing_migration_is_blocked(self):
        self.tree['tree'].pop(0)
        self.assertIn('RETAINED_MIGRATION_MISSING_OR_CHANGED', str(self.run_check()['failures']))

    def test_rewritten_retained_migration_is_blocked(self):
        self.tree['tree'][0]['sha'] = 'e' * 40
        self.assertTrue(self.run_check()['failures'])

    def test_removed_governed_engine_is_blocked(self):
        self.tree['tree'].pop()
        self.assertIn('RETAINED_FEATURE_SOURCE_MISSING', str(self.run_check()['failures']))

    def test_symlink_instead_of_migration_is_blocked(self):
        self.tree['tree'][0]['mode'] = '120000'
        self.assertTrue(self.run_check()['failures'])

    def test_incomplete_or_duplicate_inventory_is_rejected(self):
        self.tree['truncated'] = True
        with self.assertRaisesRegex(ValueError, 'COMPLETE_TREE_REQUIRED'):
            self.run_check()
        self.tree['truncated'] = False
        self.tree['tree'].append(copy.deepcopy(self.tree['tree'][0]))
        with self.assertRaisesRegex(ValueError, 'DUPLICATE_OR_INVALID_PATH'):
            self.run_check()

    def test_duplicate_flyway_version_is_blocked(self):
        self.tree['tree'].append({'path': 'src/main/resources/db/migration/V34__other.sql', 'type': 'blob', 'mode': '100644', 'sha': 'f' * 40})
        self.assertIn('DUPLICATE_MIGRATION_VERSION', str(self.run_check()['failures']))
