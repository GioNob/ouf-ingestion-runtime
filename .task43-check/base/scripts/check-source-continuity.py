"""Check retained migration bytes and feature presence; never authorizes deployment."""
import argparse
import json
import re


def check(baseline, repository, candidate):
    if baseline.get('schema') != 'ouf.source-continuity-baseline.v1':
        raise ValueError('UNSUPPORTED_BASELINE')
    retained = baseline['modules'][repository]
    if candidate.get('truncated') is not False:
        raise ValueError('COMPLETE_TREE_REQUIRED')
    entries = candidate.get('tree')
    if not isinstance(entries, list):
        raise ValueError('TREE_REQUIRED')
    by_path = {}
    for entry in entries:
        path = entry.get('path')
        if not isinstance(path, str) or path in by_path:
            raise ValueError('DUPLICATE_OR_INVALID_PATH')
        by_path[path] = entry
    failures = []
    for path, digest in retained['migrationBlobs'].items():
        entry = by_path.get(path)
        if not entry or entry.get('type') != 'blob' or entry.get('mode') != '100644' or entry.get('sha') != digest:
            failures.append({'path': path, 'reason': 'RETAINED_MIGRATION_MISSING_OR_CHANGED'})
    for path in retained['requiredSourcePaths']:
        entry = by_path.get(path)
        if not entry or entry.get('type') != 'blob' or entry.get('mode') != '100644':
            failures.append({'path': path, 'reason': 'RETAINED_FEATURE_SOURCE_MISSING'})
    versions = {}
    for path, entry in by_path.items():
        if not path.startswith('src/main/resources/db/migration/'):
            continue
        match = re.search(r'/V([0-9]+)__[^/]+\.sql$', path)
        if match and entry.get('type') == 'blob':
            version = int(match.group(1))
            if version in versions:
                failures.append({'path': path, 'reason': 'DUPLICATE_MIGRATION_VERSION'})
            versions[version] = path
    return {'schema': 'ouf.source-continuity-check.v1', 'repository': repository,
            'retainedSourceCommit': retained['sourceCommit'],
            'status': 'STRUCTURAL_CONTINUITY_PASS' if not failures else 'SOURCE_CONTINUITY_BLOCKED',
            'failures': failures, 'deploymentAuthorized': False,
            'behavioralCompatibilityProven': False, 'currentTargetRead': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--repository', required=True)
    parser.add_argument('--candidate-tree', required=True)
    args = parser.parse_args()
    with open(args.baseline, encoding='utf-8') as f:
        baseline = json.load(f)
    with open(args.candidate_tree, encoding='utf-8') as f:
        candidate = json.load(f)
    result = check(baseline, args.repository, candidate)
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(0 if not result['failures'] else 1)
