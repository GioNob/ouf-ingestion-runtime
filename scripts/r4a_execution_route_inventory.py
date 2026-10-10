#!/usr/bin/env python3
"""Read only the live execution route; never print credentials or route bodies."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import r4a_prepare_frozen_compatibility_probe as helper

URI = "/internal/object-storage/v1/content"
LEGACY_PATH = "/api/internal/v1/onboarding/managed-files/content"
OWNER_REVISION = "f74c3a9f377b5ce93b5cab298fa24372de1602bc"


def admin_key(text):
    """Accept the conventional literal APISIX admin_key list, fail closed otherwise."""
    lines = text.splitlines()
    starts = [(i, len(line) - len(line.lstrip())) for i, line in enumerate(lines)
              if re.fullmatch(r"\s*admin_key:\s*(?:#.*)?", line)]
    if len(starts) != 1:
        raise RuntimeError("APISIX_ADMIN_KEY_LAYOUT_UNSUPPORTED")
    start, indent = starts[0]
    entries, current = [], {}
    for line in lines[start + 1:]:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if len(line) - len(line.lstrip()) <= indent:
            break
        if line.lstrip().startswith("- "):
            if current:
                entries.append(current)
            current = {}
        match = re.fullmatch(r"\s*(?:-\s*)?(name|key|role):\s*(.*?)\s*", line)
        if match:
            value = match[2]
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            current[match[1]] = value
    if current:
        entries.append(current)
    keys = [e.get("key", "") for e in entries if e.get("role") == "admin"]
    if len(keys) != 1 or not re.fullmatch(r"[A-Za-z0-9._-]{8,256}", keys[0]):
        raise RuntimeError("APISIX_ADMIN_KEY_LAYOUT_UNSUPPORTED")
    return keys[0]


def mounted_config(container):
    paths = []
    target = Path("/usr/local/apisix/conf/config.yaml")
    for mount in container.get("Mounts", []):
        if mount.get("Type") != "bind":
            continue
        destination = Path(mount["Destination"])
        if target == destination or target.is_relative_to(destination):
            paths.append(Path(mount["Source"]) / target.relative_to(destination))
    if len(paths) != 1:
        raise RuntimeError("APISIX_CONFIG_BIND_NOT_UNIQUE")
    return paths[0]


def route_values(document):
    entries = document.get("list")
    if entries is None:
        entries = document.get("node", {}).get("nodes")
    if not isinstance(entries, list):
        raise RuntimeError("APISIX_ROUTE_RESPONSE_UNSUPPORTED")
    result = []
    for entry in entries:
        value = entry.get("value", {})
        if isinstance(value, str):
            value = json.loads(value)
        if not isinstance(value, dict):
            raise RuntimeError("APISIX_ROUTE_RESPONSE_UNSUPPORTED")
        result.append(value)
    return result


def summarize(routes):
    matching = [r for r in routes if URI in ([r.get("uri")] + r.get("uris", []))
                and (not r.get("methods") or "GET" in r["methods"])]
    print("EXEC_READ_ROUTE_COUNT=" + str(len(matching)))
    if len(matching) != 1:
        return
    route = matching[0]
    plugins = route.get("plugins", {})
    nodes = route.get("upstream", {}).get("nodes", {})
    facts = {
        "EXEC_READ_OWNER_LEGACY_PATH": plugins.get("proxy-rewrite", {}).get("uri") == LEGACY_PATH,
        "EXEC_READ_UPSTREAM_ONBOARDING": nodes == {"ouf-onboarding:8080": 1},
        "EXEC_READ_UPSTREAM_REFERENCE": "upstream_id" in route,
        "EXEC_READ_OIDC_PRESENT": "openid-connect" in plugins,
        "EXEC_READ_REQUIRED_SCOPE_MATCH": plugins.get("openid-connect", {}).get("required_scopes")
            == ["ouf.internal.object-storage.read"],
        "EXEC_READ_REWRITE_PRESENT": "proxy-rewrite" in plugins,
    }
    for name, value in facts.items():
        print(name + "=" + str(value).lower())


def main(args):
    if os.geteuid() != 0:
        raise RuntimeError("ROOT_REQUIRED")
    print("R4A_EXECUTION_ROUTE_INVENTORY=READ_ONLY")
    owner = helper.inspect("ouf-onboarding")
    image = helper.inspect(owner["Image"], "image")
    revision = image.get("Config", {}).get("Labels", {}).get("org.opencontainers.image.revision")
    print("EXEC_READ_OWNER_BASELINE_REVISION_MATCH=" + str(revision == OWNER_REVISION).lower())
    apisix = helper.inspect("ouf-apisix")
    if not apisix["State"]["Running"]:
        raise RuntimeError("APISIX_NOT_RUNNING")
    key = (args.admin_key.read_text().strip() if args.admin_key else
           admin_key(mounted_config(apisix).read_text()))
    if not re.fullmatch(r"[A-Za-z0-9._-]{8,256}", key):
        raise RuntimeError("APISIX_ADMIN_KEY_LAYOUT_UNSUPPORTED")
    config = ('silent\nshow-error\nfail\nmax-time = 15\nmax-filesize = 10485760\n'
              'header = "X-API-KEY: ' + key + '"\n'
              'url = "http://127.0.0.1:9180/apisix/admin/routes"\n')
    raw = helper.run(["docker", "run", "--rm", "-i", "--read-only", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--network", "container:ouf-apisix",
        "curlimages/curl:8.16.0", "--config", "-"], input=config, timeout=30)
    summarize(route_values(json.loads(raw)))
    print("R4A_EXECUTION_ROUTE_INVENTORY=COMPLETE LIVE_UNCHANGED=true SECRETS_NOT_PRINTED=true")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-key", type=Path)
    try:
        main(parser.parse_args())
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError) as error:
        code = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        print("R4A_EXECUTION_ROUTE_INVENTORY=BLOCKED CODE=" + code + " SECRETS_NOT_PRINTED=true")
        raise SystemExit(1)
