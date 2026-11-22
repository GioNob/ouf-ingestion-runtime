#!/usr/bin/env python3
"""Read-only scope/grant inventory; no secrets, identities, grant edits or GET data."""
import argparse
import base64
import json
import os
from pathlib import Path
import subprocess
import time
import r4a_prepare_frozen_compatibility_probe as helper

CAPABILITY = "ouf.semantic.read"
KC = "/opt/keycloak/bin/kcadm.sh"


def call(args):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=30).stdout.strip()


def summarize_policy(bundle, claims):
    bundle = bundle.get("bundle", bundle)
    descriptors = [d for d in bundle["capabilities"] if d.get("capabilityId") == CAPABILITY]
    grants = [g for g in bundle["grants"] if g.get("capabilityId") == CAPABILITY]
    print("SEM_DESCRIPTOR_COUNT=" + str(len(descriptors)))
    if len(descriptors) == 1:
        d = descriptors[0]
        print("SEM_DESCRIPTOR_SCOPE_MATCH=" + str(d.get("requiredScope") == CAPABILITY).lower())
        print("SEM_DESCRIPTOR_SERVICE_ALLOWED=" + str("SERVICE" in d.get("allowedActors", [])).lower())
    service = [g for g in grants if g.get("servicePrincipalId")]
    print("SEM_GRANT_COUNT=" + str(len(grants)))
    print("SEM_SERVICE_GRANT_COUNT=" + str(len(service)))
    # These are selector matches only, not a reimplementation of owner ALLOW.
    print("SEM_SERVICE_CLIENT_SELECTOR_MATCH_COUNT=" + str(sum(
        g["servicePrincipalId"] == (claims.get("client_id") or claims.get("azp")) for g in service)))
    print("SEM_SERVICE_SUBJECT_SELECTOR_MATCH_COUNT=" + str(sum(
        g["servicePrincipalId"] == claims.get("sub") for g in service)))
    print("SEM_SUBJECT_GRANT_MATCH_COUNT=" + str(sum(
        bool(g.get("subjectId")) and g["subjectId"] == claims.get("sub") for g in grants)))
    print("SEM_CONSTRAINED_SERVICE_GRANT_COUNT=" + str(sum(bool(g.get("constraints")) for g in service)))


def keycloak_inventory(realm):
    def get(path, *extra):
        return json.loads(call(["docker", "exec", "ouf-keycloak", KC, "get", path, "-r", realm, *extra]))
    try:
        scopes = [s for s in get("client-scopes") if s.get("name") == CAPABILITY]
        clients = [c for c in get("clients", "--fields", "id,clientId") if c.get("clientId") == "ouf-ingestion"]
        print("SEM_KC_SCOPE_COUNT=" + str(len(scopes)))
        print("SEM_KC_INGESTION_CLIENT_COUNT=" + str(len(clients)))
        if len(clients) == 1:
            cid = clients[0]["id"]
            for kind in ("default", "optional"):
                assigned = get("clients/" + cid + "/" + kind + "-client-scopes")
                print("SEM_KC_INGESTION_" + kind.upper() + "=" + str(
                    any(s.get("name") == CAPABILITY for s in assigned)).lower())
        print("SEM_KC_INVENTORY=PASS")
    except subprocess.CalledProcessError as error:
        text = (error.stderr or "") + (error.stdout or "")
        code = "KCADM_SESSION_EXPIRED" if "Session has expired" in text else "KCADM_READ_FAILED"
        print("SEM_KC_INVENTORY=BLOCKED CODE=" + code)


def main(args):
    if os.geteuid() != 0:
        raise RuntimeError("ROOT_REQUIRED")
    live = helper.inspect("ouf-ingestion")
    helper.probe_mounts(live)
    settings = helper.transport_settings(live, args.tenant_id)
    mount = next(m for m in live["Mounts"] if m["Destination"] == helper.AUTH)
    relative = Path(settings["ouf.ingestion.activation.token-file"]).relative_to(helper.AUTH)
    directory = Path(mount["Source"]).resolve()
    host = directory.joinpath(relative).resolve()
    if not host.is_relative_to(directory):
        raise RuntimeError("TOKEN_OUTSIDE_MOUNT")
    with host.open("rb") as stream:
        token = stream.read(16385).decode().strip()
    if len(token) > 16384 or token.count(".") != 2:
        raise RuntimeError("TOKEN_INVALID")
    part = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
    print("R4A_SEM_ACCESS_INVENTORY=READ_ONLY")
    print("SEM_TOKEN_SCOPE_PRESENT=" + str(CAPABILITY in str(claims.get("scope", "")).split()).lower())
    print("SEM_TOKEN_TTL_SECONDS=" + str(int(claims.get("exp", 0) - time.time())))
    sql = ("begin read only; select p.bundle_payload::text from ouf_authorization.active_policy_bundle a "
           "join ouf_authorization.policy_bundle p using(bundle_id,version); rollback;")
    bundle = json.loads(call(["docker", "exec", "ouf-postgres", "psql", "-X", "-qAt", "-v",
        "ON_ERROR_STOP=1", "-U", "ouf_onboarding", "-d", "ouf_onboarding", "-c", sql]))
    summarize_policy(bundle, claims)
    keycloak_inventory(args.realm)
    print("R4A_SEM_ACCESS_INVENTORY=COMPLETE VALUES_AND_IDENTITIES_NOT_PRINTED=true LIVE_UNCHANGED=true")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--realm", required=True)
    try:
        main(parser.parse_args())
    except (OSError, ValueError, KeyError, TypeError, IndexError, RuntimeError, subprocess.SubprocessError) as error:
        code = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        print("R4A_SEM_ACCESS_INVENTORY=BLOCKED CODE=" + code + " SECRETS_NOT_PRINTED=true")
        raise SystemExit(1)
