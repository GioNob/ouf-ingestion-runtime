#!/usr/bin/env python3
"""Build a pinned candidate and run the real managed consumer in an isolated JVM.

Only GET Gateway requests and read-only SQL are used. No live switch, migration,
run, ACK, policy change, approval or compatibility-attestation POST is performed.
"""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import uuid
from urllib.parse import urlsplit

BASE = "e3f04f1d8ed47a62cde8b9c7831882c9cea17169"
BRANCH = "codex/r4a-ingestion-frozen-compatibility-probe"
ROOT = Path("/opt/ouf/r4a-stage")
MANIFEST = ROOT / "identity-images.json"
PROPERTIES = "/run/secrets/ingestion-summary.properties"
AUTH = "/run/ouf-ingestion-auth"


def run(args, *, input=None, timeout=60):
    return subprocess.run(args, input=input, check=True, capture_output=True,
                          text=True, timeout=timeout).stdout.strip()


def inspect(name, kind="container"):
    return json.loads(run(["docker", "inspect", "--type", kind, name]))[0]


def migration_hashes(worktree):
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (worktree / "src/main/resources/db/migration").glob("V*__*.sql")}


def candidate_row(source, version, expected_hash):
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,160}", source):
        raise RuntimeError("SOURCE_ID_INVALID")
    version = str(uuid.UUID(version))
    # Identifiers are validated above; no content, identity or credentials are printed.
    query = ("begin read only; select json_build_object('sourceId',source_id,"
             "'onboardingVersionId',onboarding_version_id,'version',version,"
             "'state',state,'configurationHash',configuration_hash,"
             "'configuration',configuration)::text from ouf_onboarding.onboarding_version "
             f"where source_id='{source}' and onboarding_version_id='{version}'; rollback;")
    result = run(["docker", "exec", "ouf-postgres", "psql", "-X", "-qAt", "-v",
                  "ON_ERROR_STOP=1", "-U", "ouf_onboarding", "-d", "ouf_onboarding", "-c", query])
    row = json.loads(result)
    if row.get("state") not in {"IN_REVIEW", "APPROVED"}:
        raise RuntimeError("VERSION_NOT_FROZEN")
    if row.get("configurationHash") != expected_hash:
        raise RuntimeError("FROZEN_CONFIGURATION_DRIFT")
    return row


def probe_mounts(live, transport=None):
    if (not live["State"]["Running"] or live["Config"]["User"] != "10002:10002"
            or live["HostConfig"]["NetworkMode"] != "ouf-backend"
            or set(live["NetworkSettings"]["Networks"]) != {"ouf-backend"}):
        raise RuntimeError("LIVE_RUNTIME_SETTINGS_DRIFT")
    found = {m["Destination"]: m for m in live["Mounts"]}
    mounts = []
    for target in (AUTH, PROPERTIES):
        m = found.get(target)
        if not m or m["Type"] != "bind" or any(x in m["Source"] for x in (",", "\n", "\r")):
            raise RuntimeError("PROBE_TRANSPORT_MOUNT_MISSING")
        source = str(transport) if target == PROPERTIES and transport is not None else m["Source"]
        if any(x in source for x in (",", "\n", "\r")):
            raise RuntimeError("PROBE_TRANSPORT_MOUNT_MISSING")
        mounts.extend(["--mount", f"type=bind,src={source},dst={target},readonly"])
    return mounts


def literal_property(text, key):
    values = re.findall(r"^[ \t]*" + re.escape(key) + r"[ \t]*[=:][ \t]*(.*)$", text, re.MULTILINE)
    if len(values) != 1:
        raise RuntimeError("ING_COMPAT_TRANSPORT_PROPERTY_MISSING_OR_DUPLICATED")
    value = values[0].strip()
    if not value or any(x in value for x in ("${", "\\", "\n", "\r")):
        raise RuntimeError("ING_COMPAT_TRANSPORT_PROPERTY_NOT_LITERAL")
    return value


def transport_settings(live, tenant):
    """Use existing references, never a distroless exec/cat or a new credential.

    Decoded claims are diagnostic checks, not authentication; Gateway still
    validates the bearer and owner authorization on every actual consumer GET.
    """
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,160}", tenant):
        raise RuntimeError("ING_COMPAT_TENANT_INVALID")
    found = {m["Destination"]: m for m in live["Mounts"]}
    text = Path(found[PROPERTIES]["Source"]).read_text()
    registry = urlsplit(literal_property(text, "ouf.authorization.registry-url"))
    if (registry.scheme != "https" or not registry.hostname or registry.username is not None
            or registry.password is not None or registry.query or registry.fragment):
        raise RuntimeError("ING_COMPAT_REGISTRY_URL_INVALID")
    token_path = literal_property(text, "ouf.authorization.registry-token-file")
    relative = Path(token_path).relative_to(AUTH)
    if not relative.parts or ".." in relative.parts:
        raise RuntimeError("ING_COMPAT_TOKEN_PATH_INVALID")
    directory = Path(found[AUTH]["Source"]).resolve()
    host = directory.joinpath(relative).resolve()
    if not host.is_relative_to(directory) or not host.is_file():
        raise RuntimeError("ING_COMPAT_TOKEN_FILE_MISSING_OR_OUTSIDE_MOUNT")
    with host.open("rb") as stream:
        raw = stream.read(16385)
    if len(raw) > 16384:
        raise RuntimeError("ING_COMPAT_TOKEN_INVALID")
    token = raw.decode().strip()
    if token.count(".") != 2 or any(x.isspace() for x in token):
        raise RuntimeError("ING_COMPAT_TOKEN_INVALID")
    part = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
    if (claims.get("ouf_actor_type") != "SERVICE" or claims.get("tenant_id") != tenant
            or (claims.get("client_id") or claims.get("azp")) != "ouf-ingestion"
            or type(claims.get("exp")) is not int or claims["exp"] - time.time() < 60
            or "ouf.internal.object-storage.read" not in str(claims.get("scope", "")).split()):
        raise RuntimeError("ING_COMPAT_TOKEN_DIAGNOSTIC_MISMATCH")
    return {"ouf.ingestion.activation.gateway-url": registry.scheme + "://" + registry.netloc,
            "ouf.ingestion.activation.token-file": token_path,
            "ouf.ingestion.activation.tenant-id": tenant}


def write_transport(settings):
    fd, name = tempfile.mkstemp(prefix="ingestion-probe-transport-", suffix=".properties", dir=ROOT)
    path = Path(name)
    try:
        with os.fdopen(fd, "w") as output:
            os.fchown(output.fileno(), 0, 10002)
            os.fchmod(output.fileno(), 0o440)
            output.write("".join(key + "=" + value + "\n" for key, value in settings.items()))
        return path
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def validate_proof(proof, candidate):
    if (proof.get("schema") != "ouf.ingestion.compatibility-probe.v1"
            or proof.get("status") != "PASS" or proof.get("attestationSubmitted") is not False
            or proof.get("configurationHash") != candidate["configurationHash"]
            or proof.get("onboardingVersionId") != candidate["onboardingVersionId"]
            or type(proof.get("validatedRows")) is not int or proof["validatedRows"] < 1):
        raise RuntimeError("CONSUMER_PROOF_INVALID")


def main(args):
    if os.geteuid() != 0:
        raise RuntimeError("ROOT_REQUIRED")
    if not re.fullmatch(r"[a-f0-9]{40}", args.revision):
        raise RuntimeError("REVISION_INVALID")
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", args.expected_hash):
        raise RuntimeError("EXPECTED_HASH_INVALID")
    os.umask(0o077)
    live = inspect("ouf-ingestion")
    mounts = probe_mounts(live)
    transport_settings(live, args.tenant_id)
    print("R4A_ING_COMPAT_TRANSPORT=PASS PREBUILD=true LIVE_CONFIG_CHANGED=false", flush=True)
    stage = json.loads(MANIFEST.read_text())
    old = stage["modules"]["ingestion"]
    if stage.get("schema") != "ouf.r4a.identity-images.v1" or old["commit"] != BASE:
        raise RuntimeError("BASE_STAGE_DRIFT")
    base_worktree = Path(old["worktree"])
    if run(["git", "-C", str(base_worktree), "rev-parse", "HEAD"]) != BASE:
        raise RuntimeError("BASE_WORKTREE_DRIFT")
    if run(["git", "-C", str(base_worktree), "status", "--porcelain"]):
        raise RuntimeError("BASE_WORKTREE_DIRTY")
    candidate = candidate_row(args.source, args.version, args.expected_hash)
    # Refresh the bounded diagnostic after the build; the token rotates in its
    # existing directory mount. Never copy the bearer into the transport file.
    transport = write_transport(transport_settings(current, args.tenant_id))
    mounts = probe_mounts(current, transport)
    run(["git", "-C", str(base_worktree), "fetch", "--no-tags", "origin", BRANCH], timeout=180)
    if run(["git", "-C", str(base_worktree), "rev-parse", "FETCH_HEAD"]) != args.revision:
        raise RuntimeError("REMOTE_HEAD_DRIFT")
    run(["git", "-C", str(base_worktree), "merge-base", "--is-ancestor", BASE, args.revision])
    worktree = ROOT / ("ingestion-compatibility-" + args.revision[:12])
    if worktree.exists():
        if (run(["git", "-C", str(worktree), "rev-parse", "HEAD"]) != args.revision
                or run(["git", "-C", str(worktree), "status", "--porcelain"])):
            raise RuntimeError("CANDIDATE_WORKTREE_DRIFT")
    else:
        run(["git", "-C", str(base_worktree), "worktree", "add", "--detach", str(worktree), args.revision])
    if not migration_hashes(worktree) or migration_hashes(worktree) != migration_hashes(base_worktree):
        raise RuntimeError("MIGRATIONS_CHANGED_CLONE_GATE_REQUIRED")
    flyway = run(["docker", "exec", "ouf-postgres", "psql", "-X", "-qAt", "-v",
                  "ON_ERROR_STOP=1", "-U", "ouf_ingestion", "-d", "ouf_ingestion", "-c",
                  "select version from ouf_ingestion.flyway_schema_history order by installed_rank desc limit 1"])
    if flyway != "14":
        raise RuntimeError("LIVE_SCHEMA_DRIFT")
    image_tag = "ouf-ingestion:r4a-compatibility-" + args.revision[:12]
    log = ROOT / ("ingestion-compatibility-build-" + args.revision[:12] + ".log")
    print("R4A_ING_COMPAT_PREPARE=BUILDING LIVE_SWITCH=false ATTESTATION_POST=false", flush=True)
    # Build logs are retained privately; stdout only reports safe stage markers.
    with log.open("w") as output:
        subprocess.run(["docker", "build", "--label", "org.opencontainers.image.revision=" + args.revision,
                        "--tag", image_tag, str(worktree)], check=True, stdout=output,
                       stderr=subprocess.STDOUT, timeout=1800)
    image = inspect(image_tag, "image")
    if (image["Config"].get("User") != "10002:10002"
            or image["Config"].get("Labels", {}).get("org.opencontainers.image.revision") != args.revision):
        raise RuntimeError("CANDIDATE_IMAGE_PROVENANCE_INVALID")
    current = inspect("ouf-ingestion")
    if current["Id"] != live["Id"] or current["Image"] != live["Image"]:
        raise RuntimeError("LIVE_CHANGED_DURING_PREPARE")
    probe_mounts(current)
    candidate = candidate_row(args.source, args.version, args.expected_hash)
    probe_name = "ouf-ingestion-compatibility-probe-" + uuid.uuid4().hex
    command = ["docker", "run", "--rm", "--name", probe_name, "--interactive", "--network", "ouf-backend",
               "--user", "10002:10002", "--read-only", "--cap-drop", "ALL",
               "--security-opt", "no-new-privileges", "--pids-limit", "128",
               "--memory", "1g", "--cpus", "1", "--log-driver", "none",
               "--tmpfs", "/tmp:rw,nosuid,nodev,size=128m,mode=1777", *mounts,
               "--entrypoint", "java", image["Id"],
               "-Dloader.main=it.comune.trieste.ouf.ingestion.FrozenConfigurationProbeMain",
               "-cp", "/app/app.jar", "org.springframework.boot.loader.launch.PropertiesLauncher",
               "--properties", PROPERTIES]
    # No OUF_ING_DB_* credentials and no data-plane persistence port are supplied.
    try:
        completed = subprocess.run(command, input=json.dumps(candidate), capture_output=True,
                                   text=True, timeout=120)
    finally:
        # Only this invocation's disposable container may be removed, including on timeout.
        try:
            subprocess.run(["docker", "rm", "--force", probe_name], capture_output=True,
                           text=True, timeout=30)
        finally:
            transport.unlink(missing_ok=True)
    try:
        proof = json.loads(completed.stdout)
    except ValueError:
        raise RuntimeError("CONSUMER_PROBE_OUTPUT_INVALID") from None
    if completed.returncode:
        code = proof.get("code")
        raise RuntimeError(code if isinstance(code, str) and re.fullmatch(r"ING_[A-Z0-9_]{1,80}", code)
                           else "CONSUMER_PROBE_FAILED")
    validate_proof(proof, candidate)
    after = inspect("ouf-ingestion")
    if after["Id"] != live["Id"] or after["Image"] != live["Image"] or not after["State"]["Running"]:
        raise RuntimeError("LIVE_CHANGED_DURING_PROBE")
    candidate_row(args.source, args.version, args.expected_hash)
    proof.update({"candidateCommit": args.revision, "candidateImageId": image["Id"],
                  "liveImageId": live["Image"], "candidateDeployed": False, "worktree": str(worktree)})
    fd, temporary = tempfile.mkstemp(prefix=".ingestion-proof-", dir=ROOT)
    with os.fdopen(fd, "w") as out:
        json.dump(proof, out, sort_keys=True, indent=2)
        out.write("\n")
    Path(temporary).replace(ROOT / "ingestion-compatibility-probe.json")
    print("R4A_ING_COMPAT_PROBE=PASS VALIDATED_ROWS=" + str(proof["validatedRows"]), flush=True)
    print("R4A_ING_COMPAT_CANDIDATE_COMMIT=" + args.revision, flush=True)
    print("R4A_ING_COMPAT_CANDIDATE_DEPLOYED=false LIVE_CONTAINER_UNCHANGED=true ATTESTATION_POST=false", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("revision", "source", "version", "expected-hash", "tenant-id"):
        parser.add_argument("--" + option, required=True)
    arguments = parser.parse_args()
    try:
        main(arguments)
    except (OSError, ValueError, KeyError, TypeError, IndexError, RuntimeError,
            subprocess.SubprocessError) as error:
        code = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        print("R4A_ING_COMPAT_PREPARE=BLOCKED CODE=" + code + " ATTESTATION_POST=false SECRETS_NOT_PRINTED=true")
        raise SystemExit(1)
