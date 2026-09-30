import importlib.util
import base64
import json
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/r4a_prepare_frozen_compatibility_probe.py"
spec = importlib.util.spec_from_file_location("probe", SCRIPT)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class FrozenProbeGateTests(unittest.TestCase):
    def transport_fixture(self, root, **overrides):
        root = Path(root)
        auth = root / "auth"
        auth.mkdir(exist_ok=True)
        claims = {"ouf_actor_type": "SERVICE", "tenant_id": "tenant-test",
                  "azp": "ouf-ingestion", "exp": int(time.time()) + 300,
                  "scope": "ouf.internal.object-storage.read"}
        claims.update(overrides)
        encoded = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
        token = "header." + encoded + ".signature"
        (auth / "token").write_text(token)
        props = root / "summary.properties"
        original = ("ouf.authorization.registry-url=https://gateway.example.invalid/internal/policy\n"
                    "ouf.authorization.registry-token-file=" + probe.AUTH + "/token\n")
        props.write_text(original)
        live = self.live()
        live["Mounts"][0]["Source"] = str(auth)
        live["Mounts"][1]["Source"] = str(props)
        return live, props, original, token

    def test_transport_reuses_references_without_copying_token_or_mutating_live_properties(self):
        with tempfile.TemporaryDirectory() as root:
            live, props, original, token = self.transport_fixture(root)
            with patch.object(probe, "run") as command:
                settings = probe.transport_settings(live, "tenant-test")
                command.assert_not_called()
            self.assertEqual(settings["ouf.ingestion.activation.gateway-url"], "https://gateway.example.invalid")
            self.assertEqual(settings["ouf.ingestion.activation.token-file"], probe.AUTH + "/token")
            self.assertNotIn(token, str(settings))
            self.assertEqual(props.read_text(), original)
            mounts = probe.probe_mounts(live, Path(root) / "probe.properties")
            self.assertIn("src=" + root + "/probe.properties,dst=" + probe.PROPERTIES + ",readonly", " ".join(mounts))
            self.assertNotIn(str(props), " ".join(mounts))

    def test_wrong_service_tenant_expiry_or_scope_blocks_before_build(self):
        for override in ({"ouf_actor_type": "HUMAN"}, {"tenant_id": "other"},
                         {"azp": "other"}, {"exp": 1}, {"scope": "authorization.bundle.read"}):
            with tempfile.TemporaryDirectory() as root:
                live, _, _, _ = self.transport_fixture(root, **override)
                with self.assertRaisesRegex(RuntimeError, "ING_COMPAT_TOKEN_DIAGNOSTIC_MISMATCH"):
                    probe.transport_settings(live, "tenant-test")

    def test_transport_blocks_token_symlink_escape_and_missing_file(self):
        with tempfile.TemporaryDirectory() as root:
            live, _, _, _ = self.transport_fixture(root)
            token = Path(root) / "auth/token"
            token.unlink()
            with self.assertRaisesRegex(RuntimeError, "MISSING_OR_OUTSIDE_MOUNT"):
                probe.transport_settings(live, "tenant-test")
            outside = Path(root) / "outside"
            outside.write_text("secret")
            token.symlink_to(outside)
            with self.assertRaisesRegex(RuntimeError, "MISSING_OR_OUTSIDE_MOUNT"):
                probe.transport_settings(live, "tenant-test")

    def test_transport_properties_fail_closed_for_duplicates_and_placeholders(self):
        for text in ("k=${ENV}", "k=", "k=value\\", "k=one\nk=two", "other=value"):
            with self.assertRaises(RuntimeError):
                probe.literal_property(text, "k")

    def live(self):
        return {"State": {"Running": True}, "Config": {"User": "10002:10002"},
                "HostConfig": {"NetworkMode": "ouf-backend"},
                "NetworkSettings": {"Networks": {"ouf-backend": {}}},
                "Mounts": [{"Destination": d, "Source": "/private/" + str(i),
                            "Type": "bind", "RW": True}
                           for i, d in enumerate((probe.AUTH, probe.PROPERTIES))]}

    def test_mounts_are_readonly_and_do_not_include_database_or_receipt_secret(self):
        args = probe.probe_mounts(self.live())
        self.assertEqual(args.count("--mount"), 2)
        self.assertTrue(all("readonly" in args[i + 1] for i, x in enumerate(args) if x == "--mount"))
        self.assertNotIn("receipt", " ".join(args))

    def test_foreign_network_or_uid_is_blocked(self):
        for section, key, value in [("Config", "User", "0:0"),
                                    ("HostConfig", "NetworkMode", "host")]:
            live = self.live(); live[section][key] = value
            with self.assertRaisesRegex(RuntimeError, "LIVE_RUNTIME_SETTINGS_DRIFT"):
                probe.probe_mounts(live)

    def test_mount_injection_and_volume_mounts_are_blocked(self):
        for key, value in [("Source", "/file,readonly=false"), ("Type", "volume")]:
            live = self.live(); live["Mounts"][0][key] = value
            with self.assertRaisesRegex(RuntimeError, "PROBE_TRANSPORT_MOUNT_MISSING"):
                probe.probe_mounts(live)

    def test_frozen_version_and_exact_hash_are_required_without_printing_configuration(self):
        version = "68394f42-5c82-4127-a1f3-126516665749"
        row = {"state": "IN_REVIEW", "configurationHash": "sha256:expected"}
        with patch.object(probe, "run", return_value=json.dumps(row)) as command:
            self.assertEqual(probe.candidate_row("source", version, "sha256:expected"), row)
            sql = command.call_args.args[0][-1]
            self.assertIn("begin read only", sql)
            self.assertIn("rollback", sql)
            with self.assertRaisesRegex(RuntimeError, "FROZEN_CONFIGURATION_DRIFT"):
                probe.candidate_row("source", version, "sha256:changed")
        row["state"] = "DRAFT"
        with patch.object(probe, "run", return_value=json.dumps(row)):
            with self.assertRaisesRegex(RuntimeError, "VERSION_NOT_FROZEN"):
                probe.candidate_row("source", version, "sha256:expected")

    def test_sql_injection_is_rejected_before_any_command(self):
        with patch.object(probe, "run") as command:
            with self.assertRaisesRegex(RuntimeError, "SOURCE_ID_INVALID"):
                probe.candidate_row("x';drop table x;--", "unused", "unused")
            command.assert_not_called()

    def test_proof_must_be_for_the_exact_version_and_hash_without_post(self):
        candidate = {"onboardingVersionId": "version", "configurationHash": "hash"}
        good = {"schema": "ouf.ingestion.compatibility-probe.v1", "status": "PASS",
                **candidate, "attestationSubmitted": False, "validatedRows": 8}
        probe.validate_proof(good, candidate)
        for key, value in [("onboardingVersionId", "other"), ("configurationHash", "other"),
                           ("validatedRows", True), ("validatedRows", 0),
                           ("attestationSubmitted", True), ("status", "BLOCKED")]:
            with self.assertRaisesRegex(RuntimeError, "CONSUMER_PROOF_INVALID"):
                probe.validate_proof({**good, key: value}, candidate)


if __name__ == "__main__":
    unittest.main()
