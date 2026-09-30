import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import r4a_execution_route_inventory as inventory


class ExecutionRouteInventoryTests(unittest.TestCase):
    def test_admin_key_parser_selects_admin_and_rejects_ambiguity_and_refs(self):
        text = ('deployment:\n  admin:\n    admin_key:\n'
                '      - name: reader\n        key: private-reader\n        role: viewer\n'
                '      - name: admin\n        key: "private-admin"\n        role: admin\n'
                '    allow_admin:\n      - 127.0.0.1/32\n')
        self.assertEqual(inventory.admin_key(text), "private-admin")
        for changed in (text.replace("role: viewer", "role: admin"),
                        text.replace('"private-admin"', "$ENV://ADMIN_KEY")):
            with self.assertRaisesRegex(RuntimeError, "APISIX_ADMIN_KEY_LAYOUT_UNSUPPORTED"):
                inventory.admin_key(changed)

    def test_exact_get_route_summary_does_not_print_route_secrets(self):
        route = {"uri": inventory.URI, "methods": ["GET"],
                 "plugins": {"proxy-rewrite": {"uri": inventory.LEGACY_PATH},
                             "openid-connect": {"required_scopes": ["ouf.internal.object-storage.read"],
                                                "client_secret": "private-route-secret"}},
                 "upstream": {"nodes": {"ouf-onboarding:8080": 1}}}
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            inventory.summarize([route, {"uri": inventory.URI, "methods": ["POST"]}])
        self.assertIn("EXEC_READ_ROUTE_COUNT=1", output.getvalue())
        self.assertIn("EXEC_READ_OWNER_LEGACY_PATH=true", output.getvalue())
        self.assertIn("EXEC_READ_REQUIRED_SCOPE_MATCH=true", output.getvalue())
        self.assertNotIn("private-route-secret", output.getvalue())

    def test_live_read_keeps_key_on_stdin_and_emits_only_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            key_file = Path(directory) / "key"
            key_file.write_text("private-admin")
            replies = [{"Image": "image-id"},
                       {"Config": {"Labels": {"org.opencontainers.image.revision": inventory.OWNER_REVISION}}},
                       {"State": {"Running": True}}]
            output = io.StringIO()
            with patch.object(inventory.os, "geteuid", return_value=0), \
                 patch.object(inventory.helper, "inspect", side_effect=replies), \
                 patch.object(inventory.helper, "run", return_value=json.dumps({"list": []})) as run, \
                 contextlib.redirect_stdout(output):
                inventory.main(SimpleNamespace(admin_key=key_file))
            argv = run.call_args.args[0]
            self.assertNotIn("private-admin", " ".join(argv))
            self.assertIn('header = "X-API-KEY: private-admin"', run.call_args.kwargs["input"])
            self.assertNotIn("private-admin", output.getvalue())
            self.assertIn("EXEC_READ_ROUTE_COUNT=0", output.getvalue())
            self.assertIn("LIVE_UNCHANGED=true", output.getvalue())

    def test_both_admin_api_response_formats_are_supported(self):
        value = {"uri": inventory.URI}
        self.assertEqual(inventory.route_values({"list": [{"value": value}]}), [value])
        self.assertEqual(inventory.route_values({"node": {"nodes": [{"value": json.dumps(value)}]}}), [value])


if __name__ == "__main__":
    unittest.main()
