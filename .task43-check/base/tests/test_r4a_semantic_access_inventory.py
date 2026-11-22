import contextlib
import io
import subprocess
import sys
from pathlib import Path
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import r4a_semantic_access_inventory as inventory


class SemanticAccessInventoryTests(unittest.TestCase):
    def test_counts_do_not_project_policy_or_principal_values(self):
        claims = {"sub": "private-subject", "azp": "private-client"}
        bundle = {"capabilities": [{"capabilityId": inventory.CAPABILITY,
                  "requiredScope": inventory.CAPABILITY, "allowedActors": ["HUMAN", "SERVICE"]}],
                  "grants": [{"capabilityId": inventory.CAPABILITY,
                  "servicePrincipalId": "private-client", "subjectId": "private-subject",
                  "constraints": {"allowedDataLabels": ["RESTRICTED"]}}]}
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            inventory.summarize_policy(bundle, claims)
        text = output.getvalue()
        self.assertIn("SEM_SERVICE_CLIENT_SELECTOR_MATCH_COUNT=1", text)
        self.assertIn("SEM_SUBJECT_GRANT_MATCH_COUNT=1", text)
        self.assertIn("SEM_CONSTRAINED_SERVICE_GRANT_COUNT=1", text)
        for value in ("private-subject", "private-client", "RESTRICTED", "APPLICABLE_GRANT"):
            self.assertNotIn(value, text)

    def test_keycloak_expired_session_prints_only_safe_marker(self):
        output = io.StringIO()
        error = subprocess.CalledProcessError(1, ["command"], output="private-token",
                                            stderr="Session has expired private-secret")
        with patch.object(inventory, "call", side_effect=error), contextlib.redirect_stdout(output):
            inventory.keycloak_inventory("realm")
        self.assertEqual(output.getvalue(), "SEM_KC_INVENTORY=BLOCKED CODE=KCADM_SESSION_EXPIRED\n")

    def test_keycloak_scope_binding_uses_exact_names_without_writes(self):
        replies = ["[{\"name\":\"ouf.semantic.read\",\"id\":\"scope\"}]",
                   "[{\"clientId\":\"ouf-ingestion\",\"id\":\"client\"}]",
                   "[]", "[{\"name\":\"ouf.semantic.read\"}]"]
        output = io.StringIO()
        with patch.object(inventory, "call", side_effect=replies) as call, contextlib.redirect_stdout(output):
            inventory.keycloak_inventory("realm")
        self.assertIn("SEM_KC_INGESTION_DEFAULT=false", output.getvalue())
        self.assertIn("SEM_KC_INGESTION_OPTIONAL=true", output.getvalue())
        for args in call.call_args_list:
            self.assertEqual(args.args[0][4], "get")


if __name__ == "__main__":
    unittest.main()
