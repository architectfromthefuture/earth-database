from __future__ import annotations

import json
import tempfile
from pathlib import Path
import unittest

from earth_database.observability import JsonlEventLogger
from earth_database.trust.policy import ToolRequest, evaluate_tool_request


class PolicyTests(unittest.TestCase):
    def test_allows_benign_workspace_read(self) -> None:
        decision = evaluate_tool_request(
            ToolRequest(
                tool_name="read_file",
                parameters={"path": "/workspace/README.md"},
                requested_by_trust_zone="trusted_user",
            )
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.risk, "low")

    def test_blocks_sensitive_file_path(self) -> None:
        decision = evaluate_tool_request(
            ToolRequest(
                tool_name="read_file",
                parameters={"path": "~/.ssh/id_rsa"},
                requested_by_trust_zone="trusted_user",
            )
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.risk, "high")

    def test_blocks_unsafe_shell_command(self) -> None:
        decision = evaluate_tool_request(
            ToolRequest(
                tool_name="shell",
                parameters={"command": "sudo rm -rf /tmp/example"},
                requested_by_trust_zone="trusted_user",
            )
        )

        self.assertFalse(decision.allowed)
        self.assertIn("unsafe command", decision.reason)

    def test_blocks_untrusted_origin_even_for_benign_tool(self) -> None:
        decision = evaluate_tool_request(
            ToolRequest(
                tool_name="read_file",
                parameters={"path": "/workspace/README.md"},
                requested_by_trust_zone="untrusted_external",
            )
        )

        self.assertFalse(decision.allowed)
        self.assertIn("untrusted_external", decision.reason)

    def test_logs_policy_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "events.jsonl"
            logger = JsonlEventLogger(log_path)

            evaluate_tool_request(
                ToolRequest(
                    tool_name="shell",
                    parameters={"command": "wget http://example.invalid"},
                    requested_by_trust_zone="trusted_user",
                ),
                logger=logger,
            )

            event = json.loads(log_path.read_text(encoding="utf-8").splitlines()[-1])
            self.assertEqual(event["event"], "tool_request_blocked")
            self.assertEqual(event["stage"], "trust")


if __name__ == "__main__":
    unittest.main()
