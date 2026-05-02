from __future__ import annotations

import unittest

from earth_database.trust.classifier import classify_trust
from earth_database.trust.schema import ContentRole, SourceType, TrustZone


class TrustClassifierTests(unittest.TestCase):
    def test_system_policy_can_override_policy(self) -> None:
        trust = classify_trust(SourceType.SYSTEM_GENERATED, ContentRole.POLICY)

        self.assertEqual(trust.trust_zone, TrustZone.TRUSTED_SYSTEM)
        self.assertTrue(trust.can_instruct)
        self.assertTrue(trust.can_call_tools)
        self.assertTrue(trust.can_override_policy)

    def test_system_generated_non_policy_cannot_override_policy(self) -> None:
        trust = classify_trust("system_generated", "observation")

        self.assertEqual(trust.trust_zone, TrustZone.TRUSTED_SYSTEM)
        self.assertFalse(trust.can_override_policy)

    def test_user_input_can_instruct_but_not_override_policy(self) -> None:
        trust = classify_trust("user_input")

        self.assertEqual(trust.trust_zone, TrustZone.TRUSTED_USER)
        self.assertTrue(trust.can_instruct)
        self.assertFalse(trust.can_call_tools)
        self.assertFalse(trust.can_override_policy)

    def test_internal_event_is_observed_not_instructional(self) -> None:
        trust = classify_trust("internal_event")

        self.assertEqual(trust.trust_zone, TrustZone.INTERNAL_OBSERVED)
        self.assertEqual(trust.content_role, ContentRole.OBSERVATION)
        self.assertFalse(trust.can_instruct)

    def test_external_sources_cannot_instruct_or_call_tools(self) -> None:
        for source_type in (
            "uploaded_file",
            "external_repo_file",
            "external_webpage",
            "external_email",
        ):
            with self.subTest(source_type=source_type):
                trust = classify_trust(source_type, "instruction")
                self.assertEqual(trust.trust_zone, TrustZone.UNTRUSTED_EXTERNAL)
                self.assertFalse(trust.can_instruct)
                self.assertFalse(trust.can_call_tools)
                self.assertFalse(trust.can_override_policy)

    def test_unknown_source_is_not_authoritative(self) -> None:
        trust = classify_trust("legacy")

        self.assertEqual(trust.source_type, SourceType.UNKNOWN)
        self.assertEqual(trust.trust_zone, TrustZone.UNKNOWN)
        self.assertFalse(trust.can_instruct)


if __name__ == "__main__":
    unittest.main()
