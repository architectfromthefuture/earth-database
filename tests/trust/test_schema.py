from __future__ import annotations

import unittest

from earth_database.trust.schema import ContentRole, SourceType, TrustMetadata, TrustZone


class TrustSchemaTests(unittest.TestCase):
    def test_defaults_are_defensive(self) -> None:
        trust = TrustMetadata()

        self.assertEqual(trust.source_type, SourceType.UNKNOWN)
        self.assertEqual(trust.trust_zone, TrustZone.UNTRUSTED_EXTERNAL)
        self.assertEqual(trust.content_role, ContentRole.EVIDENCE)
        self.assertFalse(trust.can_instruct)
        self.assertFalse(trust.can_call_tools)
        self.assertFalse(trust.can_override_policy)


if __name__ == "__main__":
    unittest.main()
