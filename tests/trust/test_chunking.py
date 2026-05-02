from __future__ import annotations

import unittest

from earth_database.trust.chunking import chunk_text, estimate_token_count
from earth_database.trust.schema import ContentRole, SourceType, TrustMetadata, TrustZone


class ChunkingTests(unittest.TestCase):
    def test_chunk_text_preserves_trust_on_every_chunk(self) -> None:
        trust = TrustMetadata(
            source_type=SourceType.EXTERNAL_WEBPAGE,
            trust_zone=TrustZone.UNTRUSTED_EXTERNAL,
            content_role=ContentRole.EVIDENCE,
        )

        chunks = chunk_text("alpha beta gamma delta epsilon", trust, max_chars=12, overlap_chars=2)

        self.assertGreater(len(chunks), 1)
        self.assertEqual([chunk.chunk_index for chunk in chunks], list(range(len(chunks))))
        self.assertTrue(all(chunk.trust == trust for chunk in chunks))
        self.assertTrue(all(chunk.token_count >= 1 for chunk in chunks))

    def test_chunker_rejects_invalid_overlap(self) -> None:
        with self.assertRaises(ValueError):
            chunk_text("content", TrustMetadata(), max_chars=10, overlap_chars=10)

    def test_token_estimate_is_deterministic_and_nonzero_for_content(self) -> None:
        self.assertEqual(estimate_token_count(""), 0)
        self.assertEqual(estimate_token_count("abcd"), 1)
        content = "one two three four five"
        self.assertGreaterEqual(estimate_token_count(content), 5)
        self.assertEqual(estimate_token_count(content), estimate_token_count(content))


if __name__ == "__main__":
    unittest.main()
