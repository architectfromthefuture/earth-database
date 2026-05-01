"""Deterministic source and authority classification."""

from __future__ import annotations

from earth_database.trust.schema import (
    ContentRole,
    InjectionRisk,
    SourceType,
    TrustMetadata,
    TrustZone,
    coerce_content_role,
    coerce_source_type,
)


def classify_trust(
    source_type: SourceType | str | None,
    content_role: ContentRole | str | None = None,
    provenance_note: str | None = None,
) -> TrustMetadata:
    """Classify content provenance before it becomes memory."""

    normalized_source = coerce_source_type(source_type)
    normalized_role = coerce_content_role(content_role) or ContentRole.EVIDENCE

    if normalized_source == SourceType.SYSTEM_GENERATED:
        trust_zone = TrustZone.TRUSTED_SYSTEM
        can_instruct = normalized_role in {ContentRole.INSTRUCTION, ContentRole.POLICY}
        can_call_tools = normalized_role == ContentRole.POLICY
        can_override_policy = normalized_role == ContentRole.POLICY
    elif normalized_source == SourceType.USER_INPUT:
        trust_zone = TrustZone.TRUSTED_USER
        can_instruct = True
        can_call_tools = False
        can_override_policy = False
    elif normalized_source == SourceType.INTERNAL_EVENT:
        trust_zone = TrustZone.INTERNAL_OBSERVED
        can_instruct = False
        can_call_tools = False
        can_override_policy = False
        if normalized_role == ContentRole.EVIDENCE:
            normalized_role = ContentRole.OBSERVATION
    elif normalized_source == SourceType.UNKNOWN:
        trust_zone = TrustZone.UNKNOWN
        can_instruct = False
        can_call_tools = False
        can_override_policy = False
    else:
        trust_zone = TrustZone.UNTRUSTED_EXTERNAL
        can_instruct = False
        can_call_tools = False
        can_override_policy = False

    return TrustMetadata(
        source_type=normalized_source,
        trust_zone=trust_zone,
        content_role=normalized_role,
        injection_risk=InjectionRisk.LOW,
        can_instruct=can_instruct,
        can_call_tools=can_call_tools,
        can_override_policy=can_override_policy,
        provenance_note=provenance_note,
    )
