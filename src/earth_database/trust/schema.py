"""Trust metadata schema for provenance-first memory ingress."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar


class SourceType(StrEnum):
    USER_INPUT = "user_input"
    SYSTEM_GENERATED = "system_generated"
    INTERNAL_EVENT = "internal_event"
    UPLOADED_FILE = "uploaded_file"
    EXTERNAL_REPO_FILE = "external_repo_file"
    EXTERNAL_WEBPAGE = "external_webpage"
    EXTERNAL_EMAIL = "external_email"
    UNKNOWN = "unknown"


class TrustZone(StrEnum):
    TRUSTED_SYSTEM = "trusted_system"
    TRUSTED_USER = "trusted_user"
    INTERNAL_OBSERVED = "internal_observed"
    UNTRUSTED_EXTERNAL = "untrusted_external"
    HOSTILE_SUSPECTED = "hostile_suspected"
    UNKNOWN = "unknown"


class ContentRole(StrEnum):
    INSTRUCTION = "instruction"
    EVIDENCE = "evidence"
    MEMORY = "memory"
    TOOL_OUTPUT = "tool_output"
    OBSERVATION = "observation"
    POLICY = "policy"


class InjectionRisk(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


EnumT = TypeVar("EnumT", bound=StrEnum)


@dataclass(frozen=True)
class TrustMetadata:
    """Provenance and authority boundaries attached to ingested content."""

    source_type: SourceType = SourceType.UNKNOWN
    trust_zone: TrustZone = TrustZone.UNTRUSTED_EXTERNAL
    content_role: ContentRole = ContentRole.EVIDENCE
    injection_risk: InjectionRisk = InjectionRisk.LOW
    can_instruct: bool = False
    can_call_tools: bool = False
    can_override_policy: bool = False
    provenance_note: str | None = None

    def as_storage_dict(self) -> dict[str, str | bool | None]:
        return {
            "source_type": self.source_type.value,
            "trust_zone": self.trust_zone.value,
            "content_role": self.content_role.value,
            "injection_risk": self.injection_risk.value,
            "can_instruct": self.can_instruct,
            "can_call_tools": self.can_call_tools,
            "can_override_policy": self.can_override_policy,
            "provenance_note": self.provenance_note,
        }


RISK_ORDER: dict[InjectionRisk, int] = {
    InjectionRisk.LOW: 0,
    InjectionRisk.MEDIUM: 1,
    InjectionRisk.HIGH: 2,
}


def coerce_source_type(value: SourceType | str | None) -> SourceType:
    return normalize_enum(SourceType, value, default=SourceType.UNKNOWN)


def coerce_content_role(value: ContentRole | str | None) -> ContentRole:
    return normalize_enum(ContentRole, value, default=ContentRole.EVIDENCE)


def coerce_injection_risk(value: InjectionRisk | str | None) -> InjectionRisk:
    return normalize_enum(InjectionRisk, value, default=InjectionRisk.LOW)


def highest_injection_risk(*risks: InjectionRisk | str | None) -> InjectionRisk:
    coerced = [coerce_injection_risk(risk) for risk in risks]
    return max(coerced, key=lambda risk: RISK_ORDER[risk])


def normalize_enum(enum_type: type[EnumT], value: EnumT | str | None, *, default: EnumT) -> EnumT:
    if isinstance(value, enum_type):
        return value
    if value is None:
        return default
    try:
        return enum_type(value)
    except ValueError:
        return default
