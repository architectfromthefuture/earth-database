"""Deterministic policy gate for tool requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from earth_database.observability import JsonlEventLogger
from earth_database.trust.schema import TrustZone, normalize_enum


@dataclass(frozen=True)
class ToolRequest:
    tool_name: str
    parameters: dict[str, Any]
    requested_by_trust_zone: str | None = None


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reason: str
    risk: str


BLOCKED_PATH_PARTS = (
    "~/.ssh",
    ".env",
    "/etc/",
    "/root/",
    "id_rsa",
    "id_ed25519",
)

BLOCKED_COMMAND_PARTS = (
    "rm -rf",
    "sudo",
    "curl",
    "wget",
    "chmod +x",
    "nc",
    "bash -c",
)

BENIGN_TOOLS = ("read", "search", "retrieve", "list")


def evaluate_tool_request(
    request: ToolRequest,
    *,
    logger: JsonlEventLogger | None = None,
) -> PolicyDecision:
    trust_zone = normalize_enum(
        TrustZone,
        request.requested_by_trust_zone,
        default=TrustZone.UNKNOWN,
    )
    if trust_zone in {TrustZone.UNTRUSTED_EXTERNAL, TrustZone.HOSTILE_SUSPECTED}:
        decision = PolicyDecision(
            allowed=False,
            reason=f"tool requests from {trust_zone.value} content are blocked",
            risk="high",
        )
        _emit_policy_event(logger, request, decision)
        return decision

    parameter_text = _flatten_parameters(request.parameters)
    lowered = parameter_text.lower()
    for blocked in BLOCKED_PATH_PARTS:
        if blocked in lowered:
            decision = PolicyDecision(
                allowed=False,
                reason=f"blocked sensitive path pattern: {blocked}",
                risk="high",
            )
            _emit_policy_event(logger, request, decision)
            return decision
    for blocked in BLOCKED_COMMAND_PARTS:
        if blocked in lowered:
            decision = PolicyDecision(
                allowed=False,
                reason=f"blocked unsafe command pattern: {blocked}",
                risk="high",
            )
            _emit_policy_event(logger, request, decision)
            return decision

    tool_name = request.tool_name.lower()
    if any(part in tool_name for part in BENIGN_TOOLS):
        decision = PolicyDecision(
            allowed=True,
            reason="benign read/search request allowed inside workspace",
            risk="low",
        )
        _emit_policy_event(logger, request, decision)
        return decision
    decision = PolicyDecision(
        allowed=True,
        reason="no deterministic policy block matched",
        risk="low",
    )
    _emit_policy_event(logger, request, decision)
    return decision


def _flatten_parameters(parameters: dict[str, Any]) -> str:
    parts: list[str] = []
    for key, value in parameters.items():
        parts.append(str(key))
        if isinstance(value, dict):
            parts.append(_flatten_parameters(value))
        elif isinstance(value, (list, tuple, set)):
            parts.extend(str(item) for item in value)
        else:
            parts.append(str(value))
    return " ".join(parts)


def _emit_policy_event(
    logger: JsonlEventLogger | None,
    request: ToolRequest,
    decision: PolicyDecision,
) -> None:
    if logger is None:
        return
    logger.emit(
        stage="trust",
        event="tool_request_allowed" if decision.allowed else "tool_request_blocked",
        payload={
            "tool_name": request.tool_name,
            "trust_zone": request.requested_by_trust_zone,
            "reason": decision.reason,
            "risk": decision.risk,
        },
    )
