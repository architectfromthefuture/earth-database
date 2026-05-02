"""Deterministic prompt-injection tripwires."""

from __future__ import annotations

from earth_database.trust.schema import InjectionRisk


HIGH_RISK_PATTERNS = (
    "ignore previous instructions",
    "ignore all prior instructions",
    "ignore all instructions",
    "disregard previous instructions",
    "forget previous instructions",
    "system prompt",
    "developer message",
    "show hidden prompt",
    "print system message",
    "reveal secrets",
    "print secrets",
    "exfiltrate",
    "send the contents",
    "upload to",
    "post to",
    "override policy",
    "disable safety",
    "cat ~/.ssh",
    "cat .env",
    "read ~/.ssh",
    "read .env",
    "curl http",
    "wget http",
    "fetch http",
    "rm -rf",
    "chmod +x",
    "python -c",
    "sh -c",
    "powershell",
    "base64 -d",
    " env",
    "env ",
    "printenv",
    "aws_access_key",
    "github_token",
    "api_key",
)

MEDIUM_RISK_PATTERNS = (
    "you are now",
    "act as",
    "send to",
    "base64 decode",
)


def find_prompt_injection_indicators(content: str) -> list[str]:
    """Return deterministic prompt-injection indicators found in ``content``."""

    normalized = content.casefold()
    indicators: list[str] = []
    for pattern in (*HIGH_RISK_PATTERNS, *MEDIUM_RISK_PATTERNS):
        if pattern in normalized:
            indicators.append(pattern)
    return indicators


def scan_prompt_injection_risk(content: str) -> InjectionRisk:
    """Classify prompt-injection risk with auditable string matching."""

    normalized = content.casefold()
    if any(pattern in normalized for pattern in HIGH_RISK_PATTERNS):
        return InjectionRisk.HIGH
    if any(pattern in normalized for pattern in MEDIUM_RISK_PATTERNS):
        return InjectionRisk.MEDIUM
    return InjectionRisk.LOW
