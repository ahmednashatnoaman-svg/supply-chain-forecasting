"""Zero-cost guard: fail CI if a known paid/commercial SDK sneaks into the project.

Scans pyproject.toml + compose files against a denylist. (Nashat, plan Task 6.)
Exit 0 = clean; exit 1 = a paid dependency was found.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Substrings that indicate a paid API/SaaS SDK not allowed in this zero-cost project.
DENYLIST = [
    "openai",
    "anthropic",
    "cohere",
    "databricks",
    "datadog",
    "newrelic",
    "snowflake-connector",
    "confluent-cloud",
    "boto3",  # implies AWS billed usage in this context
]

FILES = [
    ROOT / "pyproject.toml",
    ROOT / "infra" / "docker" / "docker-compose.yml",
]


def main() -> int:
    violations: list[str] = []
    for f in FILES:
        if not f.exists():
            continue
        text = f.read_text().lower()
        for term in DENYLIST:
            # `plaid` is allowed only as the optional sandbox extra; not on the denylist.
            if term in text:
                violations.append(f"{f.relative_to(ROOT)}: found denylisted '{term}'")
    if violations:
        print("ZERO-COST GUARD FAILED — paid dependency detected:")
        for v in violations:
            print("  -", v)
        return 1
    print("Zero-cost guard OK: no paid dependencies found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
