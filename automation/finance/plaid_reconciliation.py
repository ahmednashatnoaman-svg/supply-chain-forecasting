"""OPTIONAL supplier-payment reconciliation via Plaid **Sandbox** (free, zero-cost).

Off the critical path. Before dispatching a large auto-reorder, optionally verify that funds are
available in the operating account. Uses Plaid's free Sandbox environment only.

Security rules (from the plaid-fintech skill) enforced here:
  * Credentials come from env vars — NEVER hard-coded (PLAID_CLIENT_ID, PLAID_SECRET, PLAID_ENV=sandbox).
  * Use REAL-TIME balance (accounts/balance/get) for a payment decision, not cached balances.
  * Access tokens are sensitive — store encrypted at rest (not persisted in this scaffold).
  * Disabled unless PLAID_ENABLED=true, so the platform stays zero-cost by default.

Install the optional dep with:  pip install "supply-chain-forecasting[finance]"  (adds `plaid-python`).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ReconciliationResult:
    approved: bool
    available: float | None
    requested: float
    reason: str


def is_enabled() -> bool:
    """True only when explicitly enabled AND pointed at the free sandbox."""
    return os.getenv("PLAID_ENABLED", "false").lower() == "true" and os.getenv("PLAID_ENV", "sandbox") == "sandbox"


def verify_funds(access_token: str, account_id: str, amount: float) -> ReconciliationResult:
    """Check real-time balance before approving a supplier payment.

    Args:
        access_token: Plaid access token (sensitive; pass from a secret store).
        account_id: the operating account to debit.
        amount: reorder cost to verify.

    Returns:
        ReconciliationResult. If Plaid is disabled, auto-approves with reason 'disabled' so the reorder
        flow is never blocked in the zero-cost default configuration.
    """
    if not is_enabled():
        return ReconciliationResult(True, None, amount, "disabled (zero-cost default)")

    # Imported lazily so the optional dependency is not required for the core platform.
    from plaid.api import plaid_api  # type: ignore
    from plaid.configuration import Configuration  # type: ignore
    from plaid.api_client import ApiClient  # type: ignore
    from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest  # type: ignore

    cfg = Configuration(
        host=f"https://{os.environ['PLAID_ENV']}.plaid.com",
        api_key={"clientId": os.environ["PLAID_CLIENT_ID"], "secret": os.environ["PLAID_SECRET"]},
    )
    client = plaid_api.PlaidApi(ApiClient(cfg))
    # REAL-TIME balance (never cached) for the payment decision.
    resp = client.accounts_balance_get(AccountsBalanceGetRequest(access_token=access_token))
    account = next((a for a in resp["accounts"] if a["account_id"] == account_id), None)
    if account is None:
        return ReconciliationResult(False, None, amount, "account not found")
    available = account["balances"]["available"] or account["balances"]["current"]
    ok = available >= amount
    return ReconciliationResult(ok, float(available), amount, "sufficient" if ok else "insufficient funds")
