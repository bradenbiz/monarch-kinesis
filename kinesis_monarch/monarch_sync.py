"""Monarch Money operations via the unofficial `monarchmoney` library.

NOTE: Monarch has no official API; this uses the community library that drives
the same GraphQL endpoint as the apps. It can break when Monarch changes things,
and automated access is against Monarch's ToS — use at your own risk for
personal data only.
"""

from __future__ import annotations

import os
from typing import Optional

from .config import Config

SESSION_DIR = ".monarch_session"
SESSION_FILE = os.path.join(SESSION_DIR, "session.pickle")

# Monarch manual-account classification for a precious-metal / asset holding.
ACCOUNT_TYPE = "other_asset"
ACCOUNT_SUB_TYPE = "other"


def _client(base_url: Optional[str] = None):
    """Construct a MonarchMoney client (lazy import so the lib is optional).

    The library hardcodes the deprecated api.monarchmoney.com host (which now
    returns 429); override BASE_URL to the current host. All of the library's
    endpoints (login, graphql, balance-history upload) derive from BASE_URL.
    """
    from monarchmoney import MonarchMoney  # type: ignore

    if base_url:
        MonarchMoney.BASE_URL = base_url
    os.makedirs(SESSION_DIR, exist_ok=True)
    return MonarchMoney(session_file=SESSION_FILE)


async def login(config: Config):
    """Log in, reusing a saved session when possible."""
    from monarchmoney import RequireMFAException  # type: ignore

    config.require_monarch()
    mm = _client(config.monarch_base_url)

    # Try a previously saved session first to avoid re-authenticating.
    if os.path.exists(SESSION_FILE):
        try:
            mm.load_session(SESSION_FILE)
            # Cheap call to confirm the session is still valid.
            await mm.get_accounts()
            return mm
        except Exception:
            pass  # fall through to a fresh login

    try:
        await mm.login(
            email=config.monarch_email,
            password=config.monarch_password,
            save_session=True,
            mfa_secret_key=config.monarch_mfa_secret or None,
        )
    except RequireMFAException:
        code = input("Enter your Monarch MFA code: ").strip()
        await mm.multi_factor_authenticate(
            config.monarch_email, config.monarch_password, code
        )
        mm.save_session(SESSION_FILE)

    return mm


def _account_display_name(account: dict) -> str:
    return (account.get("displayName") or account.get("name") or "").strip()


async def find_account_id(mm, name: str) -> Optional[str]:
    data = await mm.get_accounts()
    accounts = data.get("accounts", []) if isinstance(data, dict) else []
    for acct in accounts:
        if _account_display_name(acct).lower() == name.lower():
            return str(acct.get("id"))
    return None


async def ensure_account(mm, name: str, initial_balance: float = 0.0) -> str:
    """Return the id of the manual account named `name`, creating it if needed."""
    existing = await find_account_id(mm, name)
    if existing:
        return existing

    await mm.create_manual_account(
        account_type=ACCOUNT_TYPE,
        account_sub_type=ACCOUNT_SUB_TYPE,
        is_in_net_worth=True,
        account_name=name,
        account_balance=initial_balance,
    )
    account_id = await find_account_id(mm, name)
    if not account_id:
        raise RuntimeError(
            f"Created account {name!r} but could not find its id afterward."
        )
    return account_id


async def upload_history(mm, account_id: str, csv_content: str) -> None:
    """Backfill dated balance history (Date,Balance,Account CSV)."""
    await mm.upload_account_balance_history(account_id, csv_content)


async def set_balance(mm, account_id: str, balance: float) -> None:
    """Set the account's current balance."""
    await mm.update_account(account_id=account_id, account_balance=balance)
