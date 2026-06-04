"""Signed client for the Kinesis Money public API.

Auth scheme (from the official example, bullioncapital/kinesis-api):

    message   = str(nonce) + METHOD + path + content
    signature = HMAC_SHA256(privateKey, message).hexdigest().upper()
    headers   = x-nonce, x-api-key (publicKey), x-signature

- `path` is the URL path only (NO query string).
- For GET requests `content` is the empty string; query params are sent
  separately and are not part of the signed message.
- For POST requests `content` is the compact JSON body and is signed.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
from typing import Any, Optional

import requests

# Read-only methods are the only ones this tool ever calls. Trade/withdrawal
# endpoints exist on the same API and key, so we deliberately never expose them.


class KinesisError(RuntimeError):
    pass


class KinesisClient:
    def __init__(
        self,
        public_key: str,
        private_key: str,
        base_url: str = "https://client-api.kinesis.money",
        timeout: float = 30.0,
    ) -> None:
        self._public_key = public_key
        self._private_key = private_key.encode("utf-8")
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()

    # ---- low level ----------------------------------------------------

    @staticmethod
    def _nonce() -> str:
        now = dt.datetime.now(dt.timezone.utc)
        return str(round(now.timestamp() * 1000))

    def _sign(self, nonce: str, method: str, path: str, content: str) -> str:
        message = (nonce + method + path + content).encode("utf-8")
        return hmac.new(self._private_key, message, hashlib.sha256).hexdigest().upper()

    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        nonce = self._nonce()
        signature = self._sign(nonce, "GET", path, "")
        headers = {
            "x-nonce": nonce,
            "x-api-key": self._public_key,
            "x-signature": signature,
            "Content-Type": "application/json",
        }
        url = self._base_url + path
        try:
            resp = self._session.get(
                url, headers=headers, params=params, timeout=self._timeout
            )
        except requests.RequestException as exc:  # network errors
            raise KinesisError(f"GET {path} failed: {exc}") from exc

        if resp.status_code != 200:
            raise KinesisError(
                f"GET {path} -> HTTP {resp.status_code}: {resp.text[:500]}"
            )
        try:
            return resp.json()
        except json.JSONDecodeError as exc:
            raise KinesisError(f"GET {path} returned non-JSON: {resp.text[:500]}") from exc

    # ---- read-only endpoints -----------------------------------------

    def get_holdings(self) -> Any:
        """Current balances across all assets (KAU, KAG, fiat, crypto)."""
        return self._get("/v1/exchange/holdings")

    def get_pairs(self) -> Any:
        """All tradable pairs, e.g. KAU_USD, KAG_USD."""
        return self._get("/v1/exchange/pairs")

    def get_mid_price(self, pair: str) -> Any:
        """Current mid price for a pair such as 'KAU_USD'."""
        return self._get(f"/v1/exchange/mid-price/{pair}")

    def get_ohlc(self, pair: str, from_date: str, to_date: str, timeframe: str) -> Any:
        """Historical OHLC candles for a pair.

        from_date / to_date are ISO-8601 strings, e.g. '2022-04-01T00:00:00.000Z'.
        """
        params = {"timeFrame": timeframe, "fromDate": from_date, "toDate": to_date}
        return self._get(f"/v1/exchange/ohlc/{pair}", params=params)

    def get_balance_statement(self, params: Optional[dict] = None) -> Any:
        """Account balance statement across all symbols (transaction ledger).

        Accepts optional query params (e.g. a date range) to fetch full history;
        without them the endpoint returns only a recent window (~30 days).
        """
        return self._get("/v1/exchange/reporting/account-balance-statement", params=params)

    def get_balance_statement_symbol(self, symbol: str) -> Any:
        """Account balance statement for a single symbol, e.g. 'KAU'."""
        return self._get(
            f"/v1/exchange/reporting/account-balance-statement/{symbol}"
        )
