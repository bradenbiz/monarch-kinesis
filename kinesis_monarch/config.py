"""Configuration loaded from environment / .env file."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


class ConfigError(RuntimeError):
    pass


def _require(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        raise ConfigError(
            f"Missing required env var {name!r}. Copy .env.example to .env and fill it in."
        )
    return val


@dataclass(frozen=True)
class Config:
    kinesis_public_key: str
    kinesis_private_key: str
    kinesis_base_url: str
    monarch_email: str
    monarch_password: str
    monarch_mfa_secret: str
    monarch_base_url: str
    base_fiat: str
    kinesis_quote: str
    ohlc_timeframe: str
    symbols: list

    @classmethod
    def load(cls) -> "Config":
        return cls(
            kinesis_public_key=_require("KINESIS_PUBLIC_KEY"),
            kinesis_private_key=_require("KINESIS_PRIVATE_KEY"),
            kinesis_base_url=os.getenv(
                "KINESIS_BASE_URL", "https://client-api.kinesis.money"
            ).strip().rstrip("/"),
            monarch_email=os.getenv("MONARCH_EMAIL", "").strip(),
            monarch_password=os.getenv("MONARCH_PASSWORD", "").strip(),
            monarch_mfa_secret=os.getenv("MONARCH_MFA_SECRET", "").strip(),
            # The monarchmoney library hardcodes the OLD host (api.monarchmoney.com)
            # which now 429s; the API moved to api.monarch.com.
            monarch_base_url=os.getenv("MONARCH_BASE_URL", "https://api.monarch.com")
            .strip()
            .rstrip("/"),
            base_fiat=os.getenv("BASE_FIAT", "USD").strip().upper(),
            # Kinesis quotes metals/crypto against C1USD (its USD-pegged unit),
            # not plain USD. This is the quote leg used to build pricing pairs.
            kinesis_quote=os.getenv("KINESIS_QUOTE", "C1USD").strip().upper(),
            ohlc_timeframe=os.getenv("OHLC_TIMEFRAME", "1440").strip(),
            # Default set of symbols to track (comma-separated, e.g. KAU,KAG,KVT).
            # Empty -> the connector's metals default. CLI --symbols overrides this.
            symbols=[
                s.strip().upper()
                for s in os.getenv("SYMBOLS", "").split(",")
                if s.strip()
            ],
        )

    def require_monarch(self) -> None:
        if not self.monarch_email or not self.monarch_password:
            raise ConfigError(
                "MONARCH_EMAIL and MONARCH_PASSWORD are required for Monarch operations."
            )
