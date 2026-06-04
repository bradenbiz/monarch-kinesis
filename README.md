# Kinesis → Monarch connector

Reconstructs your full [Kinesis Money](https://kinesis.money) holdings value
**over time** (gold KAU, silver KAG, velocity token KVT) and produces CSVs you
import into [Monarch Money](https://monarchmoney.com) — so Monarch charts each
asset's value across its entire history, not just today's balance.

It builds a daily value series from your complete Kinesis transaction history ×
historical prices (USD→C1USD stitched), then you upload it via **Monarch's own
"Import Balance History" feature** — first-party, password-free.

## How it works

```
Kinesis API                          this tool                       Monarch
-----------                          ---------                       -------
/holdings ----------------> current balances ----.
/reporting/                                       \--> daily holdings
  account-balance-statement -> transaction ledger -/    × daily price       you upload
/ohlc/{KAU_C1USD},{KAU_USD} -> historical prices -/    = daily $ value  -->  CSV via
/mid-price/{KAU_C1USD} ----> today's spot price         (per-symbol CSV)     "Import
                                                                             Balance
                                                                             History"
```

The tool **never logs into Monarch** in the default flow — it only reads Kinesis
and writes CSV files. You do the import yourself in Monarch's UI.

## ⚠️ Read before using

- **The default flow is password-free.** The tool needs only your Kinesis API
  key. You import the generated CSVs through Monarch's official UI — no Monarch
  credentials are given to this tool.
- **Monarch has no official write API for balances.** An automated path exists
  (the community `monarchmoney` library, `backfill`/`update` commands) but
  **Monarch actively blocks its login with HTTP 429**, and automated access
  violates Monarch's ToS. It's kept only as an optional/advanced path and is
  expected to fail — prefer the UI import.
- **Kinesis API keys can place trades.** This tool only ever calls **read-only**
  (`GET`) endpoints and never exposes trade/withdraw calls — but keep your
  private key secret. Treat the key as read-only by only using it here.
- Review the [Kinesis API License Agreement](https://kinesis.money/legal-documents/api-license-agreement/)
  (it has clauses about integrating with other software).

## Setup

1. **Generate Kinesis API keys** at
   <https://kms.kinesis.money/settings/api-keys>. You get a **publicKey** and a
   **privateKey** (shown once — copy it). You only need read access.

2. **Install** (Python 3.10+):

   ```bash
   python3 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   ```

3. **Configure** — copy the example env file and fill it in:

   ```bash
   cp .env.example .env
   # edit .env: Kinesis publicKey + privateKey. That's all the default flow needs.
   ```

   For the default (UI-import) flow you do **not** need any Monarch settings.
   Set `SYMBOLS` to the assets you hold (e.g. `SYMBOLS=KAU,KAG,KVT`).

## Usage — primary flow (password-free)

All commands: `.venv/bin/python sync.py <command>`

```bash
# 1. Confirm Kinesis auth works and see your data shapes
.venv/bin/python sync.py inspect

# 2. Build the value-over-time CSVs (writes export/KAU.csv, KAG.csv, KVT.csv).
#    No Monarch login — reads Kinesis, writes files only.
.venv/bin/python sync.py preview --symbols KAU,KAG,KVT

# 3. Import into Monarch yourself (one-time, in the Monarch web UI):
#    a. Create a manual asset account per symbol, named to match the CSV's
#       "Account" column: "Kinesis Gold (KAU)", "Kinesis Silver (KAG)",
#       "Kinesis Velocity Token (KVT)".
#    b. For each account, use Monarch's "Import Balance History" and upload the
#       matching export/<SYMBOL>.csv.
#       (Monarch help: search "Importing Account Balance History Manually".)
```

To refresh later, re-run `preview` and re-import the CSVs (the balance-history
import overwrites the date range rather than duplicating).

| Command | What it does |
|---|---|
| `inspect` | Dump raw Kinesis API responses to `debug/` to verify data shapes. |
| `probe` | Discover the params for the **full** statement history (already wired in). |
| `preview` | Build the value series and write CSVs to `export/`. **No Monarch login.** |
| `backfill` / `update` | Advanced/optional automated push — **blocked by Monarch (429)**, see below. |

### Options

- `--symbols KAU,KAG,KVT` — choose exactly which assets (default: `SYMBOLS` env, else metals).
- `--all` — include every asset you hold (fiat, crypto), not just metals.
- `--out DIR` — output directory for CSVs (default `export/`).

## Advanced (optional): automated push — currently blocked by Monarch

`backfill`/`update` use the unofficial `monarchmoney` library to create accounts
and upload history without the UI. **Monarch now returns HTTP 429 to this login**
(they steer automated access to their OAuth MCP, which can't write balances), and
it violates Monarch's ToS. These commands are retained but expected to fail; if
they do, the tool tells you to use the `preview` + UI-import flow above. Set the
relevant `MONARCH_*` vars in `.env` only if you want to attempt it.

## If something looks off

The Kinesis JSON shapes are only lightly documented, so the parsers are written
to tolerate field-name variations. If `preview` shows wrong/empty numbers:

1. Run `inspect` and look at `debug/*.json`.
2. The candidate field names live at the top of
   [`kinesis_monarch/history.py`](kinesis_monarch/history.py) (`_DATE_FIELDS`,
   `_BALANCE_FIELDS`, etc.) and in
   [`kinesis_monarch/assets.py`](kinesis_monarch/assets.py). Add the actual field
   names you see, and re-run `preview`.

The OHLC `timeFrame` unit (minutes vs seconds) is ambiguous in Kinesis docs;
candles are resampled to one price per calendar day regardless, so the default
works — adjust `OHLC_TIMEFRAME` in `.env` only if `inspect`'s OHLC sample is empty.

## Development

```bash
.venv/bin/pip install pytest
.venv/bin/python -m pytest tests/ -q
```

The pure value-reconstruction logic (statement → daily balances → × prices →
CSV) is fully covered by `tests/test_history.py`.

## Layout

```
kinesis_monarch/
  config.py        env / .env loading
  kinesis.py       signed read-only Kinesis API client
  assets.py        holdings parsing + account naming
  history.py       tolerant parsers + daily value reconstruction (pure, tested)
  connector.py     pipeline: Kinesis -> per-symbol value series
  monarch_sync.py  Monarch login + account create/update/history upload
  cli.py           argparse CLI
sync.py            entrypoint
tests/             unit tests for the reconstruction logic
```
