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
- **Use a read-only, IP-restricted Kinesis key.** This tool only ever calls
  **read-only** (`GET`) endpoints and never exposes trade/withdraw calls. The
  key in use is created read-only and restricted to the machine's IP, so it
  can't trade even if leaked. It lives in 1Password, not in a plaintext file.
- Review the [Kinesis API License Agreement](https://kinesis.money/legal-documents/api-license-agreement/)
  (it has clauses about integrating with other software).

## Setup

1. **Generate Kinesis API keys** at
   <https://kms.kinesis.money/settings/api-keys>. You get a **publicKey** and a
   **privateKey** (shown once — copy it). Make the key **read-only** and
   **IP-restricted** to the machine that runs this tool, then store both values
   in your 1Password vault.

2. **Install** (Python 3.10+):

   ```bash
   python3 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   ```

3. **Configure** — the Kinesis keys come from 1Password at run time via
   [`op run`](https://developer.1password.com/docs/cli/reference/commands/run/),
   never from a plaintext file. Keep an env file of `op://` references
   **outside this repo** (it names your vault and item), for example
   `~/secrets/monarch-kinesis.env.1password`:

   ```bash
   KINESIS_PUBLIC_KEY=op://<vault>/<item>/<public-key-field>
   KINESIS_PRIVATE_KEY=op://<vault>/<item>/<private-key-field>
   ```

   Non-secret settings can still go in `.env` (`cp .env.example .env`); leave
   the `KINESIS_*_KEY` lines out of it. Variables injected by `op run` take
   precedence over `.env`.

   For the default (UI-import) flow you do **not** need any Monarch settings.
   Set `SYMBOLS` to the assets you hold (e.g. `SYMBOLS=KAU,KAG,KVT`).

## Usage — primary flow (password-free)

All commands run under `op run` so the Kinesis keys are injected from
1Password (use your own env file path):

```bash
op run --env-file ~/secrets/monarch-kinesis.env.1password -- .venv/bin/python sync.py <command>
```

```bash
ENV=~/secrets/monarch-kinesis.env.1password

# 1. Confirm Kinesis auth works and see your data shapes
op run --env-file "$ENV" -- .venv/bin/python sync.py inspect

# 2. Build the value-over-time CSVs (writes export/KAU.csv, KAG.csv, KVT.csv).
#    No Monarch login — reads Kinesis, writes files only.
op run --env-file "$ENV" -- .venv/bin/python sync.py preview --symbols KAU,KAG,KVT

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
| `transactions` | Build per-symbol **transaction** CSVs (USD) to `export/`. **No Monarch login.** |
| `backfill` / `update` | Advanced/optional automated push — **blocked by Monarch (429)**, see below. |

### Transactions (individual buys, distributions, deposits)

`preview` charts each asset's **value over time** (balance history). If you also
want the **individual transactions** — trades, distributions, deposits,
withdrawals — in Monarch's transaction feed:

```bash
# Writes export/KAU_transactions.csv, KAG_transactions.csv, KVT_transactions.csv
op run --env-file "$ENV" -- .venv/bin/python sync.py transactions --symbols KAU,KAG,KVT
```

Then in Monarch, open each manual account and use **"Upload transactions"** with
the matching CSV. Columns are `Date,Merchant,Category,Account,Notes,Amount`.

- **Amounts are USD value** (positive = the holding increased, negative =
  decreased). Trades use their actual execution price; KVT deposits/withdrawals
  use the USD figure Kinesis reports; distributions are valued at that day's
  price. Each row's `Notes` carries the token quantity, implied unit price, fee,
  and Kinesis transaction id.
- **Card spends are labeled.** When you pay with the Kinesis card, Kinesis books
  it as a metal sell (in the KAU/KAG account) + a `Withdrawal(Card Payment)`
  debit (in the C1USD account). The command links the two by timestamp and
  relabels the metal sell `Kinesis Card Payment`, with the exact charge and the
  card's `Transaction_ID` in the notes. (The **merchant name** — e.g. "AMAZON
  MKTPLACE PMTS" — is **not** exposed by Kinesis's balance-statement API, so it
  can't be included; use the card `Transaction_ID` to look it up in the Kinesis
  app.) Card purchases paid straight from a C1USD balance (no metal conversion)
  only appear in a C1USD export.
- **Heads-up:** for a *manual* asset account Monarch does **not** reconcile
  imported transactions against imported balance history. This feed is
  **informational/additive** — it won't drive the balance-history chart, so keep
  using `preview` + "Import Balance History" for the chart itself.

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
  config.py        env loading (process env, e.g. from op run, wins over .env)
  kinesis.py       signed read-only Kinesis API client
  assets.py        holdings parsing + account naming
  history.py       tolerant parsers + daily value reconstruction (pure, tested)
  connector.py     pipeline: Kinesis -> per-symbol value series
  monarch_sync.py  Monarch login + account create/update/history upload
  cli.py           argparse CLI
sync.py            entrypoint
tests/             unit tests for the reconstruction logic
```
