# Google Ads → Supabase sync

Daily GitHub Actions job that pulls Google Ads performance data and upserts it into Supabase (Postgres).

## Tables (schema `public`)

| Table | Grain | Key |
|---|---|---|
| `gads_campaigns` | campaign × day (all channel types, with `channel_type` / `channel_sub_type`) | date, customer_id, campaign_id |
| `gads_asset_groups` | Performance Max asset group × day | date, customer_id, asset_group_id |
| `gads_search_terms` | search term × campaign × day (Search / Shopping campaigns) | date, customer_id, campaign_id, search_term |
| `gads_product_performance` | product × campaign × day (Shopping / PMax) | date, customer_id, campaign_id, product_id |

Every table has `cost_micros` plus a generated `cost` column in account currency, and `synced_at`.

## Secrets

| Secret | Value |
|---|---|
| `SUPABASE_DB_URL` | `postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres` — use the **session pooler** host (IPv4); the direct `db.<ref>.supabase.co` host is IPv6-only and unreachable from GitHub runners |
| `GOOGLE_ADS_DEVELOPER_TOKEN` | developer token |
| `GOOGLE_ADS_TOKEN_JSON` | full contents of `token.json` |
| `GOOGLE_ADS_CUSTOMER_IDS` | comma separated customer ids (digits only) |
| `GOOGLE_ADS_LOGIN_CUSTOMER_ID` | optional MCC id |
| `NEON_DSN` | only needed by the one-off Neon backfill |

## Runs

* **Scheduled**: every day at 11:20 UTC, re-syncs the last `LOOKBACK_DAYS` (7) days up to yesterday so late conversions are picked up.
* **Backfill from the API**: Actions → *Google Ads → Supabase Daily Sync* → *Run workflow*, fill `start_date` / `end_date`. Long ranges are fetched in 31-day chunks and committed per chunk, so a failure part-way keeps what was already written.
* **Backfill from Neon**: Actions → *Backfill Google Ads tables from Neon → Supabase*. Copies the historic rows once; rows already in Supabase are kept unless `overwrite` is `true`.

The job exits non-zero if any account/report failed, but the reports that succeeded are still committed.

## Local run

```
pip install -r requirements.txt
# .env with SUPABASE_DB_URL, GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CUSTOMER_IDS (+ token.json)
python google_ads_sync.py
START_DATE=2025-01-01 END_DATE=2025-03-31 python google_ads_sync.py
```
