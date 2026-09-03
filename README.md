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

## Disk guard (shared across every pipeline)

This repo writes to a Supabase volume shared with the Business Central sync and
the GRN schedulers. Before it writes, it asks the database whether it is
allowed. **If you get an email titled `[WARN]` or `[STOP] Supabase disk`, start
here.**

```sql
-- this pipeline genuinely needs more room, and the volume has space:
UPDATE etl_disk_policy SET budget_gb = 30 WHERE pipeline = 'marketplace';

-- you resized the Supabase volume (do this EVERY time you resize):
UPDATE etl_disk_policy SET budget_gb = 100 WHERE pipeline = '_disk';

-- someone else should get the emails:
UPDATE etl_alert_config SET recipients = ARRAY['birbal@thebakersdozen.in'];
```

A `[STOP]` means this pipeline is refusing to write until you do one of those.
Nothing is lost: it stops before writing, and the next run continues.

`etl_alerts.py` is **identical in every pipeline repo** - do not add per-repo
logic to it. Everything configurable lives in Postgres (`etl_disk_policy`,
`etl_alert_config`), so budgets, thresholds and recipients change with an
`UPDATE` and no deploy, for all pipelines at once.

Two behaviours worth knowing:

- **It fails OPEN.** If the guard cannot run - no credentials in that step, the
  database unreachable - it logs an error and lets the pipeline continue. A
  guard that breaks a working pipeline is worse than one that cannot check.
  Grep the logs for `Disk guard could not run` if you suspect it is asleep.
- **Budgets grow themselves** into genuinely unallocated volume space, so a
  pipeline that is legitimately growing is not blocked by a number somebody
  guessed months ago. It can never grow past the volume ceiling, so this is
  not a way of turning the guard off.

Full documentation, including how the budgets were sized:
https://github.com/keyur-tbd/bc-supabase-sync#disk-alerts-and-auto-budgeting---start-here-if-you-got-an-email
