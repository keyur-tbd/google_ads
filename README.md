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

## Birbal reads these tables (shared across every pipeline)

Since 2026-09-03 the Supabase project this writes to also backs **Birbal**
(`birbal-tbdai/birbal-mission-control`), the app the business asks questions in
plain language. Birbal never reads `public` directly: it reads one `select *`
view per table in a separate `warehouse` schema, plus a dictionary row per table
that tells it what the columns mean. Two consequences for this repo.

**A new table, or a new column, is invisible to Birbal until somebody exposes
it.** A view freezes its column list at CREATE time, and the exposure list is an
array inside a function - so nothing errors anywhere. The table simply does not
exist as far as the business is concerned, and an answer quietly leaves the new
column out. After applying the DDL this repo prints, run as `postgres`:

```sql
select app.sync_warehouse_views();   -- mirror new tables and columns
select app.sync_role_grants();       -- re-grant: the mirror drops grants
```

and add or update that table's row in `warehouse.warehouse_meta`. A column
nobody described there is a column Birbal will not use correctly.

**Never DROP or rename a table this pipeline owns.** A `warehouse` view depends
on it, so a plain `DROP` fails and `DROP ... CASCADE` deletes Birbal's view
without a word - that is how the BC sync went red on 2026-09-04. Add columns;
never replace tables. The writes themselves are safe: rows upsert on
their natural key, so a reader never sees a half-loaded table.

**This sync applies its own DDL** - `create table if not exists` and
`add column if not exists` run on every run - so a new Google Ads field lands in
`public` with nobody in the loop, and stays invisible to Birbal until the mirror
above is rebuilt.

Exposed today: `gads_product_performance` only.
**`gads_campaigns`, `gads_asset_groups` and `gads_search_terms` are NOT exposed** -
Birbal cannot answer on them at all until the two calls above are run for those
three tables and a `warehouse_meta` row is written for each.

Full contract, and the checks to run after a schema change:
https://github.com/keyur-tbd/bc-supabase-sync#who-reads-this-database-birbal
