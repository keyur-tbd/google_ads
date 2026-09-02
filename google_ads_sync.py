"""
Google Ads -> Supabase (PostgreSQL) Incremental Sync
====================================================
Fetches daily performance for every account in GOOGLE_ADS_CUSTOMER_IDS:
  - gads_campaigns          : campaign-level metrics (all channel types)
  - gads_asset_groups       : Performance Max asset groups
  - gads_search_terms       : search terms (Search / Shopping campaigns)
  - gads_product_performance: shopping / product-level performance

Date range
  Default   : yesterday minus LOOKBACK_DAYS  ->  yesterday
  Backfill  : set START_DATE and END_DATE (YYYY-MM-DD). Long ranges are
              fetched in CHUNK_DAYS windows and committed per window.

Environment
  SUPABASE_DB_URL              postgresql://user:pass@host:5432/postgres
                               Use the Supabase *session pooler* host; the
                               direct db.*.supabase.co host is IPv6-only and
                               unreachable from GitHub runners.
  GOOGLE_ADS_DEVELOPER_TOKEN
  GOOGLE_ADS_TOKEN_JSON        contents of token.json (or GOOGLE_ADS_TOKEN_FILE)
  GOOGLE_ADS_CUSTOMER_IDS      comma separated, digits only
  GOOGLE_ADS_LOGIN_CUSTOMER_ID optional MCC id
  LOOKBACK_DAYS                default 7
  START_DATE / END_DATE        optional backfill range
  CHUNK_DAYS                   default 31

Exit status is 1 if any account or report failed, even though the
successful ones are still committed.
"""

import os
import sys
import json
import logging
from datetime import timedelta, date

import psycopg2
from psycopg2.extras import execute_values
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config ------------------------------------------------------------------

DB_URL        = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CHUNK_DAYS    = int(os.getenv("CHUNK_DAYS", "31"))
TOKEN_FILE    = os.getenv("GOOGLE_ADS_TOKEN_FILE", "token.json")

CUSTOMER_IDS: list[str] = [
    cid.strip().replace("-", "")
    for cid in os.environ.get("GOOGLE_ADS_CUSTOMER_IDS", "").split(",")
    if cid.strip()
]

# --- Logging -----------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("sync.log")],
)
log = logging.getLogger(__name__)

# --- Google Ads Client -------------------------------------------------------

def load_token() -> dict:
    token_json = os.getenv("GOOGLE_ADS_TOKEN_JSON")
    if token_json:
        log.info("Loading token from GOOGLE_ADS_TOKEN_JSON env var")
        return json.loads(token_json)
    if os.path.exists(TOKEN_FILE):
        log.info(f"Loading token from {TOKEN_FILE}")
        with open(TOKEN_FILE) as f:
            return json.load(f)
    raise FileNotFoundError(
        "No OAuth token found. Set GOOGLE_ADS_TOKEN_JSON env var "
        f"or run generate_token.py to create {TOKEN_FILE}"
    )


def build_google_ads_client() -> GoogleAdsClient:
    token = load_token()
    config = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id":       token["client_id"],
        "client_secret":   token["client_secret"],
        "refresh_token":   token["refresh_token"],
        "use_proto_plus":  True,
    }
    login_customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "").strip()
    if login_customer_id:
        config["login_customer_id"] = login_customer_id
    return GoogleAdsClient.load_from_dict(config)

# --- Date Range --------------------------------------------------------------

def get_date_range() -> tuple[date, date]:
    start_env = os.getenv("START_DATE", "").strip()
    end_env   = os.getenv("END_DATE", "").strip()
    if start_env or end_env:
        if not start_env:
            raise SystemExit("END_DATE given without START_DATE")
        start = date.fromisoformat(start_env)
        end   = date.fromisoformat(end_env) if end_env else date.today() - timedelta(days=1)
        if start > end:
            raise SystemExit(f"START_DATE {start} is after END_DATE {end}")
        return start, end
    end   = date.today() - timedelta(days=1)
    start = end - timedelta(days=LOOKBACK_DAYS)
    return start, end


def date_chunks(start: date, end: date, size: int):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=size - 1), end)
        yield cur.isoformat(), nxt.isoformat()
        cur = nxt + timedelta(days=1)

# --- Schema ------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS gads_campaigns (
    date                DATE NOT NULL,
    customer_id         TEXT NOT NULL,
    campaign_id         BIGINT NOT NULL,
    campaign_name       TEXT,
    status              TEXT,
    channel_type        TEXT,
    channel_sub_type    TEXT,
    clicks              BIGINT,
    impressions         BIGINT,
    cost_micros         BIGINT,
    cost                NUMERIC(18,6) GENERATED ALWAYS AS (cost_micros / 1000000.0) STORED,
    conversions         DOUBLE PRECISION,
    conversion_value    DOUBLE PRECISION,
    synced_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, customer_id, campaign_id)
);
ALTER TABLE gads_campaigns ADD COLUMN IF NOT EXISTS channel_type     TEXT;
ALTER TABLE gads_campaigns ADD COLUMN IF NOT EXISTS channel_sub_type TEXT;

-- PMax asset groups (equivalent of ad groups)
CREATE TABLE IF NOT EXISTS gads_asset_groups (
    date                DATE NOT NULL,
    customer_id         TEXT NOT NULL,
    asset_group_id      BIGINT NOT NULL,
    asset_group_name    TEXT,
    campaign_id         BIGINT,
    status              TEXT,
    clicks              BIGINT,
    impressions         BIGINT,
    cost_micros         BIGINT,
    cost                NUMERIC(18,6) GENERATED ALWAYS AS (cost_micros / 1000000.0) STORED,
    conversions         DOUBLE PRECISION,
    conversion_value    DOUBLE PRECISION,
    synced_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, customer_id, asset_group_id)
);

-- Search terms that triggered ads
CREATE TABLE IF NOT EXISTS gads_search_terms (
    date                DATE NOT NULL,
    customer_id         TEXT NOT NULL,
    campaign_id         BIGINT NOT NULL,
    search_term         TEXT NOT NULL,
    clicks              BIGINT,
    impressions         BIGINT,
    cost_micros         BIGINT,
    cost                NUMERIC(18,6) GENERATED ALWAYS AS (cost_micros / 1000000.0) STORED,
    conversions         DOUBLE PRECISION,
    conversion_value    DOUBLE PRECISION,
    synced_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, customer_id, campaign_id, search_term)
);

-- Product / shopping performance
CREATE TABLE IF NOT EXISTS gads_product_performance (
    date                    DATE NOT NULL,
    customer_id             TEXT NOT NULL,
    campaign_id             BIGINT NOT NULL,
    merchant_id             BIGINT,
    product_id              TEXT NOT NULL,
    product_title           TEXT,
    product_brand           TEXT,
    product_category        TEXT,
    product_type            TEXT,
    clicks                  BIGINT,
    impressions             BIGINT,
    cost_micros             BIGINT,
    cost                    NUMERIC(18,6) GENERATED ALWAYS AS (cost_micros / 1000000.0) STORED,
    conversions             DOUBLE PRECISION,
    conversion_value        DOUBLE PRECISION,
    synced_at               TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, customer_id, campaign_id, product_id)
);
"""

# --- Fetch helpers -----------------------------------------------------------

class ReportError(Exception):
    """A Google Ads report query failed."""


def _search(client, customer_id, query, report):
    ga_service = client.get_service("GoogleAdsService")
    try:
        yield from ga_service.search(customer_id=customer_id, query=query)
    except GoogleAdsException as ex:
        details = "; ".join(e.message for e in ex.failure.errors) or str(ex)
        raise ReportError(f"[{report}] {details}") from ex


def fetch_campaigns(client, customer_id, start_date, end_date):
    query = f"""
        SELECT
            segments.date,
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            campaign.advertising_channel_sub_type,
            metrics.clicks,
            metrics.impressions,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    rows = []
    for row in _search(client, customer_id, query, "campaigns"):
        rows.append((
            row.segments.date,
            customer_id,
            row.campaign.id,
            row.campaign.name,
            row.campaign.status.name,
            row.campaign.advertising_channel_type.name,
            row.campaign.advertising_channel_sub_type.name,
            row.metrics.clicks,
            row.metrics.impressions,
            row.metrics.cost_micros,
            row.metrics.conversions,
            row.metrics.conversions_value,
        ))
    log.info(f"  Fetched {len(rows)} campaign rows")
    return rows


def fetch_asset_groups(client, customer_id, start_date, end_date):
    query = f"""
        SELECT
            segments.date,
            asset_group.id,
            asset_group.name,
            asset_group.campaign,
            asset_group.status,
            metrics.clicks,
            metrics.impressions,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM asset_group
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    rows = []
    for row in _search(client, customer_id, query, "asset_groups"):
        campaign_id = int(row.asset_group.campaign.split("/")[-1])
        rows.append((
            row.segments.date,
            customer_id,
            row.asset_group.id,
            row.asset_group.name,
            campaign_id,
            row.asset_group.status.name,
            row.metrics.clicks,
            row.metrics.impressions,
            row.metrics.cost_micros,
            row.metrics.conversions,
            row.metrics.conversions_value,
        ))
    log.info(f"  Fetched {len(rows)} asset group rows")
    return rows


def fetch_search_terms(client, customer_id, start_date, end_date):
    query = f"""
        SELECT
            segments.date,
            campaign.id,
            search_term_view.search_term,
            metrics.clicks,
            metrics.impressions,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM search_term_view
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    rows = []
    for row in _search(client, customer_id, query, "search_terms"):
        rows.append((
            row.segments.date,
            customer_id,
            row.campaign.id,
            row.search_term_view.search_term,
            row.metrics.clicks,
            row.metrics.impressions,
            row.metrics.cost_micros,
            row.metrics.conversions,
            row.metrics.conversions_value,
        ))
    log.info(f"  Fetched {len(rows)} search term rows")
    return rows


def fetch_product_performance(client, customer_id, start_date, end_date):
    query = f"""
        SELECT
            segments.date,
            campaign.id,
            segments.product_merchant_id,
            segments.product_item_id,
            segments.product_title,
            segments.product_brand,
            segments.product_category_level1,
            segments.product_type_l1,
            metrics.clicks,
            metrics.impressions,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM shopping_performance_view
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    rows = []
    for row in _search(client, customer_id, query, "product_performance"):
        rows.append((
            row.segments.date,
            customer_id,
            row.campaign.id,
            row.segments.product_merchant_id or None,
            row.segments.product_item_id,
            row.segments.product_title or None,
            row.segments.product_brand or None,
            row.segments.product_category_level1 or None,
            row.segments.product_type_l1 or None,
            row.metrics.clicks,
            row.metrics.impressions,
            row.metrics.cost_micros,
            row.metrics.conversions,
            row.metrics.conversions_value,
        ))
    log.info(f"  Fetched {len(rows)} product performance rows")
    return rows

# --- Upsert Functions --------------------------------------------------------

def upsert_campaigns(cur, rows):
    if not rows:
        return
    execute_values(cur, """
        INSERT INTO gads_campaigns
            (date, customer_id, campaign_id, campaign_name, status,
             channel_type, channel_sub_type,
             clicks, impressions, cost_micros, conversions, conversion_value)
        VALUES %s
        ON CONFLICT (date, customer_id, campaign_id) DO UPDATE SET
            campaign_name    = EXCLUDED.campaign_name,
            status           = EXCLUDED.status,
            channel_type     = EXCLUDED.channel_type,
            channel_sub_type = EXCLUDED.channel_sub_type,
            clicks           = EXCLUDED.clicks,
            impressions      = EXCLUDED.impressions,
            cost_micros      = EXCLUDED.cost_micros,
            conversions      = EXCLUDED.conversions,
            conversion_value = EXCLUDED.conversion_value,
            synced_at        = NOW()
    """, rows, page_size=1000)
    log.info(f"  Upserted {len(rows)} campaign rows")


def upsert_asset_groups(cur, rows):
    if not rows:
        return
    execute_values(cur, """
        INSERT INTO gads_asset_groups
            (date, customer_id, asset_group_id, asset_group_name, campaign_id, status,
             clicks, impressions, cost_micros, conversions, conversion_value)
        VALUES %s
        ON CONFLICT (date, customer_id, asset_group_id) DO UPDATE SET
            asset_group_name = EXCLUDED.asset_group_name,
            campaign_id      = EXCLUDED.campaign_id,
            status           = EXCLUDED.status,
            clicks           = EXCLUDED.clicks,
            impressions      = EXCLUDED.impressions,
            cost_micros      = EXCLUDED.cost_micros,
            conversions      = EXCLUDED.conversions,
            conversion_value = EXCLUDED.conversion_value,
            synced_at        = NOW()
    """, rows, page_size=1000)
    log.info(f"  Upserted {len(rows)} asset group rows")


def upsert_search_terms(cur, rows):
    if not rows:
        return
    execute_values(cur, """
        INSERT INTO gads_search_terms
            (date, customer_id, campaign_id, search_term,
             clicks, impressions, cost_micros, conversions, conversion_value)
        VALUES %s
        ON CONFLICT (date, customer_id, campaign_id, search_term) DO UPDATE SET
            clicks           = EXCLUDED.clicks,
            impressions      = EXCLUDED.impressions,
            cost_micros      = EXCLUDED.cost_micros,
            conversions      = EXCLUDED.conversions,
            conversion_value = EXCLUDED.conversion_value,
            synced_at        = NOW()
    """, rows, page_size=1000)
    log.info(f"  Upserted {len(rows)} search term rows")


def upsert_product_performance(cur, rows):
    if not rows:
        return
    # The query segments by title/brand/category/type, which can split one
    # (date, campaign, product) key into several rows. Sum metrics per key
    # and keep the last non-empty value for each dimension.
    seen = {}
    for r in rows:
        key = (r[0], r[1], r[2], r[4])
        if key in seen:
            e = seen[key]
            seen[key] = (
                r[0], r[1], r[2],
                r[3] or e[3], r[4],
                r[5] or e[5], r[6] or e[6], r[7] or e[7], r[8] or e[8],
                e[9] + r[9], e[10] + r[10], e[11] + r[11], e[12] + r[12], e[13] + r[13],
            )
        else:
            seen[key] = r
    deduped = list(seen.values())
    if len(deduped) != len(rows):
        log.info(f"  Deduped {len(rows)} -> {len(deduped)} product performance rows")

    execute_values(cur, """
        INSERT INTO gads_product_performance
            (date, customer_id, campaign_id, merchant_id, product_id,
             product_title, product_brand, product_category, product_type,
             clicks, impressions, cost_micros, conversions, conversion_value)
        VALUES %s
        ON CONFLICT (date, customer_id, campaign_id, product_id) DO UPDATE SET
            merchant_id      = EXCLUDED.merchant_id,
            product_title    = EXCLUDED.product_title,
            product_brand    = EXCLUDED.product_brand,
            product_category = EXCLUDED.product_category,
            product_type     = EXCLUDED.product_type,
            clicks           = EXCLUDED.clicks,
            impressions      = EXCLUDED.impressions,
            cost_micros      = EXCLUDED.cost_micros,
            conversions      = EXCLUDED.conversions,
            conversion_value = EXCLUDED.conversion_value,
            synced_at        = NOW()
    """, deduped, page_size=1000)
    log.info(f"  Upserted {len(deduped)} product performance rows")

# --- Sync One Account --------------------------------------------------------

REPORTS = [
    ("campaigns",           fetch_campaigns,           upsert_campaigns),
    ("asset_groups",        fetch_asset_groups,        upsert_asset_groups),
    ("search_terms",        fetch_search_terms,        upsert_search_terms),
    ("product_performance", fetch_product_performance, upsert_product_performance),
]


def sync_account(client, conn, customer_id, start_date, end_date) -> list[str]:
    """Sync every report for one account/window. Each report is committed on
    its own so one failing report does not discard the others. Returns the
    list of error messages."""
    errors = []
    log.info(f"-- Account: {customer_id}  {start_date} -> {end_date}")
    for name, fetch, upsert in REPORTS:
        log.info(f"  Fetching {name}...")
        try:
            rows = fetch(client, customer_id, start_date, end_date)
            with conn.cursor() as cur:
                upsert(cur, rows)
            conn.commit()
        except Exception as e:  # ReportError, psycopg2 errors, etc.
            conn.rollback()
            msg = f"{customer_id} {name} {start_date}..{end_date}: {e}"
            log.error(f"  FAILED {msg}")
            errors.append(msg)
    return errors

# --- Main --------------------------------------------------------------------

def connect_db():
    if not DB_URL:
        raise SystemExit("SUPABASE_DB_URL is not set")
    dsn = DB_URL
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '300s'")
    conn.commit()
    return conn


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def main():
    if not CUSTOMER_IDS:
        raise SystemExit("GOOGLE_ADS_CUSTOMER_IDS is not set")

    start, end = get_date_range()
    log.info("=" * 60)
    log.info(f"Google Ads -> Supabase sync | {len(CUSTOMER_IDS)} accounts | {start} -> {end}")

    conn = connect_db()
    ensure_schema(conn)
    log.info("Schema ready")

    client = build_google_ads_client()

    errors = []
    for chunk_start, chunk_end in date_chunks(start, end, CHUNK_DAYS):
        for customer_id in CUSTOMER_IDS:
            errors += sync_account(client, conn, customer_id, chunk_start, chunk_end)

    conn.close()

    if errors:
        log.error(f"Finished with {len(errors)} error(s):")
        for e in errors:
            log.error(f"  - {e}")
        sys.exit(1)
    log.info(f"All {len(CUSTOMER_IDS)} accounts synced successfully")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
