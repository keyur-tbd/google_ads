"""
Google Ads (Performance Max) -> Neon (PostgreSQL) Incremental Sync
==================================================================
Fetches data for Performance Max campaigns:
  - gads_campaigns          : campaign-level metrics
  - gads_asset_groups       : asset group metrics (PMax equiv of ad groups)
  - gads_search_terms       : search terms report
  - gads_product_performance: shopping / product-level performance

Credentials setup:
  Local dev:
    - token.json   (generated once by running generate_token.py)
    - .env         (GOOGLE_ADS_DEVELOPER_TOKEN + config)

  GitHub Actions:
    - GOOGLE_ADS_TOKEN_JSON secret (paste full contents of token.json)
    - All other vars from .env

Requirements:
    pip install google-ads psycopg2-binary google-auth-oauthlib
"""

import os
import json
import logging
from datetime import datetime, timedelta, date
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import psycopg2
from psycopg2.extras import execute_values

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config ------------------------------------------------------------------

NEON_DSN      = os.environ["NEON_DSN"]
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
TOKEN_FILE    = os.getenv("GOOGLE_ADS_TOKEN_FILE", "token.json")

CUSTOMER_IDS: list[str] = [
    cid.strip()
    for cid in os.environ["GOOGLE_ADS_CUSTOMER_IDS"].split(",")
    if cid.strip()
]

# --- Logging -----------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("sync.log"),
    ],
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
    login_customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
    if login_customer_id:
        config["login_customer_id"] = login_customer_id
    return GoogleAdsClient.load_from_dict(config)

# --- Date Range --------------------------------------------------------------

def get_date_range():
    end   = date.today() - timedelta(days=1)
    start = end - timedelta(days=LOOKBACK_DAYS)   # FIX: removed erroneous "- 60"
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

# --- Schema ------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS gads_campaigns (
    date                DATE NOT NULL,
    customer_id         TEXT NOT NULL,
    campaign_id         BIGINT NOT NULL,
    campaign_name       TEXT,
    status              TEXT,
    clicks              BIGINT,
    impressions         BIGINT,
    cost_micros         BIGINT,
    conversions         FLOAT,
    conversion_value    FLOAT,
    synced_at           TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, customer_id, campaign_id)
);

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
    conversions         FLOAT,
    conversion_value    FLOAT,
    synced_at           TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, customer_id, asset_group_id)
);

-- Search terms that triggered PMax ads
CREATE TABLE IF NOT EXISTS gads_search_terms (
    date                DATE NOT NULL,
    customer_id         TEXT NOT NULL,
    campaign_id         BIGINT NOT NULL,
    search_term         TEXT NOT NULL,
    clicks              BIGINT,
    impressions         BIGINT,
    cost_micros         BIGINT,
    conversions         FLOAT,
    conversion_value    FLOAT,
    synced_at           TIMESTAMP DEFAULT NOW(),
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
    conversions             FLOAT,
    conversion_value        FLOAT,
    synced_at               TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, customer_id, campaign_id, product_id)
);
"""

# --- Fetch: Campaigns --------------------------------------------------------

def fetch_campaigns(client, customer_id, start_date, end_date):
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
            segments.date,
            campaign.id,
            campaign.name,
            campaign.status,
            metrics.clicks,
            metrics.impressions,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY segments.date DESC
    """
    rows = []
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
            rows.append((
                row.segments.date,
                customer_id,
                row.campaign.id,
                row.campaign.name,
                row.campaign.status.name,
                row.metrics.clicks,
                row.metrics.impressions,
                row.metrics.cost_micros,
                row.metrics.conversions,
                row.metrics.conversions_value,
            ))
    except GoogleAdsException as ex:
        log.error(f"  [campaigns] API error for {customer_id}: {ex}")
    log.info(f"  Fetched {len(rows)} campaign rows")
    return rows

# --- Fetch: Asset Groups -----------------------------------------------------

def fetch_asset_groups(client, customer_id, start_date, end_date):
    ga_service = client.get_service("GoogleAdsService")
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
        ORDER BY segments.date DESC
    """
    rows = []
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
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
    except GoogleAdsException as ex:
        log.error(f"  [asset_groups] API error for {customer_id}: {ex}")
    log.info(f"  Fetched {len(rows)} asset group rows")
    return rows

# --- Fetch: Search Terms -----------------------------------------------------

def fetch_search_terms(client, customer_id, start_date, end_date):
    ga_service = client.get_service("GoogleAdsService")
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
        ORDER BY segments.date DESC
    """
    rows = []
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
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
    except GoogleAdsException as ex:
        log.error(f"  [search_terms] API error for {customer_id}: {ex}")
    log.info(f"  Fetched {len(rows)} search term rows")
    return rows

# --- Fetch: Product Performance ----------------------------------------------

def fetch_product_performance(client, customer_id, start_date, end_date):
    ga_service = client.get_service("GoogleAdsService")
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
        ORDER BY segments.date DESC
    """
    rows = []
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
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
    except GoogleAdsException as ex:
        log.error(f"  [product_performance] API error for {customer_id}: {ex}")
    log.info(f"  Fetched {len(rows)} product performance rows")
    return rows

# --- Upsert Functions --------------------------------------------------------

def upsert_campaigns(cur, rows):
    if not rows:
        return
    execute_values(cur, """
        INSERT INTO gads_campaigns
            (date, customer_id, campaign_id, campaign_name, status,
             clicks, impressions, cost_micros, conversions, conversion_value, synced_at)
        VALUES %s
        ON CONFLICT (date, customer_id, campaign_id) DO UPDATE SET
            campaign_name    = EXCLUDED.campaign_name,
            status           = EXCLUDED.status,
            clicks           = EXCLUDED.clicks,
            impressions      = EXCLUDED.impressions,
            cost_micros      = EXCLUDED.cost_micros,
            conversions      = EXCLUDED.conversions,
            conversion_value = EXCLUDED.conversion_value,
            synced_at        = NOW()
    """, [r + (datetime.now(),) for r in rows])
    log.info(f"  Upserted {len(rows)} campaign rows")


def upsert_asset_groups(cur, rows):
    if not rows:
        return
    execute_values(cur, """
        INSERT INTO gads_asset_groups
            (date, customer_id, asset_group_id, asset_group_name, campaign_id, status,
             clicks, impressions, cost_micros, conversions, conversion_value, synced_at)
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
    """, [r + (datetime.now(),) for r in rows])
    log.info(f"  Upserted {len(rows)} asset group rows")


def upsert_search_terms(cur, rows):
    if not rows:
        return
    execute_values(cur, """
        INSERT INTO gads_search_terms
            (date, customer_id, campaign_id, search_term,
             clicks, impressions, cost_micros, conversions, conversion_value, synced_at)
        VALUES %s
        ON CONFLICT (date, customer_id, campaign_id, search_term) DO UPDATE SET
            clicks           = EXCLUDED.clicks,
            impressions      = EXCLUDED.impressions,
            cost_micros      = EXCLUDED.cost_micros,
            conversions      = EXCLUDED.conversions,
            conversion_value = EXCLUDED.conversion_value,
            synced_at        = NOW()
    """, [r + (datetime.now(),) for r in rows])
    log.info(f"  Upserted {len(rows)} search term rows")


def upsert_product_performance(cur, rows):
    if not rows:
        return
    # Deduplicate rows with the same primary key (date, customer_id, campaign_id, product_id)
    # by aggregating metrics — keeps last seen values for dimensions, sums metrics
    seen = {}
    for r in rows:
        date_, customer_id, campaign_id, merchant_id, product_id, \
            product_title, product_brand, product_category, product_type, \
            clicks, impressions, cost_micros, conversions, conversion_value = r
        key = (date_, customer_id, campaign_id, product_id)
        if key in seen:
            existing = seen[key]
            seen[key] = (
                date_, customer_id, campaign_id,
                merchant_id or existing[3],
                product_id,
                product_title or existing[5],
                product_brand or existing[6],
                product_category or existing[7],
                product_type or existing[8],
                existing[9]  + clicks,
                existing[10] + impressions,
                existing[11] + cost_micros,
                existing[12] + conversions,
                existing[13] + conversion_value,
            )
        else:
            seen[key] = r

    deduped = list(seen.values())
    log.info(f"  Deduped {len(rows)} -> {len(deduped)} product performance rows")

    execute_values(cur, """
        INSERT INTO gads_product_performance
            (date, customer_id, campaign_id, merchant_id, product_id,
             product_title, product_brand, product_category, product_type,
             clicks, impressions, cost_micros, conversions, conversion_value, synced_at)
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
    """, [r + (datetime.now(),) for r in deduped])
    log.info(f"  Upserted {len(deduped)} product performance rows")

# --- Sync One Account --------------------------------------------------------

def sync_account(client, cur, customer_id, start_date, end_date):
    log.info(f"-- Account: {customer_id} " + "-" * 40)

    log.info("  Fetching campaigns...")
    upsert_campaigns(cur, fetch_campaigns(client, customer_id, start_date, end_date))

    log.info("  Fetching asset groups...")
    upsert_asset_groups(cur, fetch_asset_groups(client, customer_id, start_date, end_date))

    log.info("  Fetching search terms...")
    upsert_search_terms(cur, fetch_search_terms(client, customer_id, start_date, end_date))

    log.info("  Fetching product performance...")
    upsert_product_performance(cur, fetch_product_performance(client, customer_id, start_date, end_date))

# --- Main --------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info(f"Starting Google Ads PMax -> Neon sync ({len(CUSTOMER_IDS)} accounts)")

    start_date, end_date = get_date_range()
    log.info(f"Date range: {start_date} -> {end_date} (lookback={LOOKBACK_DAYS} days)")
    log.info(f"Accounts: {', '.join(CUSTOMER_IDS)}")

    conn = psycopg2.connect(NEON_DSN)
    cur  = conn.cursor()

    cur.execute(SCHEMA_SQL)
    conn.commit()
    log.info("Schema ready")

    client = build_google_ads_client()

    failed = []
    for customer_id in CUSTOMER_IDS:
        try:
            sync_account(client, cur, customer_id, start_date, end_date)
            conn.commit()
            log.info(f"  Account {customer_id} synced OK")
        except Exception as e:
            conn.rollback()
            log.error(f"  Account {customer_id} FAILED: {e}")
            failed.append(customer_id)

    cur.close()
    conn.close()

    if failed:
        log.warning(f"Finished with errors. Failed accounts: {', '.join(failed)}")
        raise SystemExit(1)
    else:
        log.info(f"All {len(CUSTOMER_IDS)} accounts synced successfully")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
