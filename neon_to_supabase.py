"""
One-off historic backfill: copy tables from Neon into Supabase.

Environment
  NEON_DSN          source (Neon) connection string
  SUPABASE_DB_URL   target (Supabase) connection string
  TABLES            comma separated table names to copy
  SYNC_MODULE       python module exposing ensure_schema(conn) and optionally
                    ensure_columns(conn, table, column_names); it is used to
                    create the typed target tables before copying
  OVERWRITE         "1" to let Neon rows overwrite existing Supabase rows.
                    Default keeps whatever Supabase already has (newer sync
                    rows win over the historic copy).
  BATCH_SIZE        rows per insert batch, default 5000

Rows are streamed from Neon ordered by the target's unique key, coerced to
the target column types (Neon's Meta tables are mostly TEXT) and inserted
with ON CONFLICT on that key. Columns that do not exist in the target are
reported and skipped.
"""

import os
import sys
import math
import importlib
import logging
from datetime import date, datetime

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

NEON_DSN   = os.environ["NEON_DSN"]
TARGET_DSN = os.environ["SUPABASE_DB_URL"]
TABLES     = [t.strip() for t in os.environ["TABLES"].split(",") if t.strip()]
OVERWRITE  = os.getenv("OVERWRITE", "0") == "1"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
SYNC_MOD   = os.getenv("SYNC_MODULE", "")


def with_ssl(dsn: str) -> str:
    if "sslmode=" in dsn:
        return dsn
    return dsn + ("&" if "?" in dsn else "?") + "sslmode=require"


def table_exists(cur, table) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table}",))
    return cur.fetchone()[0]


def columns(cur, table) -> dict:
    """name -> (data_type, is_generated) in ordinal order."""
    cur.execute("""
        SELECT column_name, data_type, is_generated
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    return {n: (t, g == "ALWAYS") for n, t, g in cur.fetchall()}


def unique_key(cur, table) -> list[str]:
    """Columns of the primary key, else the first unique index."""
    cur.execute("""
        SELECT i.indisprimary, array_agg(a.attname ORDER BY k.ord)
        FROM pg_index i
        JOIN unnest(i.indkey) WITH ORDINALITY k(attnum, ord) ON TRUE
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
        WHERE i.indrelid = %s::regclass AND (i.indisprimary OR i.indisunique)
        GROUP BY i.indexrelid, i.indisprimary
        ORDER BY i.indisprimary DESC
        LIMIT 1
    """, (f"public.{table}",))
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"{table}: target has no primary key or unique index")
    return list(row[1])


def coercer(data_type: str):
    t = data_type.lower()
    if t in ("double precision", "real", "numeric"):
        def f(v):
            if v is None or v == "":
                return None
            try:
                x = float(v)
            except (TypeError, ValueError):
                return None
            return None if math.isnan(x) else x
        return f
    if t in ("bigint", "integer", "smallint"):
        def f(v):
            if v is None or v == "":
                return None
            try:
                return int(float(v))
            except (TypeError, ValueError):
                return None
        return f
    if t == "date":
        def f(v):
            if v is None or v == "":
                return None
            if isinstance(v, (date, datetime)):
                return v if isinstance(v, date) else v.date()
            return date.fromisoformat(str(v)[:10])
        return f
    if t.startswith("timestamp"):
        def f(v):
            if v is None or v == "":
                return None
            if isinstance(v, datetime):
                return v
            return datetime.fromisoformat(str(v))
        return f
    if t == "boolean":
        return lambda v: None if v is None or v == "" else str(v).lower() in ("1", "true", "t", "yes")
    return lambda v: None if v is None else str(v)


def copy_table(src, dst, table, module):
    with src.cursor() as scur:
        if not table_exists(scur, table):
            log.warning(f"{table}: not present in Neon, skipping")
            return 0
        src_cols = list(columns(scur, table).keys())
        scur.execute(f'SELECT COUNT(*) FROM "{table}"')
        total = scur.fetchone()[0]
    log.info(f"{table}: {total} rows in Neon, {len(src_cols)} columns")

    with dst.cursor() as dcur:
        if module and hasattr(module, "ensure_columns"):
            module.ensure_columns(dst, table, src_cols)
        tgt = columns(dcur, table)
        key = unique_key(dcur, table)
    dst.commit()

    copy_cols = [c for c in src_cols if c in tgt and not tgt[c][1]]
    skipped   = [c for c in src_cols if c not in tgt]
    if skipped:
        log.warning(f"{table}: columns missing in Supabase, skipped: {skipped}")
    missing_key = [k for k in key if k not in src_cols]
    if missing_key:
        raise SystemExit(f"{table}: Neon lacks key columns {missing_key}")

    coerce  = [coercer(tgt[c][0]) for c in copy_cols]
    collist = ", ".join(f'"{c}"' for c in copy_cols)
    keylist = ", ".join(f'"{c}"' for c in key)
    if OVERWRITE:
        sets = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in copy_cols if c not in key)
        conflict = f"ON CONFLICT ({keylist}) DO UPDATE SET {sets}" if sets else f"ON CONFLICT ({keylist}) DO NOTHING"
    else:
        conflict = f"ON CONFLICT ({keylist}) DO NOTHING"
    sql = f'INSERT INTO "{table}" ({collist}) VALUES %s {conflict}'

    order = ", ".join(f'"{c}"' for c in key)
    done = inserted = 0
    with src.cursor(name=f"copy_{table}") as scur:
        scur.itersize = BATCH_SIZE
        scur.execute(f'SELECT {collist} FROM "{table}" ORDER BY {order}')
        while True:
            batch = scur.fetchmany(BATCH_SIZE)
            if not batch:
                break
            rows = [tuple(f(v) for f, v in zip(coerce, r)) for r in batch]
            with dst.cursor() as dcur:
                execute_values(dcur, sql, rows, page_size=1000)
                inserted += dcur.rowcount if dcur.rowcount >= 0 else 0
            dst.commit()
            done += len(batch)
            log.info(f"{table}: {done}/{total} read, {inserted} written")
    return inserted


def main():
    module = importlib.import_module(SYNC_MOD) if SYNC_MOD else None
    src = psycopg2.connect(with_ssl(NEON_DSN))
    dst = psycopg2.connect(with_ssl(TARGET_DSN))
    if module:
        module.ensure_schema(dst)
    failed = []
    for table in TABLES:
        try:
            copy_table(src, dst, table, module)
        except Exception as e:
            dst.rollback()
            log.error(f"{table}: FAILED: {e}")
            failed.append(table)
    src.close()
    dst.close()
    if failed:
        log.error(f"Failed tables: {failed}")
        sys.exit(1)
    log.info("Backfill complete")


if __name__ == "__main__":
    main()
