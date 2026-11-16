"""Run a loader's idempotent setup DDL without taking table locks it does not need.

`ALTER TABLE .. ADD COLUMN IF NOT EXISTS`, `ALTER TABLE .. ENABLE ROW LEVEL SECURITY` and `CREATE INDEX IF NOT
EXISTS` all lock the table (ACCESS EXCLUSIVE / SHARE) BEFORE finding there is nothing to do. Every run then queues
behind whatever is reading the table (a Birbal board, a matview refresh) until statement_timeout cancels the load,
and while it queues it blocks every later reader too. That was targets' intermittent "canceling statement due to
statement timeout" at write_detail (Nov 2026), and google_ads failing four nights running in ensure_schema.

ensure_ddl(cur, sql) runs the statements one by one and skips those already in effect: a column that exists, RLS
already on, an index that exists, a table comment already equal. What still has to run gets a short lock_timeout
inside a savepoint and is retried, so it neither waits minutes nor stalls other sessions; the caller's transaction
is left as it was (no commit here).
"""
from __future__ import annotations

import logging
import re
import time

log = logging.getLogger('ddl_guard')

_ID = r'("?[\w$]+"?(?:\."?[\w$]+"?)?)'
_ADD_COL = re.compile(r'^alter\s+table\s+(?:if\s+exists\s+)?' + _ID + r'\s+add\s+column\s+if\s+not\s+exists\s+"?([\w$]+)"?', re.I)
_RLS = re.compile(r'^alter\s+table\s+(?:if\s+exists\s+)?' + _ID + r'\s+enable\s+row\s+level\s+security$', re.I)
_INDEX = re.compile(r'^create\s+(?:unique\s+)?index\s+if\s+not\s+exists\s+"?([\w$]+)"?\s+on\s+(?:only\s+)?' + _ID, re.I)
_COMMENT = re.compile(r"^comment\s+on\s+table\s+" + _ID + r"\s+is\s+'(.*)'$", re.I | re.S)


def statements(sql: str):
    """Split on a semicolon that ends a line (string literals may hold '; '), dropping -- comment lines."""
    body = '\n'.join(l for l in sql.splitlines() if not l.lstrip().startswith('--'))
    for s in re.split(r';[ \t]*(?:\n|$)', body):
        s = s.strip()
        if s:
            yield s


def _one(cur, q, args):
    cur.execute(q, args)
    return cur.fetchone()[0]


def needed(cur, stmt: str) -> bool:
    """False when the statement would change nothing (and so must not lock anything)."""
    m = _ADD_COL.match(stmt)
    if m:
        return not _one(cur, 'select exists (select 1 from pg_attribute where attrelid = to_regclass(%s) '
                             'and attname = %s and attnum > 0 and not attisdropped)', (m[1], m[2].lower()))
    m = _RLS.match(stmt)
    if m:
        return not _one(cur, 'select coalesce((select relrowsecurity from pg_class where oid = to_regclass(%s)), false)', (m[1],))
    m = _INDEX.match(stmt)
    if m:
        # an index lives in its table's schema
        return not _one(cur, 'select exists (select 1 from pg_class i join pg_class t on t.relnamespace = i.relnamespace '
                             'where t.oid = to_regclass(%s) and i.relname = %s)', (m[2], m[1].lower()))
    m = _COMMENT.match(stmt)
    if m:
        return _one(cur, "select obj_description(to_regclass(%s), 'pg_class') is distinct from %s",
                    (m[1], m[2].replace("''", "'")))
    return True


def ensure_ddl(cur, sql: str, lock_timeout: str = '10s', tries: int = 6, wait: float = 20, dry_run: bool = False):
    """Run the statements of `sql` that are still needed; returns them. dry_run: only report them."""
    ran = []
    for stmt in statements(sql):
        if not needed(cur, stmt):
            continue
        ran.append(stmt)
        if dry_run:
            continue
        for attempt in range(1, tries + 1):
            cur.execute("select current_setting('lock_timeout')")
            before = cur.fetchone()[0]
            cur.execute('savepoint ddl_guard')
            cur.execute("select set_config('lock_timeout', %s, true)", (lock_timeout,))
            try:
                cur.execute(stmt)
            except Exception as e:                       # noqa: BLE001
                cur.execute('rollback to savepoint ddl_guard')
                if getattr(e, 'pgcode', None) != '55P03' or attempt == tries:   # 55P03 = lock_not_available
                    raise
                log.warning('setup DDL waiting on a lock (%s), retry %d/%d in %.0fs: %s',
                            type(e).__name__, attempt, tries, wait, stmt.splitlines()[0][:90])
                time.sleep(wait)
                continue
            cur.execute('release savepoint ddl_guard')
            cur.execute("select set_config('lock_timeout', %s, true)", (before,))
            break
    if ran:
        log.info('setup DDL: %d statement(s) %s', len(ran), 'would run' if dry_run else 'ran')
    return ran
