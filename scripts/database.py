# scripts/database.py

import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List

DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'promotions.db'
)


def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── 1. Init ───────────────────────────────────────────────────────────────────

def init_db():
    conn = _get_conn()
    try:
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS promotions (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                bank_id       TEXT    NOT NULL,
                bank_name     TEXT    NOT NULL,
                title         TEXT    NOT NULL,
                description   TEXT    DEFAULT '',
                highlight     TEXT    DEFAULT '',
                url           TEXT    DEFAULT '',
                tc_link       TEXT    DEFAULT '',
                period        TEXT    DEFAULT '',
                end_date      TEXT    DEFAULT NULL,
                quota         TEXT    DEFAULT '',
                cost          TEXT    DEFAULT '',
                interest_rate TEXT    DEFAULT '',
                min_deposit   TEXT    DEFAULT '',
                valid_until   TEXT    DEFAULT '',
                promo_type    TEXT    DEFAULT '',
                created_at    TEXT    NOT NULL,
                last_seen     TEXT    NOT NULL,
                active        INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_bank_id   ON promotions(bank_id);
            CREATE INDEX IF NOT EXISTS idx_active    ON promotions(active);
            CREATE INDEX IF NOT EXISTS idx_last_seen ON promotions(last_seen);
        ''')

        # Live migration — add columns introduced after initial deploy
        existing_cols = [
            row[1] for row in conn.execute('PRAGMA table_info(promotions)').fetchall()
        ]
        migrations = [
            ('highlight', "ALTER TABLE promotions ADD COLUMN highlight TEXT DEFAULT ''"),
            ('tc_link',   "ALTER TABLE promotions ADD COLUMN tc_link   TEXT DEFAULT ''"),
            ('period',    "ALTER TABLE promotions ADD COLUMN period     TEXT DEFAULT ''"),
            ('end_date',  "ALTER TABLE promotions ADD COLUMN end_date   TEXT DEFAULT NULL"),
            ('quota',     "ALTER TABLE promotions ADD COLUMN quota      TEXT DEFAULT ''"),
            ('cost',      "ALTER TABLE promotions ADD COLUMN cost       TEXT DEFAULT ''"),
        ]
        for col, sql in migrations:
            if col not in existing_cols:
                conn.execute(sql)
                print(f'  ✅ DB migration: added column "{col}"')

        conn.commit()
        print('  ✅ Database ready')
    except Exception as e:
        print(f'  ❌ init_db error: {e}')
        raise
    finally:
        conn.close()


# ── 2. Save (upsert) ──────────────────────────────────────────────────────────

def save_promotions(promos: List[Dict[str, Any]]):
    if not promos:
        return

    conn = _get_conn()
    now  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ins = upd = skip = 0

    try:
        for promo in promos:
            bank_id = (promo.get('bank_id') or promo.get('bank') or '').strip()
            title   = (promo.get('title')   or promo.get('name')  or '').strip()

            if not bank_id or not title:
                skip += 1
                continue

            bank_name   = promo.get('bank_name')  or promo.get('bName')    or ''
            description = promo.get('description') or ''
            highlight   = promo.get('highlight')   or ''
            url         = promo.get('url')         or promo.get('link')     or ''
            tc_link     = promo.get('tc_link')     or url
            period      = promo.get('period')      or promo.get('valid_until') or ''
            end_date    = promo.get('end_date')    or None
            quota       = promo.get('quota')       or ''
            cost        = promo.get('cost')        or ''

            raw_types  = promo.get('promo_type') or promo.get('types') or ''
            promo_type = (
                ', '.join(raw_types) if isinstance(raw_types, list) else str(raw_types)
            )

            existing = conn.execute(
                'SELECT id FROM promotions WHERE bank_id = ? AND title = ?',
                (bank_id, title),
            ).fetchone()

            if existing:
                conn.execute('''
                    UPDATE promotions SET
                        description=?, highlight=?, url=?, tc_link=?,
                        period=?, end_date=?, quota=?, cost=?,
                        interest_rate=?, min_deposit=?, valid_until=?,
                        promo_type=?, last_seen=?, active=1
                    WHERE id=?
                ''', (
                    description, highlight, url, tc_link, period, end_date,
                    quota, cost,
                    promo.get('interest_rate', ''), promo.get('min_deposit', ''),
                    period, promo_type, now, existing['id'],
                ))
                upd += 1
            else:
                conn.execute('''
                    INSERT INTO promotions
                        (bank_id, bank_name, title, description, highlight, url, tc_link,
                         period, end_date, quota, cost,
                         interest_rate, min_deposit, valid_until, promo_type,
                         created_at, last_seen, active)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                ''', (
                    bank_id, bank_name, title, description, highlight, url, tc_link,
                    period, end_date, quota, cost,
                    promo.get('interest_rate', ''), promo.get('min_deposit', ''),
                    period, promo_type, now, now,
                ))
                ins += 1

        conn.commit()
        print(f'  💾 DB: {ins} inserted  |  {upd} updated  |  {skip} skipped')
    except Exception as e:
        conn.rollback()
        print(f'  ❌ save_promotions error: {e}')
        raise
    finally:
        conn.close()


# ── 3. Load ───────────────────────────────────────────────────────────────────

def load_promotions(active_only: bool = True) -> List[Dict[str, Any]]:
    conn = _get_conn()
    try:
        where = 'WHERE active = 1' if active_only else ''
        rows  = conn.execute(f'''
            SELECT * FROM promotions {where}
            ORDER BY bank_id ASC, last_seen DESC
        ''').fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f'  ❌ load_promotions error: {e}')
        return []
    finally:
        conn.close()


# ── 4. Mark old as inactive ───────────────────────────────────────────────────

def mark_inactive_old(days_threshold: int = 90):
    cutoff = (datetime.now() - timedelta(days=days_threshold)
              ).strftime('%Y-%m-%d %H:%M:%S')
    conn = _get_conn()
    try:
        cur = conn.execute('''
            UPDATE promotions SET active = 0
            WHERE last_seen < ? AND active = 1
        ''', (cutoff,))
        conn.commit()
        print(f'  🗑️  {cur.rowcount} old promotions marked inactive '
              f'(threshold: {days_threshold} days)')
    except Exception as e:
        conn.rollback()
        print(f'  ❌ mark_inactive_old error: {e}')
    finally:
        conn.close()


# ── 5. Export to JSON for website ─────────────────────────────────────────────

def export_to_json(output_path: str):
    """Export ALL promotions (active + expired) to docs/data.json for the website."""
    all_promos = load_promotions(active_only=False)

    records = []
    for p in all_promos:
        raw_type = p.get('promo_type') or ''
        if isinstance(raw_type, str):
            types_list = [t.strip() for t in raw_type.split(',') if t.strip()]
        else:
            types_list = list(raw_type)
        if not types_list:
            types_list = ['Others']

        records.append({
            'id':          p.get('id'),
            'bank_id':     p.get('bank_id', ''),
            'bank_name':   p.get('bank_name', ''),
            'title':       p.get('title') or '',
            'highlight':   p.get('highlight') or '',
            'description': p.get('description') or '',
            'period':      p.get('period') or p.get('valid_until') or 'Ongoing',
            'end_date':    p.get('end_date'),
            'quota':       p.get('quota') or '',
            'cost':        p.get('cost') or '',
            'types':       types_list,
            'url':         p.get('url') or '',
            'tc_link':     p.get('tc_link') or p.get('url') or '',
            'active':      bool(p.get('active', 1)),
            'created_at':  p.get('created_at') or '',
            'last_seen':   p.get('last_seen') or '',
        })

    data = {
        'updated':    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'promotions': records,
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    active_n  = sum(1 for r in records if r['active'])
    expired_n = sum(1 for r in records if not r['active'])
    print(f'  📄 data.json exported → {active_n} active, {expired_n} expired → {output_path}')