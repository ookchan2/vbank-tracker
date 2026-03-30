# scripts/database.py

import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'promotions.db'
)


def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')   # 並發更安全
    conn.execute('PRAGMA foreign_keys=ON')
    return conn


# ── 1. Init ───────────────────────────────────────────────────────────────────

def init_db():
    """建立資料表並執行 live migration。"""
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
                category      TEXT    DEFAULT '',
                url           TEXT    DEFAULT '',
                tc_link       TEXT    DEFAULT '',
                period        TEXT    DEFAULT '',
                end_date      TEXT    DEFAULT NULL,
                quota         TEXT    DEFAULT '',
                cost          TEXT    DEFAULT '',
                interest_rate TEXT    DEFAULT '',
                min_deposit   TEXT    DEFAULT '',
                promo_type    TEXT    DEFAULT '',
                created_at    TEXT    NOT NULL,
                last_seen     TEXT    NOT NULL,
                active        INTEGER NOT NULL DEFAULT 1
            );

            CREATE INDEX IF NOT EXISTS idx_bank_id    ON promotions(bank_id);
            CREATE INDEX IF NOT EXISTS idx_active     ON promotions(active);
            CREATE INDEX IF NOT EXISTS idx_last_seen  ON promotions(last_seen);
            CREATE INDEX IF NOT EXISTS idx_created_at ON promotions(created_at);
        ''')

        # Live migration — 安全可重複執行
        existing_cols = {
            row[1] for row in conn.execute('PRAGMA table_info(promotions)').fetchall()
        }
        migrations = [
            ('highlight',     "ALTER TABLE promotions ADD COLUMN highlight     TEXT DEFAULT ''"),
            ('tc_link',       "ALTER TABLE promotions ADD COLUMN tc_link       TEXT DEFAULT ''"),
            ('period',        "ALTER TABLE promotions ADD COLUMN period        TEXT DEFAULT ''"),
            ('end_date',      "ALTER TABLE promotions ADD COLUMN end_date      TEXT DEFAULT NULL"),
            ('quota',         "ALTER TABLE promotions ADD COLUMN quota         TEXT DEFAULT ''"),
            ('cost',          "ALTER TABLE promotions ADD COLUMN cost          TEXT DEFAULT ''"),
            ('category',      "ALTER TABLE promotions ADD COLUMN category      TEXT DEFAULT ''"),
            ('interest_rate', "ALTER TABLE promotions ADD COLUMN interest_rate TEXT DEFAULT ''"),
            ('min_deposit',   "ALTER TABLE promotions ADD COLUMN min_deposit   TEXT DEFAULT ''"),
        ]
        for col, sql in migrations:
            if col not in existing_cols:
                conn.execute(sql)
                print(f'  🔧 DB migration: added column "{col}"')

        conn.commit()
        print('  ✅ Database ready')
    except Exception as e:
        print(f'  ❌ init_db error: {e}')
        raise
    finally:
        conn.close()


# ── 2. Save (upsert) ──────────────────────────────────────────────────────────

def save_promotions(promos: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """
    Upsert promotions。Match key = (bank_id, title)。
    Returns (inserted, updated, skipped).
    """
    if not promos:
        return 0, 0, 0

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

            # ── 欄位正規化 ────────────────────────────────────────────────────
            bank_name     = str(promo.get('bank_name')     or promo.get('bName')      or bank_id)
            description   = str(promo.get('description')   or '')
            highlight     = str(promo.get('highlight')     or '')
            category      = str(promo.get('category')      or '')
            url           = str(promo.get('url')           or promo.get('link')        or '')
            tc_link       = str(promo.get('tc_link')       or url)
            period        = str(promo.get('period')        or promo.get('valid_until') or '')
            end_date      = promo.get('end_date') or None
            quota         = str(promo.get('quota')         or '')
            cost          = str(promo.get('cost')          or '')
            interest_rate = str(promo.get('interest_rate') or '')
            min_deposit   = str(promo.get('min_deposit')   or '')

            raw_type   = promo.get('promo_type') or promo.get('types') or ''
            promo_type = ', '.join(raw_type) if isinstance(raw_type, list) else str(raw_type)

            existing = conn.execute(
                'SELECT id FROM promotions WHERE bank_id = ? AND title = ?',
                (bank_id, title),
            ).fetchone()

            if existing:
                conn.execute('''
                    UPDATE promotions SET
                        bank_name=?, description=?, highlight=?, category=?,
                        url=?, tc_link=?, period=?, end_date=?,
                        quota=?, cost=?, interest_rate=?, min_deposit=?,
                        promo_type=?, last_seen=?, active=1
                    WHERE id=?
                ''', (
                    bank_name, description, highlight, category,
                    url, tc_link, period, end_date,
                    quota, cost, interest_rate, min_deposit,
                    promo_type, now, existing['id'],
                ))
                upd += 1
            else:
                conn.execute('''
                    INSERT INTO promotions (
                        bank_id, bank_name, title, description, highlight, category,
                        url, tc_link, period, end_date, quota, cost,
                        interest_rate, min_deposit, promo_type,
                        created_at, last_seen, active
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                ''', (
                    bank_id, bank_name, title, description, highlight, category,
                    url, tc_link, period, end_date, quota, cost,
                    interest_rate, min_deposit, promo_type,
                    now, now,
                ))
                ins += 1

        conn.commit()
        print(f'  💾 DB: {ins} inserted  |  {upd} updated  |  {skip} skipped')
        return ins, upd, skip

    except Exception as e:
        conn.rollback()
        print(f'  ❌ save_promotions error: {e}')
        raise
    finally:
        conn.close()


# ── 3. Mark stale / old inactive ─────────────────────────────────────────────

def mark_stale_as_inactive(bank_ids_scraped: List[str], today_str: str = None) -> int:
    """
    針對今日成功爬取的銀行，把「今天沒出現」的 promo 標為 inactive（已下架）。
    只處理有爬到的銀行，避免爬蟲失敗時誤刪全部資料。
    Returns: 被標 inactive 的筆數。
    """
    if not bank_ids_scraped:
        return 0

    today_str = today_str or datetime.now().strftime('%Y-%m-%d')
    conn = _get_conn()
    total = 0
    try:
        for bank_id in bank_ids_scraped:
            cur = conn.execute('''
                UPDATE promotions SET active = 0
                WHERE bank_id = ?
                  AND active  = 1
                  AND DATE(last_seen) < ?
            ''', (bank_id, today_str))
            if cur.rowcount:
                print(f'  🗑️  {bank_id}: {cur.rowcount} promo(s) marked inactive')
            total += cur.rowcount
        conn.commit()
        return total
    except Exception as e:
        conn.rollback()
        print(f'  ❌ mark_stale_as_inactive error: {e}')
        return 0
    finally:
        conn.close()


def mark_inactive_old(days_threshold: int = 90) -> int:
    """Fallback: 超過 N 天沒出現的 promo 一律標 inactive。"""
    cutoff = (datetime.now() - timedelta(days=days_threshold)).strftime('%Y-%m-%d %H:%M:%S')
    conn = _get_conn()
    try:
        cur = conn.execute(
            'UPDATE promotions SET active = 0 WHERE last_seen < ? AND active = 1',
            (cutoff,)
        )
        conn.commit()
        print(f'  🗑️  {cur.rowcount} old promos marked inactive (>{days_threshold}d)')
        return cur.rowcount
    except Exception as e:
        conn.rollback()
        print(f'  ❌ mark_inactive_old error: {e}')
        return 0
    finally:
        conn.close()


# ── 4. Report 查詢 ─────────────────────────────────────────────────────────────

def _to_dicts(rows) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows]


def get_new_promotions(today_str: str = None) -> List[Dict[str, Any]]:
    """今天首次出現的 promo（created_at date = today）。"""
    today_str = today_str or datetime.now().strftime('%Y-%m-%d')
    conn = _get_conn()
    try:
        return _to_dicts(conn.execute('''
            SELECT * FROM promotions
            WHERE DATE(created_at) = ? AND active = 1
            ORDER BY bank_id ASC, id ASC
        ''', (today_str,)).fetchall())
    except Exception as e:
        print(f'  ❌ get_new_promotions error: {e}')
        return []
    finally:
        conn.close()


def get_active_promotions(today_str: str = None) -> List[Dict[str, Any]]:
    """持續有效、不是今天才新增的 promo。"""
    today_str = today_str or datetime.now().strftime('%Y-%m-%d')
    conn = _get_conn()
    try:
        return _to_dicts(conn.execute('''
            SELECT * FROM promotions
            WHERE active = 1 AND DATE(created_at) != ?
            ORDER BY bank_id ASC, last_seen DESC
        ''', (today_str,)).fetchall())
    except Exception as e:
        print(f'  ❌ get_active_promotions error: {e}')
        return []
    finally:
        conn.close()


def get_expired_promotions(today_str: str = None) -> List[Dict[str, Any]]:
    """今天或昨天剛下架的 promo（active=0，last_seen >= 昨天）。"""
    today_str     = today_str or datetime.now().strftime('%Y-%m-%d')
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    conn = _get_conn()
    try:
        return _to_dicts(conn.execute('''
            SELECT * FROM promotions
            WHERE active = 0 AND DATE(last_seen) >= ?
            ORDER BY bank_id ASC, last_seen DESC
        ''', (yesterday_str,)).fetchall())
    except Exception as e:
        print(f'  ❌ get_expired_promotions error: {e}')
        return []
    finally:
        conn.close()


def generate_daily_report(today_str: str = None) -> Dict[str, Any]:
    """
    彙整每日報告，供 main.py 直接使用。

    Returns:
        {
            'new':     [...],
            'active':  [...],
            'expired': [...],
            'summary': {
                'total_active': int,
                'new_count':    int,
                'expired_count':int,
                'by_bank':      {bank_id: count},
            }
        }
    """
    today_str = today_str or datetime.now().strftime('%Y-%m-%d')

    new_promos     = get_new_promotions(today_str)
    active_promos  = get_active_promotions(today_str)
    expired_promos = get_expired_promotions(today_str)

    by_bank: Dict[str, int] = {}
    for p in new_promos + active_promos:
        bid = p.get('bank_id', 'unknown')
        by_bank[bid] = by_bank.get(bid, 0) + 1

    report = {
        'new':     new_promos,
        'active':  active_promos,
        'expired': expired_promos,
        'summary': {
            'total_active':  len(new_promos) + len(active_promos),
            'new_count':     len(new_promos),
            'expired_count': len(expired_promos),
            'by_bank':       by_bank,
        },
    }

    print(
        f'  📊 Daily report — '
        f'🆕 {len(new_promos)} new  |  '
        f'✅ {len(active_promos)} active  |  '
        f'❌ {len(expired_promos)} expired'
    )
    return report


# ── 5. Load (一般用途) ────────────────────────────────────────────────────────

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


# ── 6. Export to JSON for website ─────────────────────────────────────────────

def export_to_json(output_path: str):
    """Export ALL promotions to docs/data.json for the GitHub Pages website."""
    all_promos = load_promotions(active_only=False)

    records = []
    for p in all_promos:
        raw_type   = p.get('promo_type') or ''
        types_list = (
            [t.strip() for t in raw_type.split(',') if t.strip()]
            if isinstance(raw_type, str) else list(raw_type)
        )
        if not types_list:
            types_list = ['Others']

        records.append({
            'id':          p.get('id'),
            'bank_id':     p.get('bank_id',     ''),
            'bank_name':   p.get('bank_name',   ''),
            'title':       p.get('title')       or '',
            'highlight':   p.get('highlight')   or '',
            'description': p.get('description') or '',
            'category':    p.get('category')    or '',
            'period':      p.get('period')      or 'Ongoing',
            'end_date':    p.get('end_date'),
            'quota':       p.get('quota')       or '',
            'cost':        p.get('cost')        or '',
            'types':       types_list,
            'url':         p.get('url')         or '',
            'tc_link':     p.get('tc_link')     or p.get('url') or '',
            'active':      bool(p.get('active', 1)),
            'created_at':  p.get('created_at')  or '',
            'last_seen':   p.get('last_seen')   or '',
        })

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(
            {'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'promotions': records},
            f, ensure_ascii=False, indent=2,
        )

    active_n  = sum(1 for r in records if     r['active'])
    expired_n = sum(1 for r in records if not r['active'])
    print(f'  📄 data.json → {active_n} active, {expired_n} expired → {output_path}')