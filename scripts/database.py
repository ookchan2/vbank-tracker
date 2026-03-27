# scripts/database.py
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List

# DB 存放在專案根目錄 data/ 資料夾
DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'promotions.db'
)


# ── 取得連線 ────────────────────────────────────────────────────────────────
def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row   # 讓 row 可用 dict 方式存取
    return conn


# ── 1. 建表 ─────────────────────────────────────────────────────────────────
def init_db():
    """建立 promotions 資料表（如不存在）。"""
    conn = _get_conn()
    try:
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS promotions (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                bank_id       TEXT    NOT NULL,
                bank_name     TEXT    NOT NULL,
                title         TEXT    NOT NULL,
                description   TEXT    DEFAULT '',
                url           TEXT    DEFAULT '',
                interest_rate TEXT    DEFAULT '',
                min_deposit   TEXT    DEFAULT '',
                valid_until   TEXT    DEFAULT '',
                promo_type    TEXT    DEFAULT '',
                created_at    TEXT    NOT NULL,
                last_seen     TEXT    NOT NULL,
                active        INTEGER NOT NULL DEFAULT 1
            );

            CREATE INDEX IF NOT EXISTS idx_bank_id ON promotions(bank_id);
            CREATE INDEX IF NOT EXISTS idx_active  ON promotions(active);
            CREATE INDEX IF NOT EXISTS idx_last_seen ON promotions(last_seen);
        ''')
        conn.commit()
        print('  ✅ Database ready')
    except Exception as e:
        print(f'  ❌ init_db error: {e}')
        raise
    finally:
        conn.close()


# ── 2. 儲存促銷（upsert） ────────────────────────────────────────────────────
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
            url         = promo.get('url')         or promo.get('link')     or ''
            valid_until = (
                promo.get('valid_until') or
                promo.get('end_date')    or
                promo.get('period')      or ''
            )
            raw_types  = promo.get('promo_type') or promo.get('types') or ''
            promo_type = (
                ', '.join(raw_types)
                if isinstance(raw_types, list)
                else str(raw_types)
            )

            existing = conn.execute(
                'SELECT id FROM promotions WHERE bank_id = ? AND title = ?',
                (bank_id, title),
            ).fetchone()

            if existing:
                conn.execute('''
                    UPDATE promotions SET
                        description   = ?,
                        url           = ?,
                        interest_rate = ?,
                        min_deposit   = ?,
                        valid_until   = ?,
                        promo_type    = ?,
                        last_seen     = ?,
                        active        = 1
                    WHERE id = ?
                ''', (
                    description,
                    url,
                    promo.get('interest_rate', ''),
                    promo.get('min_deposit',   ''),
                    valid_until,
                    promo_type,
                    now,
                    existing['id'],
                ))
                upd += 1
            else:
                conn.execute('''
                    INSERT INTO promotions
                        (bank_id, bank_name, title, description, url,
                         interest_rate, min_deposit, valid_until, promo_type,
                         created_at, last_seen, active)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,1)
                ''', (
                    bank_id,
                    bank_name,
                    title,
                    description,
                    url,
                    promo.get('interest_rate', ''),
                    promo.get('min_deposit',   ''),
                    valid_until,
                    promo_type,
                    now,
                    now,
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


# ── 3. 讀取促銷 ──────────────────────────────────────────────────────────────
def load_promotions(active_only: bool = True) -> List[Dict[str, Any]]:
    """從 DB 讀取促銷，按 bank_id 升序、last_seen 降序排列。"""
    conn = _get_conn()
    try:
        where = 'WHERE active = 1' if active_only else ''
        rows  = conn.execute(f'''
            SELECT * FROM promotions
            {where}
            ORDER BY bank_id ASC, last_seen DESC
        ''').fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f'  ❌ load_promotions error: {e}')
        return []
    finally:
        conn.close()


# ── 4. 標記舊記錄為 inactive ─────────────────────────────────────────────────
def mark_inactive_old(days_threshold: int = 90):
    """將超過 days_threshold 天未見到的 active 記錄設為 inactive。"""
    cutoff = (datetime.now() - timedelta(days=days_threshold)
              ).strftime('%Y-%m-%d %H:%M:%S')
    conn = _get_conn()
    try:
        cur = conn.execute('''
            UPDATE promotions
            SET    active = 0
            WHERE  last_seen < ?
            AND    active   = 1
        ''', (cutoff,))
        conn.commit()
        print(f'  🗑️  {cur.rowcount} old promotions marked inactive '
              f'(threshold: {days_threshold} days)')
    except Exception as e:
        conn.rollback()
        print(f'  ❌ mark_inactive_old error: {e}')
    finally:
        conn.close()