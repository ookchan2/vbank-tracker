# scripts/db.py  ── 新增文件（若之前沒有）

import json, os, sqlite3, hashlib
from datetime import datetime

DB_PATH = os.environ.get('DB_PATH', 'promotions.db')


def init_db():
    with sqlite3.connect(DB_PATH) as c:
        c.execute('''
            CREATE TABLE IF NOT EXISTS promotions (
                id          TEXT PRIMARY KEY,
                bank        TEXT NOT NULL,
                bName       TEXT NOT NULL,
                name        TEXT NOT NULL,
                types       TEXT,
                period      TEXT,
                end_date    TEXT,
                highlight   TEXT,
                description TEXT,
                quota       TEXT,
                cost        TEXT,
                link        TEXT,
                first_seen  TEXT,
                last_seen   TEXT,
                is_active   INTEGER DEFAULT 1
            )
        ''')
        c.commit()
    print(f'  DB ready: {DB_PATH}')


def _pid(bank, name):
    return hashlib.md5(f'{bank}|{name}'.encode()).hexdigest()[:14]


def save_promotions(promos: list):
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect(DB_PATH) as c:
        for p in promos:
            pid = _pid(p.get('bank', ''), p.get('name', ''))
            exists = c.execute(
                'SELECT id FROM promotions WHERE id=?', (pid,)
            ).fetchone()

            if exists:
                c.execute('''
                    UPDATE promotions
                    SET last_seen=?, highlight=?, description=?,
                        period=?, end_date=?, is_active=1
                    WHERE id=?
                ''', (today,
                      p.get('highlight', ''), p.get('description', ''),
                      p.get('period', ''),    p.get('end_date'),
                      pid))
            else:
                c.execute('''
                    INSERT INTO promotions
                    (id,bank,bName,name,types,period,end_date,
                     highlight,description,quota,cost,link,
                     first_seen,last_seen,is_active)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                ''', (
                    pid,
                    p.get('bank', ''),
                    p.get('bName', ''),
                    p.get('name', ''),
                    json.dumps(p.get('types', []), ensure_ascii=False),
                    p.get('period', ''),
                    p.get('end_date'),
                    p.get('highlight', ''),
                    p.get('description', ''),
                    p.get('quota', ''),
                    p.get('cost', ''),
                    p.get('link', ''),
                    today, today
                ))
        c.commit()


def load_promotions(active_only=True) -> list:
    if not os.path.exists(DB_PATH):
        return []
    with sqlite3.connect(DB_PATH) as c:
        c.row_factory = sqlite3.Row
        sql = 'SELECT * FROM promotions'
        if active_only:
            sql += ' WHERE is_active=1'
        sql += ' ORDER BY bank, name'
        rows = c.execute(sql).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        try:
            d['types'] = json.loads(d.get('types') or '[]')
        except Exception:
            d['types'] = ['Others']
        result.append(d)
    return result


def mark_inactive_old(days_threshold=90):
    """超過 N 天未見的促銷標記為 inactive。"""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=days_threshold)).strftime('%Y-%m-%d')
    with sqlite3.connect(DB_PATH) as c:
        c.execute(
            'UPDATE promotions SET is_active=0 WHERE last_seen < ?',
            (cutoff,)
        )
        c.commit()