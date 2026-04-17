# scripts/database.py

import json
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional

DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'promotions.db'
)


def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')
    return conn


@contextmanager
def _db_connection():
    """
    Context manager that opens, yields, and closes a connection.
    Replaces the repetitive try/finally/conn.close() pattern in every function
    and ensures the connection is always closed even if the caller raises.
    """
    conn = _get_conn()
    try:
        yield conn
    finally:
        conn.close()


# ── 0. Schema rebuild helper ──────────────────────────────────────────────────

def _rebuild_promotions_table(conn: sqlite3.Connection) -> None:
    existing = {
        row[1] for row in conn.execute('PRAGMA table_info(promotions)').fetchall()
    }

    if 'bank_id' in existing and 'bank' in existing:
        bank_id_expr = "COALESCE(NULLIF(bank_id, ''), bank, '')"
    elif 'bank' in existing:
        bank_id_expr = "COALESCE(bank, '')"
    else:
        bank_id_expr = "COALESCE(bank_id, '')"

    def _col(name: str, default: str = "''") -> str:
        return f'COALESCE({name}, {default})' if name in existing else default

    def _nullable(name: str) -> str:
        return name if name in existing else 'NULL'

    conn.execute('DROP TABLE IF EXISTS _promotions_rebuild')
    conn.execute('''
        CREATE TABLE _promotions_rebuild (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_id       TEXT    NOT NULL DEFAULT '',
            bank_name     TEXT    NOT NULL DEFAULT '',
            title         TEXT    NOT NULL DEFAULT '',
            description   TEXT    DEFAULT '',
            highlight     TEXT    DEFAULT '',
            category      TEXT    DEFAULT '',
            url           TEXT    DEFAULT '',
            tc_link       TEXT    DEFAULT '',
            start_date    TEXT    DEFAULT NULL,
            period        TEXT    DEFAULT '',
            end_date      TEXT    DEFAULT NULL,
            quota         TEXT    DEFAULT '',
            cost          TEXT    DEFAULT '',
            interest_rate TEXT    DEFAULT '',
            min_deposit   TEXT    DEFAULT '',
            promo_type    TEXT    DEFAULT '',
            is_bau        INTEGER DEFAULT 0,
            first_run_id  INTEGER DEFAULT NULL,
            created_at    TEXT    NOT NULL DEFAULT '',
            last_seen     TEXT    NOT NULL DEFAULT '',
            active        INTEGER NOT NULL DEFAULT 1
        )
    ''')

    conn.execute(f'''
        INSERT INTO _promotions_rebuild
            (id, bank_id, bank_name, title, description, highlight, category,
             url, tc_link, start_date, period, end_date, quota, cost,
             interest_rate, min_deposit, promo_type, is_bau, first_run_id,
             created_at, last_seen, active)
        SELECT
            id,
            {bank_id_expr},
            {_col('bank_name')},
            {_col('title')},
            {_col('description')},
            {_col('highlight')},
            {_col('category')},
            {_col('url')},
            {_col('tc_link')},
            {_nullable('start_date')},
            {_col('period')},
            {_nullable('end_date')},
            {_col('quota')},
            {_col('cost')},
            {_col('interest_rate')},
            {_col('min_deposit')},
            {_col('promo_type')},
            {_col('is_bau', '0')},
            {_nullable('first_run_id')},
            {_col('created_at')},
            {_col('last_seen')},
            {_col('active', '1')}
        FROM promotions
    ''')

    conn.execute('DROP TABLE promotions')
    conn.execute('ALTER TABLE _promotions_rebuild RENAME TO promotions')
    conn.commit()


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
                category      TEXT    DEFAULT '',
                url           TEXT    DEFAULT '',
                tc_link       TEXT    DEFAULT '',
                start_date    TEXT    DEFAULT NULL,
                period        TEXT    DEFAULT '',
                end_date      TEXT    DEFAULT NULL,
                quota         TEXT    DEFAULT '',
                cost          TEXT    DEFAULT '',
                interest_rate TEXT    DEFAULT '',
                min_deposit   TEXT    DEFAULT '',
                promo_type    TEXT    DEFAULT '',
                is_bau        INTEGER DEFAULT 0,
                first_run_id  INTEGER DEFAULT NULL,
                created_at    TEXT    NOT NULL,
                last_seen     TEXT    NOT NULL,
                active        INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS scrape_runs (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at        TEXT    NOT NULL,
                banks_scraped TEXT    DEFAULT ''
            );
        ''')

        existing_cols = {
            row[1] for row in conn.execute('PRAGMA table_info(promotions)').fetchall()
        }

        if 'bank' in existing_cols:
            _rebuild_promotions_table(conn)
            existing_cols = {
                row[1] for row in conn.execute('PRAGMA table_info(promotions)').fetchall()
            }
            print('  🔧 DB migration: rebuilt table (legacy "bank" → "bank_id")')

        migrations = [
            ('bank_id',       "ALTER TABLE promotions ADD COLUMN bank_id       TEXT NOT NULL DEFAULT ''"),
            ('bank_name',     "ALTER TABLE promotions ADD COLUMN bank_name     TEXT NOT NULL DEFAULT ''"),
            ('url',           "ALTER TABLE promotions ADD COLUMN url           TEXT DEFAULT ''"),
            ('highlight',     "ALTER TABLE promotions ADD COLUMN highlight     TEXT DEFAULT ''"),
            ('tc_link',       "ALTER TABLE promotions ADD COLUMN tc_link       TEXT DEFAULT ''"),
            ('start_date',    "ALTER TABLE promotions ADD COLUMN start_date    TEXT DEFAULT NULL"),
            ('period',        "ALTER TABLE promotions ADD COLUMN period        TEXT DEFAULT ''"),
            ('end_date',      "ALTER TABLE promotions ADD COLUMN end_date      TEXT DEFAULT NULL"),
            ('quota',         "ALTER TABLE promotions ADD COLUMN quota         TEXT DEFAULT ''"),
            ('cost',          "ALTER TABLE promotions ADD COLUMN cost          TEXT DEFAULT ''"),
            ('category',      "ALTER TABLE promotions ADD COLUMN category      TEXT DEFAULT ''"),
            ('interest_rate', "ALTER TABLE promotions ADD COLUMN interest_rate TEXT DEFAULT ''"),
            ('min_deposit',   "ALTER TABLE promotions ADD COLUMN min_deposit   TEXT DEFAULT ''"),
            ('promo_type',    "ALTER TABLE promotions ADD COLUMN promo_type    TEXT DEFAULT ''"),
            ('is_bau',        "ALTER TABLE promotions ADD COLUMN is_bau        INTEGER DEFAULT 0"),
            ('first_run_id',  "ALTER TABLE promotions ADD COLUMN first_run_id  INTEGER DEFAULT NULL"),
        ]
        for col, sql in migrations:
            if col not in existing_cols:
                conn.execute(sql)
                print(f'  🔧 DB migration: added column "{col}"')

        conn.executescript('''
            CREATE INDEX IF NOT EXISTS idx_bank_id    ON promotions(bank_id);
            CREATE INDEX IF NOT EXISTS idx_active     ON promotions(active);
            CREATE INDEX IF NOT EXISTS idx_last_seen  ON promotions(last_seen);
            CREATE INDEX IF NOT EXISTS idx_created_at ON promotions(created_at);
            CREATE INDEX IF NOT EXISTS idx_first_run  ON promotions(first_run_id);
            CREATE INDEX IF NOT EXISTS idx_is_bau     ON promotions(is_bau);
            CREATE INDEX IF NOT EXISTS idx_bank_name  ON promotions(bank_name);
        ''')

        conn.commit()
        print('  ✅ Database ready')
    except Exception as exc:
        print(f'  ❌ init_db error: {exc}')
        raise
    finally:
        conn.close()


# ── 2. Run tracking ───────────────────────────────────────────────────────────

def start_new_run(banks: List[str] = None) -> int:
    with _db_connection() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO scrape_runs (run_at, banks_scraped) VALUES (?, ?)",
                (
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    ','.join(banks or []),
                ),
            )
            conn.commit()
            run_id = cur.lastrowid
            print(f'  🏃 Scrape run #{run_id} started')
            return run_id
        except Exception as exc:
            print(f'  ❌ start_new_run error: {exc}')
            return 0


def get_previous_run_id(current_run_id: int) -> Optional[int]:
    with _db_connection() as conn:
        try:
            row = conn.execute(
                "SELECT id FROM scrape_runs WHERE id < ? ORDER BY id DESC LIMIT 1",
                (current_run_id,),
            ).fetchone()
            return row['id'] if row else None
        except Exception as exc:
            print(f'  ❌ get_previous_run_id error: {exc}')
            return None


# ── 3. Dedup helpers ──────────────────────────────────────────────────────────

_JACCARD_THRESHOLD = 0.50
_LCP_THRESHOLD     = 0.72
_MIN_NORM_LEN      = 10
_MIN_TOKENS        = 2

_RE_NONALNUM   = re.compile(r'[\s\W]+')
_RE_INSTALMENT = re.compile(r'installment', re.IGNORECASE)
_RE_AMOUNT     = re.compile(r'(?:hkd|usd|rmb|sgd|cny)\s*[\d,]+(?:\.\d+)?', re.IGNORECASE)
_RE_PCT        = re.compile(r'\d+(?:\.\d+)?\s*%')

_PROMO_CODE_SKIP = frozenset({
    'SWIFT', 'VISA', 'FPS', 'ATM', 'HKD', 'USD', 'APR', 'ETF', 'IPO',
    'HKT', 'CSL', 'USA', 'UFO', 'VIP', 'APP', 'SMS', 'PIN', 'QR',
    'MOX', 'ZA', 'ANT', 'PAO', 'LIVI', 'AIRSTAR', 'FUSION', 'WELAB',
})


def _extract_promo_code_stem(title: str) -> Optional[str]:
    if not title:
        return None
    for m in re.finditer(r'\b([A-Z][A-Z0-9]{3,14})\b', title):
        code = m.group(1)
        if code in _PROMO_CODE_SKIP:
            continue
        if len(re.findall(r'[A-Z]', code)) < 2:
            continue
        stem = re.sub(r'\d{2}$', '', code)
        return stem if len(stem) >= 3 else code
    for m in re.finditer(r'\b(\d+[A-Z]{2,}(?:\d+)?)\b', title):
        return m.group(1)
    return None


_SYNONYM_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'余[額额]\+'), 'depositplus'),
    (re.compile(r'\bdeposit[- ]?plus\b', re.IGNORECASE), 'depositplus'),
    (re.compile(r'balance\+', re.IGNORECASE), 'depositplus'),
    (re.compile(
        r'\b(?:daily|high|boosted?|tiered?)[- ]?interest[- ]?(?:saving\w*|earn\w*|account\w*)?\b',
        re.IGNORECASE,
    ), 'hisavings'),
    (re.compile(
        r'(?:'
        r'(?:zero|0)[- ]?(?:brokerage|commission)[- ]?fees?'
        r'|commission[- ]?free[- ]?(?:stock|trading)?'
        r'|no[- ]?brokerage'
        r'|lifetime[- ]?\$0[- ]?commission'
        r'|\$0[- ]?commission'
        r')',
        re.IGNORECASE,
    ), 'zerobrokfee'),
    (re.compile(r'\bswift\b', re.IGNORECASE), 'swifttransfer'),
    (re.compile(r'\bpayment[- ]?connect\b', re.IGNORECASE), 'swifttransfer'),
    (re.compile(
        r'(?:'
        r'zero[- ]?(?:subscription[- ]?)?fees?[- ]?(?:on[- ]?(?:all[- ]?)?)?funds?'
        r'|featured?[- ]?funds?[- ]?with[- ]?zero[- ]?(?:subscription[- ]?)?fees?'
        r'|zero[- ]?fees?[- ]?fund[- ]?(?:subscription|switching)?'
        r'|zero[- ]?(?:fee|cost)[- ]?(?:investment[- ]?)?fund'
        r')',
        re.IGNORECASE,
    ), 'zerosubfee'),
    (re.compile(r'\bcrypto(?:currency|currencies|currenc\w*)?\b', re.IGNORECASE), 'crypto'),
    (re.compile(r'\bdigital[- ]?assets?\b', re.IGNORECASE), 'crypto'),
    (re.compile(r'\bvirtual[- ]?assets?\b', re.IGNORECASE), 'crypto'),
    (re.compile(r'\b(?:fx|foreign[- ]?exchange|currency[- ]?exchange|forex)\b', re.IGNORECASE), 'fxexchange'),
    (re.compile(r'\bglobal[- ]?(?:remittance|wallet)\b', re.IGNORECASE), 'fxexchange'),
    (re.compile(r'\bwelab[- ]?global[- ]?wallet\b', re.IGNORECASE), 'welabwallet'),
    (re.compile(r'\btime[- ]?deposit\b', re.IGNORECASE), 'timedeposit'),
    (re.compile(r'\bpowerdraw\b', re.IGNORECASE), 'powerdraw'),
    (re.compile(
        r'insurance[- ]?(?:product\w*)?[- ]?(?:with[- ]?)?(?:annual[- ]?rate|premium[- ]?rebate|\d+(?:\.\d+)?%)',
        re.IGNORECASE,
    ), 'insurance_rate'),
    (re.compile(
        r'\d+(?:\.\d+)?%[- ]?(?:annuali[sz]ed|annual)[- ]?rate',
        re.IGNORECASE,
    ), 'insurance_rate'),
    (re.compile(r'\bpayroll[- ]?(?:switch(?:ing)?|deposit|benefit\w*)?\b', re.IGNORECASE), 'payroll'),
    (re.compile(
        r'\b(?:(?:quick|fast|instant|mobile|online|digital)[- ]?)?account[- ]?open(?:ing)?\b',
        re.IGNORECASE,
    ), 'accountopen'),
    (re.compile(r'\b24[/×x]7\b'), 'banking247'),
    (re.compile(r'\basia[- ]?miles?\b', re.IGNORECASE), 'asiamiles'),
    (re.compile(r'\bmiles?[- ]?(?:reward|earn|redeem)\w*\b', re.IGNORECASE), 'milesreward'),
    (re.compile(r'\bgosave\b', re.IGNORECASE), 'gosave'),
    (re.compile(r'\blivisave\b', re.IGNORECASE), 'livisave'),
    (re.compile(r'\btrip\.com\b', re.IGNORECASE), 'tripcom'),
    (re.compile(r'\bxiaomi\b', re.IGNORECASE), 'xiaomi'),
    (re.compile(r'\bsamsung[- ]?s\d+\b', re.IGNORECASE), 'samsung_phone'),
    (re.compile(r'\bbest[- ]?in[- ]?town\b', re.IGNORECASE), 'bestintown'),
    (re.compile(r'\bdevice[- ]?plans?\b', re.IGNORECASE), 'deviceplan'),
    (re.compile(r'\bintegrated[- ]?investment\b', re.IGNORECASE), 'intinvest'),
    (re.compile(r'\bone[- ]?stop[- ]?(?:trading|investment)[- ]?platform\b', re.IGNORECASE), 'onestoplatform'),
    (re.compile(r'\bant[- ]?bank[- ]?investment[- ]?fund[- ]?platform\b', re.IGNORECASE), 'antfundplatform'),
    (re.compile(r'\bpersonal[- ]?(?:revolving[- ]?)?loan\b', re.IGNORECASE), 'personalloan'),
    (re.compile(
        r'(?:'
        r'zero[- ]?(?:\w+[- ]?)?fees?'
        r'|0\s*%[- ]?(?:\w+[- ]?)?fees?'
        r'|fee[- ]?(?:free|waiver|waived)'
        r'|no[- ]?(?:\w+[- ]?)?fees?'
        r')',
        re.IGNORECASE,
    ), 'zfee'),
    (re.compile(r'\b(?:quick|fast|instant|rapid|express|immediate)\b', re.IGNORECASE), 'fastspd'),
    (re.compile(
        r'\b(?:flexible|custom(?:iz\w*)?|select(?:able|ion)?|personali[sz]\w*)\b',
        re.IGNORECASE,
    ), 'flexcust'),
    (re.compile(
        r'\b(?:welcome|sign[- ]?up|new[- ]?customer|new[- ]?user)[- ]?(?:bonus|reward|offer|gift)?\b',
        re.IGNORECASE,
    ), 'welcome'),
    (re.compile(
        r'\b(?:refer(?:ral)?[- ]?(?:bonus|reward|program)?|invite[- ]?friend\w*|friend[- ]?refer\w*)\b',
        re.IGNORECASE,
    ), 'referral'),
    (re.compile(r'\b(?:cash[- ]?back|cash[- ]?rebate)\b', re.IGNORECASE), 'cashback'),
]

_NOISE_WORDS = (
    'rebateprogram', 'rewardprogram', 'program',
    'promotion', 'campaign', 'offer', 'bonus',
    'reward', 'scheme', 'deal', 'activity',
    'rebate', 'exclusive', 'special',
    'with', 'from', 'for', 'and', 'the', 'your',
)
_NOISE_PATTERNS: list[re.Pattern] = [
    re.compile(r'(?<![a-z0-9])' + re.escape(w) + r'(?![a-z0-9])', re.IGNORECASE)
    for w in _NOISE_WORDS
]

_JACCARD_STOPWORDS = frozenset({
    'for', 'with', 'and', 'or', 'the', 'a', 'an', 'of', 'on', 'in',
    'at', 'to', 'by', 'from', 'all', 'via',
    'new', 'customers', 'customer', 'users', 'user',
    'service', 'services', 'promotion', 'promotions',
    'offer', 'offering', 'program', 'feature', 'platform',
    'enhanced', 'advanced', 'exclusive', 'special',
    'get', 'your', 'our', 'rebate', 'bonus',
    'earn', 'enjoy', 'limited', 'only', 'up', 'upto',
    'valid', 'terms', 'apply', 'conditions',
    'hong', 'kong', 'hk',
    'mox', 'za', 'ant', 'airstar', 'fusion', 'pao', 'livi', 'welab',
})


@lru_cache(maxsize=4096)
def _normalize_title(title: str) -> str:
    if not title:
        return ''
    t = _RE_INSTALMENT.sub('instalment', title.lower())
    for pattern, replacement in _SYNONYM_PATTERNS:
        t = pattern.sub(replacement, t)
    t = _RE_AMOUNT.sub('', t)
    t = _RE_PCT.sub('', t)
    for pat in _NOISE_PATTERNS:
        t = pat.sub('', t)
    t = _RE_NONALNUM.sub('', t)
    return t


def _stem(tok: str) -> str:
    if len(tok) > 4 and tok.endswith('s') and not tok.endswith('ss'):
        return tok[:-1]
    return tok


@lru_cache(maxsize=4096)
def _tokenize_for_jaccard(title: str) -> frozenset:
    if not title:
        return frozenset()
    t = _RE_INSTALMENT.sub('instalment', title.lower())
    for pattern, replacement in _SYNONYM_PATTERNS:
        t = pattern.sub(replacement, t)
    t = _RE_AMOUNT.sub('', t)
    t = _RE_PCT.sub('', t)
    t = re.sub(r'[^\w\s]', ' ', t)
    return frozenset(
        _stem(tok)
        for tok in t.split()
        if tok not in _JACCARD_STOPWORDS and len(tok) > 1
    )


def _jaccard_similarity(title1: str, title2: str) -> float:
    a = _tokenize_for_jaccard(title1)
    b = _tokenize_for_jaccard(title2)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _common_prefix_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    min_len = min(len(a), len(b))
    i = 0
    while i < min_len and a[i] == b[i]:
        i += 1
    return i / min_len


def _find_duplicate_id(
    conn: sqlite3.Connection,
    bank_id: str,
    title: str,
    highlight: str,
) -> Optional[int]:
    rows = conn.execute(
        "SELECT id, title, highlight FROM promotions WHERE bank_id = ?",
        (bank_id,)
    ).fetchall()

    norm_new      = _normalize_title(title)
    hi_snippet    = (highlight or '').strip()[:150]
    new_code_stem = _extract_promo_code_stem(title)
    toks_new      = _tokenize_for_jaccard(title)

    for row in rows:
        if new_code_stem:
            old_code_stem = _extract_promo_code_stem(row['title'])
            if old_code_stem and new_code_stem == old_code_stem:
                return row['id']

        norm_old = _normalize_title(row['title'])
        old_snip = (row['highlight'] or '').strip()[:150]

        if norm_new and norm_old:
            if norm_new == norm_old:
                return row['id']

            len_new, len_old = len(norm_new), len(norm_old)
            min_len = min(len_new, len_old)
            max_len = max(len_new, len_old)
            if (
                min_len >= _MIN_NORM_LEN
                and min_len >= max_len * 0.35
                and (norm_new in norm_old or norm_old in norm_new)
            ):
                return row['id']

            toks_old = _tokenize_for_jaccard(row['title'])
            if (
                len(toks_new) >= _MIN_TOKENS
                and len(toks_old) >= _MIN_TOKENS
                and len(toks_new & toks_old) >= _MIN_TOKENS
                and _jaccard_similarity(title, row['title']) >= _JACCARD_THRESHOLD
            ):
                return row['id']

            if (
                len_new >= _MIN_NORM_LEN
                and len_old >= _MIN_NORM_LEN
                and _common_prefix_ratio(norm_new, norm_old) >= _LCP_THRESHOLD
            ):
                return row['id']

        if hi_snippet and old_snip and hi_snippet == old_snip:
            return row['id']

    return None


# ── 4. Save (upsert) ──────────────────────────────────────────────────────────

def save_promotions(
    bank_id: str,
    bank_name: str,
    promotions: List[Dict],
    current_run_id: int = 0,
    today_str: str = None,   # shared run date — prevents midnight UTC skew
) -> Dict:
    # Use the caller-supplied date so that last_seen and mark_stale_as_inactive
    # are always compared against the exact same calendar date string.
    today = today_str or datetime.now().strftime('%Y-%m-%d')
    stats = {'new': 0, 'updated': 0, 'skipped': 0}

    with _db_connection() as conn:
        try:
            for p in promotions:
                title = (
                    p.get('title') or p.get('name') or
                    p.get('promotion_name') or p.get('promo_name') or ''
                ).strip()
                highlight = (p.get('highlight') or '').strip()

                if not title:
                    stats['skipped'] += 1
                    print(
                        f'  ⚠️  [{bank_id}] skipped promo with empty title '
                        f'— keys: {list(p.keys())}'
                    )
                    continue

                types_raw  = p.get('types') or p.get('promo_type') or []
                promo_type = ','.join(types_raw) if isinstance(types_raw, list) else str(types_raw)
                is_bau     = int(bool(p.get('is_bau', False)))
                start_date = p.get('start_date') or None
                end_date   = p.get('end_date')   or None

                period = (p.get('period') or '').strip()
                if start_date and end_date:
                    period = f'{start_date} to {end_date}'
                elif start_date:
                    period = f'From {start_date}'
                elif end_date:
                    period = f'Until {end_date}'
                elif not period:
                    period = 'Ongoing'

                pre_match_id = p.pop('_matched_id', None)
                dup_id = (
                    pre_match_id
                    if pre_match_id is not None
                    else _find_duplicate_id(conn, bank_id, title, highlight)
                )

                if dup_id:
                    existing = conn.execute(
                        "SELECT title FROM promotions WHERE id = ?", (dup_id,)
                    ).fetchone()
                    keep_title = (
                        title
                        if (existing and len(title) >= len(existing['title']))
                        else (existing['title'] if existing else title)
                    )
                    conn.execute("""
                        UPDATE promotions SET
                            title       = ?,
                            highlight   = COALESCE(NULLIF(?, ''), highlight),
                            description = COALESCE(NULLIF(?, ''), description),
                            category    = COALESCE(NULLIF(?, ''), category),
                            start_date  = COALESCE(NULLIF(?, ''), start_date),
                            end_date    = COALESCE(NULLIF(?, ''), end_date),
                            period      = COALESCE(NULLIF(?, ''), period),
                            quota       = COALESCE(NULLIF(?, ''), quota),
                            cost        = COALESCE(NULLIF(?, ''), cost),
                            promo_type  = COALESCE(NULLIF(?, ''), promo_type),
                            url         = COALESCE(NULLIF(?, ''), url),
                            tc_link     = COALESCE(NULLIF(?, ''), tc_link),
                            is_bau      = ?,
                            active      = 1,
                            last_seen   = ?
                        WHERE id = ?
                    """, (
                        keep_title,
                        highlight,
                        p.get('description', ''), p.get('category', ''),
                        start_date, end_date, period,
                        p.get('quota', ''), p.get('cost', ''),
                        promo_type, p.get('url', ''), p.get('tc_link', ''),
                        is_bau, today, dup_id,
                    ))
                    stats['updated'] += 1
                else:
                    conn.execute("""
                        INSERT INTO promotions
                            (bank_id, bank_name, title, highlight, description,
                             category, start_date, period, end_date, quota, cost,
                             promo_type, url, tc_link, is_bau,
                             first_run_id, active, created_at, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    """, (
                        bank_id, bank_name, title, highlight,
                        p.get('description', ''), p.get('category', ''),
                        start_date, period, end_date,
                        p.get('quota', ''), p.get('cost', ''),
                        promo_type, p.get('url', ''), p.get('tc_link', ''),
                        is_bau,
                        current_run_id if current_run_id else None,
                        today, today,
                    ))
                    stats['new'] += 1

            conn.commit()
            print(
                f"  [{bank_id}] saved → "
                f"new:{stats['new']}  updated:{stats['updated']}  "
                f"skipped:{stats['skipped']}"
            )
            return stats

        except Exception as exc:
            conn.rollback()
            print(f'  ❌ save_promotions error: {exc}')
            raise


# ── 5. Mark stale / old inactive ─────────────────────────────────────────────

def mark_stale_as_inactive(
    bank_ids_scraped: List[str],
    today_str: str = None,   # shared run date — must match save_promotions
) -> int:
    if not bank_ids_scraped:
        return 0
    # Use the supplied date so the comparison is always against the same
    # calendar date that was written into last_seen by save_promotions.
    today_str = today_str or datetime.now().strftime('%Y-%m-%d')
    total = 0
    with _db_connection() as conn:
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
        except Exception as exc:
            conn.rollback()
            print(f'  ❌ mark_stale_as_inactive error: {exc}')
            return 0


def mark_inactive_old(days_threshold: int = 90) -> int:
    cutoff = (datetime.now() - timedelta(days=days_threshold)).strftime('%Y-%m-%d %H:%M:%S')
    with _db_connection() as conn:
        try:
            cur = conn.execute(
                'UPDATE promotions SET active = 0 WHERE last_seen < ? AND active = 1',
                (cutoff,)
            )
            conn.commit()
            print(f'  🗑️  {cur.rowcount} old promos marked inactive (>{days_threshold}d)')
            return cur.rowcount
        except Exception as exc:
            conn.rollback()
            print(f'  ❌ mark_inactive_old error: {exc}')
            return 0


def reactivate_promotions_seen_on(date_str: str) -> int:
    """
    Emergency recovery: reactivate all promotions whose last_seen date matches
    date_str but were incorrectly marked inactive (e.g. by a date-skew bug
    where save_promotions and mark_stale_as_inactive sampled datetime() on
    opposite sides of a UTC midnight boundary).

    Called automatically by main() when a post-staleness sanity check detects
    0 active promotions immediately after a successful save run.
    """
    with _db_connection() as conn:
        try:
            cur = conn.execute(
                "UPDATE promotions SET active = 1 "
                "WHERE DATE(last_seen) = ? AND active = 0",
                (date_str,)
            )
            conn.commit()
            count = cur.rowcount
            if count:
                print(f'  🔄 Recovery: reactivated {count} promo(s) with last_seen={date_str}')
            else:
                print(f'  ⚠️  Recovery: no promotions found with last_seen={date_str}')
            return count
        except Exception as exc:
            conn.rollback()
            print(f'  ❌ reactivate_promotions_seen_on error: {exc}')
            return 0


# ── 6. Queries ────────────────────────────────────────────────────────────────

def _to_dicts(rows) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows]


def get_new_promotions_for_run(
    current_run_id: int,
    include_bau: bool = False,
) -> List[Dict[str, Any]]:
    with _db_connection() as conn:
        try:
            bau_clause = '' if include_bau else 'AND is_bau = 0'
            return _to_dicts(conn.execute(f'''
                SELECT * FROM promotions
                WHERE first_run_id = ?
                  AND active = 1
                  {bau_clause}
                ORDER BY bank_id ASC, id ASC
            ''', (current_run_id,)).fetchall())
        except Exception as exc:
            print(f'  ❌ get_new_promotions_for_run error: {exc}')
            return []


def get_new_promotions_last_n_days(
    days: int = 6,
    include_bau: bool = False,
    exclude_run_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Return active promotions first seen within the past `days` days,
    excluding today's date (so it complements get_new_promotions_for_run
    which covers today).

    Pass exclude_run_id=current_run_id to also exclude any rows whose
    first_run_id matches the current run — a belt-and-suspenders guard
    in case created_at and the run boundary straddle midnight.
    """
    since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')

    with _db_connection() as conn:
        try:
            bau_clause = '' if include_bau else 'AND is_bau = 0'
            run_clause = (
                f'AND (first_run_id IS NULL OR first_run_id != {int(exclude_run_id)})'
                if exclude_run_id is not None else ''
            )
            return _to_dicts(conn.execute(f'''
                SELECT * FROM promotions
                WHERE active          = 1
                  AND DATE(created_at) >= ?
                  AND DATE(created_at) <  ?
                  {bau_clause}
                  {run_clause}
                ORDER BY created_at DESC, bank_id ASC
            ''', (since, today)).fetchall())
        except Exception as exc:
            print(f'  ❌ get_new_promotions_last_n_days error: {exc}')
            return []


def get_active_promotions(include_bau: bool = True) -> List[Dict[str, Any]]:
    with _db_connection() as conn:
        try:
            bau_clause = '' if include_bau else 'AND is_bau = 0'
            return _to_dicts(conn.execute(f'''
                SELECT * FROM promotions
                WHERE active = 1 {bau_clause}
                ORDER BY bank_id ASC, last_seen DESC
            ''').fetchall())
        except Exception as exc:
            print(f'  ❌ get_active_promotions error: {exc}')
            return []


def get_expired_promotions() -> List[Dict[str, Any]]:
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    with _db_connection() as conn:
        try:
            return _to_dicts(conn.execute('''
                SELECT * FROM promotions
                WHERE active = 0
                  AND is_bau  = 0
                  AND DATE(last_seen) >= ?
                ORDER BY bank_id ASC, last_seen DESC
            ''', (yesterday,)).fetchall())
        except Exception as exc:
            print(f'  ❌ get_expired_promotions error: {exc}')
            return []


def get_active_promos_for_bank(bank_id: str) -> List[Dict[str, Any]]:
    with _db_connection() as conn:
        try:
            return _to_dicts(conn.execute(
                'SELECT id, title FROM promotions WHERE bank_id = ? AND active = 1',
                (bank_id,)
            ).fetchall())
        except Exception as exc:
            print(f'  ❌ get_active_promos_for_bank error: {exc}')
            return []


def get_promotions_by_bank_name(bank_name: str) -> List[Dict[str, Any]]:
    """
    Return all active promotions (full rows) for a given bank_name string.
    Used as the db_fetch_fn passed to ai_helper.generate_strategic_insights()
    so that supplement_from_db() can fill sparse banks from the DB.
    """
    with _db_connection() as conn:
        try:
            return _to_dicts(conn.execute(
                'SELECT * FROM promotions WHERE bank_name = ? AND active = 1 '
                'ORDER BY last_seen DESC',
                (bank_name,)
            ).fetchall())
        except Exception as exc:
            print(f'  ❌ get_promotions_by_bank_name error: {exc}')
            return []


def get_db_stats() -> Dict[str, Any]:
    """
    Lightweight summary of DB state — used in the main() final log line and
    useful for health-check scripts.
    """
    with _db_connection() as conn:
        try:
            total    = conn.execute('SELECT COUNT(*) FROM promotions').fetchone()[0]
            active   = conn.execute('SELECT COUNT(*) FROM promotions WHERE active=1').fetchone()[0]
            bau      = conn.execute('SELECT COUNT(*) FROM promotions WHERE active=1 AND is_bau=1').fetchone()[0]
            runs     = conn.execute('SELECT COUNT(*) FROM scrape_runs').fetchone()[0]
            last_run = conn.execute(
                'SELECT run_at FROM scrape_runs ORDER BY id DESC LIMIT 1'
            ).fetchone()
            return {
                'total_promotions':  total,
                'active_promotions': active,
                'bau_promotions':    bau,
                'non_bau_active':    active - bau,
                'total_runs':        runs,
                'last_run_at':       last_run['run_at'] if last_run else None,
            }
        except Exception as exc:
            print(f'  ❌ get_db_stats error: {exc}')
            return {}


def generate_daily_report(current_run_id: int) -> Dict[str, Any]:
    new_promos     = get_new_promotions_for_run(current_run_id, include_bau=False)
    expired_promos = get_expired_promotions()
    all_active     = get_active_promotions(include_bau=False)

    new_ids = {p['id'] for p in new_promos}
    ongoing = [p for p in all_active if p['id'] not in new_ids]

    by_bank: Dict[str, int] = {}
    for p in all_active:
        bid = p.get('bank_id', 'unknown')
        by_bank[bid] = by_bank.get(bid, 0) + 1

    return {
        'new':     new_promos,
        'active':  ongoing,
        'expired': expired_promos,
        'summary': {
            'total_active':  len(all_active),
            'new_count':     len(new_promos),
            'expired_count': len(expired_promos),
            'by_bank':       by_bank,
        },
    }


# ── 7. Merge duplicates (bulk cleanup utility) ────────────────────────────────

def merge_duplicate_promotions(dry_run: bool = True) -> int:
    with _db_connection() as conn:
        try:
            rows = conn.execute(
                "SELECT id, bank_id, title, highlight "
                "FROM promotions WHERE active = 1 ORDER BY id ASC"
            ).fetchall()

            by_bank: Dict[str, List[Dict]] = {}
            for row in rows:
                by_bank.setdefault(row['bank_id'], []).append(dict(row))

            discard_ids: set = set()

            for bank_id, promos in by_bank.items():
                for i, pa in enumerate(promos):
                    if pa['id'] in discard_ids:
                        continue
                    norm_a      = _normalize_title(pa['title'])
                    hi_a        = (pa['highlight'] or '').strip()[:150]
                    code_stem_a = _extract_promo_code_stem(pa['title'])
                    toks_a      = _tokenize_for_jaccard(pa['title'])

                    for pb in promos[i + 1:]:
                        if pb['id'] in discard_ids:
                            continue
                        norm_b      = _normalize_title(pb['title'])
                        hi_b        = (pb['highlight'] or '').strip()[:150]
                        code_stem_b = _extract_promo_code_stem(pb['title'])
                        toks_b      = _tokenize_for_jaccard(pb['title'])

                        is_dup, reason = False, ''

                        if code_stem_a and code_stem_b and code_stem_a == code_stem_b:
                            is_dup, reason = True, f'promo-code={code_stem_a}'
                        elif norm_a and norm_b:
                            if norm_a == norm_b:
                                is_dup, reason = True, 'exact'
                            else:
                                min_len = min(len(norm_a), len(norm_b))
                                max_len = max(len(norm_a), len(norm_b))
                                if (
                                    min_len >= _MIN_NORM_LEN
                                    and min_len >= max_len * 0.35
                                    and (norm_a in norm_b or norm_b in norm_a)
                                ):
                                    is_dup, reason = True, 'substring'

                            if not is_dup:
                                shared = toks_a & toks_b
                                if (
                                    len(toks_a) >= _MIN_TOKENS
                                    and len(toks_b) >= _MIN_TOKENS
                                    and len(shared) >= _MIN_TOKENS
                                ):
                                    j = len(shared) / len(toks_a | toks_b)
                                    if j >= _JACCARD_THRESHOLD:
                                        is_dup, reason = True, f'Jaccard={j:.2f}'

                            if not is_dup and len(norm_a) >= _MIN_NORM_LEN and len(norm_b) >= _MIN_NORM_LEN:
                                lcp = _common_prefix_ratio(norm_a, norm_b)
                                if lcp >= _LCP_THRESHOLD:
                                    is_dup, reason = True, f'LCP={lcp:.2f}'

                        if not is_dup and hi_a and hi_b and hi_a == hi_b:
                            is_dup, reason = True, 'same-highlight'

                        if is_dup:
                            keep    = pa if len(pa['title']) >= len(pb['title']) else pb
                            discard = pb if keep['id'] == pa['id'] else pa
                            discard_ids.add(discard['id'])
                            tag = '[DRY RUN] ' if dry_run else ''
                            print(
                                f"    🔀 {tag}[{reason}]\n"
                                f"       KEEP    #{keep['id']:>5}  '{keep['title'][:70]}'\n"
                                f"       DISCARD #{discard['id']:>5}  '{discard['title'][:70]}'"
                            )

            if not dry_run and discard_ids:
                conn.execute(
                    f"UPDATE promotions SET active = 0 "
                    f"WHERE id IN ({','.join('?' * len(discard_ids))})",
                    list(discard_ids),
                )
                conn.commit()

            merged = len(discard_ids)
            tag    = '[DRY RUN] ' if dry_run else ''
            print(f"  {tag}merge_duplicate_promotions: {merged} removed")
            return merged

        except Exception as exc:
            conn.rollback()
            print(f'  ❌ merge_duplicate_promotions error: {exc}')
            return 0


# ── 8. Maintenance ────────────────────────────────────────────────────────────

def vacuum_db() -> None:
    """
    Reclaim disk space after large deletions (e.g. after mark_inactive_old).
    SQLite does not reclaim pages automatically; calling VACUUM rewrites the
    entire database file.  Run this periodically (e.g. weekly) rather than
    after every run — it is slow on large databases.
    """
    with _db_connection() as conn:
        try:
            conn.execute('VACUUM')
            print('  🧹 VACUUM completed')
        except Exception as exc:
            print(f'  ❌ vacuum_db error: {exc}')


# ── 9. Load & Export ──────────────────────────────────────────────────────────

def load_promotions(active_only: bool = True) -> List[Dict[str, Any]]:
    with _db_connection() as conn:
        try:
            where = 'WHERE active = 1' if active_only else ''
            return [dict(r) for r in conn.execute(
                f'SELECT * FROM promotions {where} ORDER BY bank_id ASC, last_seen DESC'
            ).fetchall()]
        except Exception as exc:
            print(f'  ❌ load_promotions error: {exc}')
            return []


def export_to_json(output_path: str):
    all_promos = load_promotions(active_only=False)
    records    = []
    for p in all_promos:
        raw_type   = p.get('promo_type') or p.get('category') or ''
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
            'start_date':  p.get('start_date'),
            'end_date':    p.get('end_date'),
            'period':      p.get('period')      or 'Ongoing',
            'quota':       p.get('quota')       or '',
            'cost':        p.get('cost')        or '',
            'types':       types_list,
            'url':         p.get('url')         or '',
            'tc_link':     p.get('tc_link')     or p.get('url') or '',
            'is_bau':      bool(p.get('is_bau', 0)),
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
    bau_n     = sum(1 for r in records if     r['is_bau'])
    print(f'  📄 data.json → {active_n} active ({bau_n} BAU), {expired_n} expired → {output_path}')