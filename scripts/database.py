# scripts/database.py
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta
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

            CREATE INDEX IF NOT EXISTS idx_bank_id    ON promotions(bank_id);
            CREATE INDEX IF NOT EXISTS idx_active     ON promotions(active);
            CREATE INDEX IF NOT EXISTS idx_last_seen  ON promotions(last_seen);
            CREATE INDEX IF NOT EXISTS idx_created_at ON promotions(created_at);
            CREATE INDEX IF NOT EXISTS idx_first_run  ON promotions(first_run_id);
            CREATE INDEX IF NOT EXISTS idx_is_bau     ON promotions(is_bau);
        ''')

        existing_cols = {
            row[1] for row in conn.execute('PRAGMA table_info(promotions)').fetchall()
        }
        migrations = [
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

        conn.commit()
        print('  ✅ Database ready')
    except Exception as e:
        print(f'  ❌ init_db error: {e}')
        raise
    finally:
        conn.close()


# ── 2. Run tracking ───────────────────────────────────────────────────────────

def start_new_run(banks: List[str] = None) -> int:
    conn = _get_conn()
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
    except Exception as e:
        print(f'  ❌ start_new_run error: {e}')
        return 0
    finally:
        conn.close()


def get_previous_run_id(current_run_id: int) -> Optional[int]:
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT id FROM scrape_runs WHERE id < ? ORDER BY id DESC LIMIT 1",
            (current_run_id,),
        ).fetchone()
        return row['id'] if row else None
    except Exception as e:
        print(f'  ❌ get_previous_run_id error: {e}')
        return None
    finally:
        conn.close()


# ── 3. Dedup helpers ──────────────────────────────────────────────────────────

# ── CHANGE 1: module-level threshold constants (used by both _find_duplicate_id
#              and merge_duplicate_promotions so they are always in sync) ───────
_JACCARD_THRESHOLD = 0.50   # raised from 0.25 — eliminates most false positives
_LCP_THRESHOLD     = 0.72   # raised from 0.58
_MIN_NORM_LEN      = 10     # minimum chars in a normalised string to attempt substring
_MIN_TOKENS        = 2      # minimum token-set size to attempt Jaccard

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

    # ── Deposit Plus / 余額+ / Balance+ ────────────────────────────────────────
    (re.compile(r'余[額额]\+'), 'depositplus'),
    (re.compile(r'\bdeposit[- ]?plus\b', re.IGNORECASE), 'depositplus'),
    (re.compile(r'balance\+', re.IGNORECASE), 'depositplus'),
    (re.compile(
        r'\b(?:daily|high|boosted?|tiered?)[- ]?interest[- ]?(?:saving\w*|earn\w*|account\w*)?\b',
        re.IGNORECASE,
    ), 'hisavings'),

    # ── Stock brokerage / commission-free ─────────────────────────────────────
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

    # ── SWIFT / Payment Connect ────────────────────────────────────────────────
    (re.compile(r'\bswift\b', re.IGNORECASE), 'swifttransfer'),
    (re.compile(r'\bpayment[- ]?connect\b', re.IGNORECASE), 'swifttransfer'),

    # ── Fund subscription / zero-fee funds ────────────────────────────────────
    (re.compile(
        r'(?:'
        r'zero[- ]?(?:subscription[- ]?)?fees?[- ]?(?:on[- ]?(?:all[- ]?)?)?funds?'
        r'|featured?[- ]?funds?[- ]?with[- ]?zero[- ]?(?:subscription[- ]?)?fees?'
        r'|zero[- ]?fees?[- ]?fund[- ]?(?:subscription|switching)?'
        r'|zero[- ]?(?:fee|cost)[- ]?(?:investment[- ]?)?fund'
        r')',
        re.IGNORECASE,
    ), 'zerosubfee'),

    # ── Crypto / digital / virtual assets ─────────────────────────────────────
    (re.compile(r'\bcrypto(?:currency|currencies|currenc\w*)?\b', re.IGNORECASE), 'crypto'),
    (re.compile(r'\bdigital[- ]?assets?\b', re.IGNORECASE), 'crypto'),
    (re.compile(r'\bvirtual[- ]?assets?\b', re.IGNORECASE), 'crypto'),

    # ── FX / foreign exchange / global wallet ─────────────────────────────────
    (re.compile(r'\b(?:fx|foreign[- ]?exchange|currency[- ]?exchange|forex)\b', re.IGNORECASE), 'fxexchange'),
    (re.compile(r'\bglobal[- ]?(?:remittance|wallet)\b', re.IGNORECASE), 'fxexchange'),
    (re.compile(r'\bwelab[- ]?global[- ]?wallet\b', re.IGNORECASE), 'welabwallet'),

    # ── Time deposit ──────────────────────────────────────────────────────────
    (re.compile(r'\btime[- ]?deposit\b', re.IGNORECASE), 'timedeposit'),

    # ── PowerDraw (ZA Bank) ────────────────────────────────────────────────────
    (re.compile(r'\bpowerdraw\b', re.IGNORECASE), 'powerdraw'),

    # ── Insurance with interest rate or rebate ────────────────────────────────
    (re.compile(
        r'insurance[- ]?(?:product\w*)?[- ]?(?:with[- ]?)?(?:annual[- ]?rate|premium[- ]?rebate|\d+(?:\.\d+)?%)',
        re.IGNORECASE,
    ), 'insurance_rate'),
    (re.compile(
        r'\d+(?:\.\d+)?%[- ]?(?:annuali[sz]ed|annual)[- ]?rate',
        re.IGNORECASE,
    ), 'insurance_rate'),

    # ── Payroll switch/deposit (Mox) ──────────────────────────────────────────
    (re.compile(r'\bpayroll[- ]?(?:switch(?:ing)?|deposit|benefit\w*)?\b', re.IGNORECASE), 'payroll'),

    # ── Account opening ───────────────────────────────────────────────────────
    (re.compile(
        r'\b(?:(?:quick|fast|instant|mobile|online|digital)[- ]?)?account[- ]?open(?:ing)?\b',
        re.IGNORECASE,
    ), 'accountopen'),

    # ── 24/7 banking ─────────────────────────────────────────────────────────
    (re.compile(r'\b24[/×x]7\b'), 'banking247'),

    # ── Asia Miles / miles reward ─────────────────────────────────────────────
    (re.compile(r'\basia[- ]?miles?\b', re.IGNORECASE), 'asiamiles'),
    (re.compile(r'\bmiles?[- ]?(?:reward|earn|redeem)\w*\b', re.IGNORECASE), 'milesreward'),

    # ── GoSave (WeLab specific) ───────────────────────────────────────────────
    (re.compile(r'\bgosave\b', re.IGNORECASE), 'gosave'),

    # ── liviSave (Livi specific) ──────────────────────────────────────────────
    (re.compile(r'\blivisave\b', re.IGNORECASE), 'livisave'),

    # ── Trip.com (Mox specific) ────────────────────────────────────────────────
    (re.compile(r'\btrip\.com\b', re.IGNORECASE), 'tripcom'),

    # ── Xiaomi (Mox specific) ─────────────────────────────────────────────────
    (re.compile(r'\bxiaomi\b', re.IGNORECASE), 'xiaomi'),

    # ── Samsung (Mox × The Club) ──────────────────────────────────────────────
    (re.compile(r'\bsamsung[- ]?s\d+\b', re.IGNORECASE), 'samsung_phone'),

    # ── Mox × CSL Best-in-Town ────────────────────────────────────────────────
    (re.compile(r'\bbest[- ]?in[- ]?town\b', re.IGNORECASE), 'bestintown'),
    (re.compile(r'\bdevice[- ]?plans?\b', re.IGNORECASE), 'deviceplan'),

    # ── Integrated investment platform (PAO) ──────────────────────────────────
    (re.compile(r'\bintegrated[- ]?investment\b', re.IGNORECASE), 'intinvest'),

    # ── One-stop trading platform (Ant) ───────────────────────────────────────
    (re.compile(r'\bone[- ]?stop[- ]?(?:trading|investment)[- ]?platform\b', re.IGNORECASE), 'onestoplatform'),

    # ── Ant Bank investment fund platform ─────────────────────────────────────
    (re.compile(r'\bant[- ]?bank[- ]?investment[- ]?fund[- ]?platform\b', re.IGNORECASE), 'antfundplatform'),

    # ── Personal loan / revolving credit ──────────────────────────────────────
    (re.compile(r'\bpersonal[- ]?(?:revolving[- ]?)?loan\b', re.IGNORECASE), 'personalloan'),

    # ── Zero fee (specific compound phrases only) ─────────────────────────────
    # CHANGE 2: removed standalone \bzero\b → 'zfee' and \bfree\b → 'zfee'
    # because they caused completely unrelated promotions to share the 'zfee'
    # token and then falsely match via Jaccard/substring.
    # Kept only compound phrases that are unambiguous.
    (re.compile(
        r'(?:'
        r'zero[- ]?(?:\w+[- ]?)?fees?'
        r'|0\s*%[- ]?(?:\w+[- ]?)?fees?'
        r'|fee[- ]?(?:free|waiver|waived)'
        r'|no[- ]?(?:\w+[- ]?)?fees?'
        r')',
        re.IGNORECASE,
    ), 'zfee'),

    # ── Speed synonyms (generic) ──────────────────────────────────────────────
    (re.compile(
        r'\b(?:quick|fast|instant|rapid|express|immediate)\b', re.IGNORECASE
    ), 'fastspd'),

    # ── Flexible / custom (generic) ───────────────────────────────────────────
    (re.compile(
        r'\b(?:flexible|custom(?:iz\w*)?|select(?:able|ion)?|personali[sz]\w*)\b',
        re.IGNORECASE,
    ), 'flexcust'),

    # ── Welcome / sign-up bonus ───────────────────────────────────────────────
    (re.compile(
        r'\b(?:welcome|sign[- ]?up|new[- ]?customer|new[- ]?user)[- ]?(?:bonus|reward|offer|gift)?\b',
        re.IGNORECASE,
    ), 'welcome'),

    # ── Referral ──────────────────────────────────────────────────────────────
    (re.compile(
        r'\b(?:refer(?:ral)?[- ]?(?:bonus|reward|program)?|invite[- ]?friend\w*|friend[- ]?refer\w*)\b',
        re.IGNORECASE,
    ), 'referral'),

    # ── Cashback ──────────────────────────────────────────────────────────────
    (re.compile(r'\b(?:cash[- ]?back|cash[- ]?rebate)\b', re.IGNORECASE), 'cashback'),
]

# CHANGE 3: noise words are now applied as whole-word boundaries in
# _normalize_title (before non-alnum stripping), so short words like 'with',
# 'for', 'and' cannot accidentally corrupt compound tokens like 'withdrawals'.
_NOISE_WORDS = (
    'rebateprogram', 'rewardprogram', 'program',
    'promotion', 'campaign', 'offer', 'bonus',
    'reward', 'scheme', 'deal', 'activity',
    'rebate', 'exclusive', 'special',
    'with', 'from', 'for', 'and', 'the', 'your',
)

# Pre-compiled noise-word patterns (whole-word, case-insensitive)
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


def _normalize_title(title: str) -> str:
    if not title:
        return ''
    t = _RE_INSTALMENT.sub('instalment', title.lower())
    for pattern, replacement in _SYNONYM_PATTERNS:
        t = pattern.sub(replacement, t)
    t = _RE_AMOUNT.sub('', t)
    t = _RE_PCT.sub('', t)
    # CHANGE 3: remove noise words as whole-word tokens BEFORE stripping
    # non-alnum characters, so 'with' cannot eat into 'withdrawals' etc.
    for pat in _NOISE_PATTERNS:
        t = pat.sub('', t)
    t = _RE_NONALNUM.sub('', t)
    return t


def _stem(tok: str) -> str:
    if len(tok) > 4 and tok.endswith('s') and not tok.endswith('ss'):
        return tok[:-1]
    return tok


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
    """
    Return the id of an existing promo that is the same as the incoming one.

    Check order (highest → lowest confidence):
      0. Promo code stem match  — MOXBILL25 matches MOXBILL26 (same campaign)
      1. Exact normalised title
      2. Substring containment  (both normalised strings ≥ _MIN_NORM_LEN chars,
                                  shorter ≥ 35 % of longer — prevents tiny tokens
                                  from matching unrelated long strings)
      3. Jaccard ≥ _JACCARD_THRESHOLD  (both token sets ≥ _MIN_TOKENS)
      4. Common-prefix ratio ≥ _LCP_THRESHOLD
      5. Identical highlight snippet
    """
    rows = conn.execute(
        "SELECT id, title, highlight FROM promotions WHERE bank_id = ?",
        (bank_id,)
    ).fetchall()

    norm_new      = _normalize_title(title)
    hi_snippet    = (highlight or '').strip()[:150]
    new_code_stem = _extract_promo_code_stem(title)
    # CHANGE 4: precompute tokens once outside the row loop
    toks_new      = _tokenize_for_jaccard(title)

    for row in rows:
        # 0. Promo code stem match
        if new_code_stem:
            old_code_stem = _extract_promo_code_stem(row['title'])
            if old_code_stem and new_code_stem == old_code_stem:
                return row['id']

        norm_old = _normalize_title(row['title'])
        old_snip = (row['highlight'] or '').strip()[:150]

        if norm_new and norm_old:
            # 1. Exact match
            if norm_new == norm_old:
                return row['id']

            # 2. Substring — guard: both strings must be long enough AND the
            #    shorter must cover at least 35 % of the longer to prevent a
            #    tiny compound token (e.g. 'zfee', 8 chars) from matching an
            #    unrelated 40-char normalised string.
            len_new, len_old = len(norm_new), len(norm_old)
            min_len = min(len_new, len_old)
            max_len = max(len_new, len_old)
            if (
                min_len >= _MIN_NORM_LEN
                and min_len >= max_len * 0.35
                and (norm_new in norm_old or norm_old in norm_new)
            ):
                return row['id']

            # 3. Jaccard — guard: both token sets need enough tokens to be
            #    meaningful; a single shared token is never sufficient on its own.
            toks_old = _tokenize_for_jaccard(row['title'])
            if (
                len(toks_new) >= _MIN_TOKENS
                and len(toks_old) >= _MIN_TOKENS
                and len(toks_new & toks_old) >= _MIN_TOKENS  # ← need ≥2 shared tokens
                and _jaccard_similarity(title, row['title']) >= _JACCARD_THRESHOLD
            ):
                return row['id']

            # 4. Common prefix
            if (
                len_new >= _MIN_NORM_LEN
                and len_old >= _MIN_NORM_LEN
                and _common_prefix_ratio(norm_new, norm_old) >= _LCP_THRESHOLD
            ):
                return row['id']

        # 5. Identical highlight snippet
        if hi_snippet and old_snip and hi_snippet == old_snip:
            return row['id']

    return None


# ── 4. Save (upsert) ──────────────────────────────────────────────────────────

def save_promotions(
    bank_id: str,
    bank_name: str,
    promotions: List[Dict],
    current_run_id: int = 0,
) -> Dict:
    conn  = _get_conn()
    today = datetime.now().strftime('%Y-%m-%d')
    stats = {'new': 0, 'updated': 0, 'skipped': 0}

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

            is_bau = int(bool(p.get('is_bau', False)))

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

    except Exception as e:
        conn.rollback()
        print(f'  ❌ save_promotions error: {e}')
        raise
    finally:
        conn.close()


# ── 5. Mark stale / old inactive ─────────────────────────────────────────────

def mark_stale_as_inactive(bank_ids_scraped: List[str], today_str: str = None) -> int:
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


# ── 6. Queries ────────────────────────────────────────────────────────────────

def _to_dicts(rows) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows]


def get_new_promotions_for_run(
    current_run_id: int,
    include_bau: bool = False,
) -> List[Dict[str, Any]]:
    conn = _get_conn()
    try:
        bau_clause = '' if include_bau else 'AND is_bau = 0'
        return _to_dicts(conn.execute(f'''
            SELECT * FROM promotions
            WHERE first_run_id = ?
              AND active = 1
              {bau_clause}
            ORDER BY bank_id ASC, id ASC
        ''', (current_run_id,)).fetchall())
    except Exception as e:
        print(f'  ❌ get_new_promotions_for_run error: {e}')
        return []
    finally:
        conn.close()


def get_active_promotions(include_bau: bool = True) -> List[Dict[str, Any]]:
    conn = _get_conn()
    try:
        bau_clause = '' if include_bau else 'AND is_bau = 0'
        return _to_dicts(conn.execute(f'''
            SELECT * FROM promotions
            WHERE active = 1 {bau_clause}
            ORDER BY bank_id ASC, last_seen DESC
        ''').fetchall())
    except Exception as e:
        print(f'  ❌ get_active_promotions error: {e}')
        return []
    finally:
        conn.close()


def get_expired_promotions() -> List[Dict[str, Any]]:
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    conn = _get_conn()
    try:
        return _to_dicts(conn.execute('''
            SELECT * FROM promotions
            WHERE active = 0
              AND is_bau  = 0
              AND DATE(last_seen) >= ?
            ORDER BY bank_id ASC, last_seen DESC
        ''', (yesterday,)).fetchall())
    except Exception as e:
        print(f'  ❌ get_expired_promotions error: {e}')
        return []
    finally:
        conn.close()


def get_active_promos_for_bank(bank_id: str) -> List[Dict[str, Any]]:
    conn = _get_conn()
    try:
        return _to_dicts(conn.execute(
            'SELECT id, title FROM promotions WHERE bank_id = ? AND active = 1',
            (bank_id,)
        ).fetchall())
    except Exception as e:
        print(f'  ❌ get_active_promos_for_bank error: {e}')
        return []
    finally:
        conn.close()


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


# ── 7. Merge duplicates (bulk cleanup) ───────────────────────────────────────

def merge_duplicate_promotions(dry_run: bool = True) -> int:
    """
    Bulk dedup pass over all active rows.
    Uses the same module-level thresholds and guards as _find_duplicate_id
    so dry-run results match what the live scraper would do.
    """
    conn = _get_conn()
    merged = 0
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
                toks_a      = _tokenize_for_jaccard(pa['title'])  # precompute

                for pb in promos[i + 1:]:
                    if pb['id'] in discard_ids:
                        continue
                    norm_b      = _normalize_title(pb['title'])
                    hi_b        = (pb['highlight'] or '').strip()[:150]
                    code_stem_b = _extract_promo_code_stem(pb['title'])
                    toks_b      = _tokenize_for_jaccard(pb['title'])  # precompute

                    is_dup, reason = False, ''

                    # 0. Promo code stem
                    if code_stem_a and code_stem_b and code_stem_a == code_stem_b:
                        is_dup, reason = True, f'promo-code={code_stem_a}'

                    elif norm_a and norm_b:
                        # 1. Exact
                        if norm_a == norm_b:
                            is_dup, reason = True, 'exact'

                        # 2. Substring — same guards as _find_duplicate_id
                        else:
                            min_len = min(len(norm_a), len(norm_b))
                            max_len = max(len(norm_a), len(norm_b))
                            if (
                                min_len >= _MIN_NORM_LEN
                                and min_len >= max_len * 0.35
                                and (norm_a in norm_b or norm_b in norm_a)
                            ):
                                is_dup, reason = True, 'substring'

                        # 3. Jaccard — require ≥ _MIN_TOKENS shared tokens
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

                        # 4. Common prefix
                        if not is_dup:
                            if (
                                len(norm_a) >= _MIN_NORM_LEN
                                and len(norm_b) >= _MIN_NORM_LEN
                            ):
                                lcp = _common_prefix_ratio(norm_a, norm_b)
                                if lcp >= _LCP_THRESHOLD:
                                    is_dup, reason = True, f'LCP={lcp:.2f}'

                    # 5. Identical highlight snippet
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
        tag = '[DRY RUN] ' if dry_run else ''
        print(f"  {tag}merge_duplicate_promotions: {merged} removed")
        return merged

    except Exception as e:
        conn.rollback()
        print(f'  ❌ merge_duplicate_promotions error: {e}')
        return 0
    finally:
        conn.close()


# ── 8. Load & Export ──────────────────────────────────────────────────────────

def load_promotions(active_only: bool = True) -> List[Dict[str, Any]]:
    conn = _get_conn()
    try:
        where = 'WHERE active = 1' if active_only else ''
        return [dict(r) for r in conn.execute(
            f'SELECT * FROM promotions {where} ORDER BY bank_id ASC, last_seen DESC'
        ).fetchall()]
    except Exception as e:
        print(f'  ❌ load_promotions error: {e}')
        return []
    finally:
        conn.close()


def export_to_json(output_path: str):
    all_promos = load_promotions(active_only=False)
    records = []
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