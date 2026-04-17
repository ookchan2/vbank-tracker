# scripts/ai_helper.py

import json
import os
import re
from collections import Counter
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Any, Callable, Optional

# ── OpenAI client (lazy init) ─────────────────────────────────────────────────

try:
    from openai import OpenAI as _OpenAIClass
    _openai_client: Optional[_OpenAIClass] = None

    def _get_client() -> _OpenAIClass:
        global _openai_client
        if _openai_client is None:
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise EnvironmentError('OPENAI_API_KEY environment variable is not set.')
            _openai_client = _OpenAIClass(api_key=api_key)
        return _openai_client

    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False
    def _get_client():
        raise ImportError('openai package is not installed. Run: pip install openai')

# ── Constants ─────────────────────────────────────────────────────────────────

_DUPLICATE_TITLE_THRESHOLD = 0.72   # lowered to catch case/punctuation variants
_DUPLICATE_DESC_THRESHOLD  = 0.88
_MERGE_TITLE_THRESHOLD     = 0.80   # stricter: only merge when titles nearly identical

VALID_TYPES = [
    '迎新 Welcome', '消費 Spending', '投資 Investment', '旅遊 Travel',
    '保險 Insurance', '貸款 Loan', '活期存款 Savings', '定期存款 TDeposit',
    '外匯 FX', '推薦 Referral', '新資金 New Funds', 'Others 其他',
]

INSIGHT_CATEGORIES = [
    'Investment (Stock/Crypto Trading)',
    'Fund Investment',
    'Spending/CashBack',
    'Welcome Bonus',
    'Travel',
    'Loan APR',
    'FX/Multi-Currency',
    'Referral Bonus',
]

EIGHT_BANKS = [
    'ZA Bank', 'Mox Bank', 'WeLab Bank', 'livi bank',
    'PAObank', 'Airstar Bank', 'Fusion Bank', 'Ant Bank',
]

# ── Static BAU promotions ─────────────────────────────────────────────────────

STATIC_BAU_PROMOTIONS: list[dict] = [
    {
        'bank_name':   'Ant Bank',
        'title':       'SME Loan Cash Rebate Promotion',
        'highlight':   'SME customers enjoy cash rebates on eligible business loan products.',
        'description': (
            'Ant Bank SME customers who successfully apply for and drawdown eligible '
            'business loan products can receive a cash rebate. The rebate amount varies '
            'by loan amount and is credited after drawdown completion.'
        ),
        'types':   ['貸款 Loan'],
        'is_bau':  True,
        'active':  True,
        'period':  'Ongoing',
        'quota':   'SME customers only',
        'tc_link': 'https://www.antbank.hk/',
    },
    {
        'bank_name':   'Ant Bank',
        'title':       '100% Insurance Premium Rebate',
        'highlight':   "Enjoy 100% rebate on the first month's premium for eligible insurance products.",
        'description': (
            "Eligible Ant Bank customers receive a 100% rebate on the first month's "
            'insurance premium when purchasing designated insurance products through the '
            'Ant Bank app. This is a permanent BAU benefit available to qualifying customers.'
        ),
        'types':   ['保險 Insurance'],
        'is_bau':  True,
        'active':  True,
        'period':  'Ongoing',
        'quota':   'New insurance policy applicants via Ant Bank app',
        'tc_link': 'https://www.antbank.hk/',
    },
    {
        'bank_name':   'Ant Bank',
        'title':       'Insurance Products with up to 3.6% Annualized Rate',
        'highlight':   'Ant Bank insurance savings products offer up to 3.6% p.a. annualized return.',
        'description': (
            'Ant Bank offers insurance-linked savings products combining life insurance '
            'coverage with competitive annualized returns of up to 3.6%. These products '
            'are suited for customers seeking both protection and long-term wealth accumulation.'
        ),
        'types':   ['保險 Insurance'],
        'is_bau':  True,
        'active':  True,
        'period':  'Ongoing',
        'tc_link': 'https://www.antbank.hk/',
    },
]

# ── Generic listing-URL detection ─────────────────────────────────────────────

# URL path endings that indicate a promotions hub/listing page, NOT a specific campaign page
_GENERIC_URL_ENDINGS: frozenset[str] = frozenset({
    'promotion', 'promotions', 'offer', 'offers',
    'campaign', 'campaigns', 'deal', 'deals',
    'promo', 'promos', 'event', 'events',
    'special', 'specials', 'reward', 'rewards',
    'benefit', 'benefits', 'news', 'latest',
    'whats-new', 'whatsnew', 'products', 'product',
    'services', 'service', 'feature', 'features',
    # language-only single-segment paths
    'en', 'zh', 'tc', 'sc', 'hk', 'cn', 'zh-hk', 'zh-tw',
})


def _is_generic_listing_url(url: str) -> bool:
    """
    Returns True when the URL is a promotions listing/hub page rather than a
    deep-link to one specific campaign.

    Banks like Mox Bank sometimes return the same generic URL for every
    promotion because their site uses a single promotions directory page.
    We must NOT merge those entries — they are genuinely different promotions.
    """
    if not url:
        return True

    clean = url.lower().rstrip('/')

    # Strip protocol + domain to get the path
    path = re.sub(r'^[a-z]+://[^/]+', '', clean).strip('/')

    # Root URL (empty path after stripping)
    if not path:
        return True

    # Last path segment (strip query string / fragment)
    last_seg = path.rsplit('/', 1)[-1]
    last_seg = re.split(r'[?#]', last_seg)[0]

    if last_seg in _GENERIC_URL_ENDINGS:
        return True

    # Path has only 1–2 meaningful segments after stripping language prefixes
    meaningful = [
        seg for seg in path.split('/')
        if seg and seg not in ('en', 'zh', 'tc', 'sc', 'hk', 'cn', 'zh-hk', 'zh-tw')
    ]
    if len(meaningful) == 1 and meaningful[0] in _GENERIC_URL_ENDINGS:
        return True

    return False

# ── String helpers ────────────────────────────────────────────────────────────

def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _normalize_for_exact(s: str) -> str:
    """
    Aggressive normalisation for the first dedup pass.
    Catches case variants ("One-Stop" vs "One-stop") and punctuation variants.
    """
    if not s:
        return ''
    s = s.lower().strip()
    s = re.sub(r'[\s\-–—_/\\]+', ' ', s)   # unify separators
    s = re.sub(r'[^\w\s]', '', s)            # remove remaining punctuation
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _normalize_url(url: str) -> str:
    if not url:
        return ''
    url = url.lower().strip().rstrip('/')
    url = re.sub(r'^https?://', '', url)
    return url


def _score_completeness(p: dict) -> int:
    return (
        len(p.get('description') or '') +
        len(p.get('highlight')   or '') +
        len(p.get('title')       or '') * 2 +
        len(p.get('quota')       or '') +
        len(p.get('cost')        or '') +
        (20 if p.get('end_date')   else 0) +
        (20 if p.get('start_date') else 0) +
        (10 if p.get('tc_link')    else 0)
    )

# ── Deduplication helpers ─────────────────────────────────────────────────────

def _are_duplicate(p1: dict, p2: dict) -> bool:
    b1 = (p1.get('bank_name') or p1.get('bank') or '').lower().strip()
    b2 = (p2.get('bank_name') or p2.get('bank') or '').lower().strip()
    if not b1 or not b2 or b1 != b2:
        return False

    # 1. URL match — strongest signal, but only for specific (non-listing) URLs
    u1 = _normalize_url(p1.get('tc_link') or p1.get('url') or '')
    u2 = _normalize_url(p2.get('tc_link') or p2.get('url') or '')
    if u1 and u2 and u1 == u2 and not _is_generic_listing_url(u1):
        return True

    t1 = (p1.get('title') or p1.get('name') or '').strip()
    t2 = (p2.get('title') or p2.get('name') or '').strip()

    # 2. Exact match after aggressive normalisation
    if t1 and t2 and _normalize_for_exact(t1) == _normalize_for_exact(t2):
        return True

    # 3. High similarity ratio
    if t1 and t2 and _similarity(t1, t2) >= _DUPLICATE_TITLE_THRESHOLD:
        return True

    return False


def _merge_group(group: list[dict]) -> dict:
    best      = max(group, key=_score_completeness)
    all_types: list[str] = []
    seen_t:    set[str]  = set()
    for p in group:
        for t in (p.get('types') or []):
            if t not in seen_t:
                seen_t.add(t)
                all_types.append(t)
    best['types'] = all_types or ['Others 其他']
    return best


def _merge_same_url_promos(promotions: list[dict]) -> list[dict]:
    """
    Consolidate entries that the AI incorrectly split from a single campaign page.

    Decision tree for each group of promotions sharing (bank, url):
    ┌─ count == 1  → pass through unchanged
    ├─ count >= 3  → listing/hub page; keep ALL separate (e.g. Mox Bank)
    ├─ count == 2  AND  url is a generic listing page  → keep both separate
    ├─ count == 2  AND  titles are very similar (≥0.80) → merge into one
    └─ count == 2  AND  titles clearly differ           → keep both separate

    The key insight: banks like Mox Bank use a single promotions URL for every
    campaign.  Blindly merging by URL would collapse their entire catalogue.
    We use URL-pattern detection AND the within-bank count as dual guards.
    """
    if not promotions:
        return []

    # Group by (bank_normalised, url_normalised)
    groups: dict[tuple[str, str], list[int]] = {}
    no_url_idxs: list[int] = []

    for i, p in enumerate(promotions):
        bank = (p.get('bank_name') or p.get('bank') or '').lower().strip()
        url  = _normalize_url(p.get('tc_link') or p.get('url') or '')
        if url and bank:
            groups.setdefault((bank, url), []).append(i)
        else:
            no_url_idxs.append(i)

    result:      list[dict] = []
    merge_count: int        = 0

    for (bank, url), idxs in groups.items():
        count = len(idxs)

        # ── Case 1: single entry ──────────────────────────────────
        if count == 1:
            result.append(promotions[idxs[0]])
            continue

        # ── Case 2: three or more share the same URL ──────────────
        # Almost certainly a listing/hub page (e.g. Mox Bank's /promotions).
        if count >= 3:
            for i in idxs:
                result.append(promotions[i])
            print(
                f'    ⚠️  URL-merge skipped: {count} promos share '
                f'listing URL [{url[:55]}] — all kept separate'
            )
            continue

        # ── Case 3: exactly 2 share the same URL ─────────────────
        p1 = promotions[idxs[0]]
        p2 = promotions[idxs[1]]
        t1 = (p1.get('title') or '').strip()
        t2 = (p2.get('title') or '').strip()

        # Guard A: URL is a known listing-page pattern
        if _is_generic_listing_url(url):
            result.append(p1)
            result.append(p2)
            # Logged only when titles are suspiciously close (worth auditing)
            if t1 and t2 and _similarity(t1, t2) >= 0.65:
                print(
                    f'    ⚠️  URL-merge skipped (listing URL, similar titles): '
                    f'[{url[:50]}]\n'
                    f'       "{t1[:55]}"\n'
                    f'       "{t2[:55]}"'
                )
            continue

        # Guard B: titles are nearly identical → AI split one campaign into two
        if t1 and t2 and _similarity(t1, t2) >= _MERGE_TITLE_THRESHOLD:
            group = [p1, p2]
            best  = max(group, key=_score_completeness)

            # Combine descriptions
            desc_parts: list[str] = []
            for p in group:
                d = (p.get('description') or p.get('highlight') or '').strip()
                if d and d not in desc_parts:
                    desc_parts.append(d)

            # Merge types
            all_types: list[str] = []
            seen_t:    set[str]  = set()
            for p in group:
                for t in (p.get('types') or []):
                    if t not in seen_t:
                        seen_t.add(t)
                        all_types.append(t)

            best['title']       = max([t1, t2], key=len)
            best['description'] = ' | '.join(desc_parts) if len(desc_parts) > 1 else (desc_parts[0] if desc_parts else '')
            best['types']       = all_types or ['Others 其他']

            result.append(best)
            merge_count += 1
            print(
                f'    🔀 URL-merged 2 near-identical → 1 '
                f'[{url[:50]}] "{best["title"][:45]}"'
            )
            continue

        # Guard C: titles differ → genuinely distinct promotions sharing a URL
        # (e.g. two campaigns on the same deep page)
        result.append(p1)
        result.append(p2)

    for i in no_url_idxs:
        result.append(promotions[i])

    if merge_count:
        print(f'    📦 _merge_same_url_promos: {merge_count} pair(s) consolidated')

    return result


def deduplicate_promotions(promotions: list[dict]) -> list[dict]:
    if not promotions:
        return []

    # Step 1: smart same-URL merge
    pass1 = _merge_same_url_promos(promotions)

    # Step 2: title similarity dedup
    final: list[dict] = []
    for p in pass1:
        found_dup = False
        for i, existing in enumerate(final):
            if _are_duplicate(p, existing):
                found_dup = True
                if _score_completeness(p) > _score_completeness(existing):
                    final[i] = p
                print(
                    f'    ♻  Title-dedup removed: '
                    f'"{(p.get("title") or "")[:60]}" [{p.get("bank_name", "?")}]'
                )
                break
        if not found_dup:
            final.append(p)

    return final

# ── Date helpers ──────────────────────────────────────────────────────────────

def is_new_today(promo: dict, today: str) -> bool:
    created = (promo.get('created_at') or '')[:10]
    if created != today:
        return False
    start = (promo.get('start_date') or '')[:10]
    if start and start < today:
        return False
    return True


def is_new_this_week(promo: dict, today: str) -> bool:
    created = (promo.get('created_at') or '')[:10]
    if not created:
        return False
    try:
        today_dt = datetime.strptime(today, '%Y-%m-%d')
    except ValueError:
        return False

    six_days_ago = (today_dt - timedelta(days=6)).strftime('%Y-%m-%d')
    yesterday    = (today_dt - timedelta(days=1)).strftime('%Y-%m-%d')
    if not (six_days_ago <= created <= yesterday):
        return False

    start = (promo.get('start_date') or '')[:10]
    if start and start < six_days_ago:
        return False
    return True


def revalidate_expired(promotions: list[dict], today: str) -> list[dict]:
    active_set: set[str] = set()
    for p in promotions:
        if p.get('active'):
            url = _normalize_url(p.get('tc_link') or p.get('url') or '')
            if url:
                active_set.add(url)

    corrected: list[dict] = []
    for p in promotions:
        if p.get('active'):
            corrected.append(p)
            continue

        end = (p.get('end_date') or '')[:10]

        if end and end >= today:
            p_fixed           = dict(p)
            p_fixed['active'] = True
            print(
                f'    🔄 Re-activated (valid end_date {end}): '
                f'"{(p.get("title") or "")[:60]}" [{p.get("bank_name", "?")}]'
            )
            corrected.append(p_fixed)
            continue

        if not end:
            url = _normalize_url(p.get('tc_link') or p.get('url') or '')
            if url and url in active_set:
                print(
                    f'    🗑  Expired+duplicate removed: '
                    f'"{(p.get("title") or "")[:60]}" [{p.get("bank_name", "?")}]'
                )
                continue
            corrected.append(p)
            continue

        corrected.append(p)

    return corrected

# ── Reconcile ─────────────────────────────────────────────────────────────────

def reconcile_with_existing(
    newly_extracted: list[dict],
    existing_promos: list[dict],
    today:           str,
) -> tuple[list[dict], list[str]]:
    logs:         list[str] = []
    reconciled:   list[dict] = []
    matched_idxs: set[int]   = set()

    for new_p in newly_extracted:
        match_idx: Optional[int] = None
        for idx, ex_p in enumerate(existing_promos):
            if _are_duplicate(new_p, ex_p):
                match_idx = idx
                break

        if match_idx is not None:
            matched_idxs.add(match_idx)
            ex_p    = existing_promos[match_idx]
            updated = dict(ex_p)
            updated['last_seen'] = today
            updated['active']    = True
            for fld in ('description', 'highlight', 'period', 'end_date',
                        'start_date', 'quota', 'cost', 'types'):
                if new_p.get(fld) and new_p.get(fld) != ex_p.get(fld):
                    updated[fld] = new_p[fld]
            reconciled.append(updated)
        else:
            new_p.setdefault('created_at', today)
            new_p['last_seen'] = today
            new_p.setdefault('active', True)
            reconciled.append(new_p)
            logs.append(f'NEW: [{new_p.get("bank_name")}] {new_p.get("title", "?")}')

    for idx, ex_p in enumerate(existing_promos):
        if idx in matched_idxs:
            continue
        end = (ex_p.get('end_date') or '')[:10]
        if end and end < today:
            expired_p           = dict(ex_p)
            expired_p['active'] = False
            reconciled.append(expired_p)
            logs.append(f'EXPIRED: [{ex_p.get("bank_name")}] {ex_p.get("title", "?")}')
        else:
            kept_p              = dict(ex_p)
            kept_p['last_seen'] = today
            reconciled.append(kept_p)

    return reconciled, logs

# ── Prompt builder ────────────────────────────────────────────────────────────

def _build_extraction_prompt(
    bank_name:         str,
    scraped_text:      str,
    existing_for_bank: list[dict],
    today:             str,
) -> str:
    if not existing_for_bank:
        db_lines = '  (none — this is a fresh bank)'
    else:
        db_lines = '\n'.join(
            '  * [URL: {}]  "{}"'.format(
                p.get('tc_link') or p.get('url') or 'no-url',
                p.get('title') or '?',
            )
            for p in existing_for_bank[:30]
        )

    valid_types_str = ', '.join(VALID_TYPES)

    return (
        f'You are extracting structured bank promotion data for {bank_name}.\n\n'
        f'TODAY: {today}\n\n'
        f'PROMOTIONS ALREADY IN THE DATABASE FOR {bank_name}\n'
        '(You MUST NOT re-add any of these — check carefully before adding anything):\n'
        f'{db_lines}\n\n'
        'SCRAPED WEBSITE TEXT (multiple pages, each prefixed with === SOURCE: [URL] ===):\n'
        f'{scraped_text[:32000]}\n\n'
        '══════════════════════════════════════════════════\n'
        'EXTRACTION RULES — READ ALL BEFORE WRITING OUTPUT\n'
        '══════════════════════════════════════════════════\n\n'
        'RULE 1 — EXACT TITLE:\n'
        '* Copy the promotion title VERBATIM from the source page text.\n'
        '* Do NOT rename, simplify, embellish, or invent a title.\n\n'
        'RULE 2 — ONE PROMOTION PER CAMPAIGN PAGE (CRITICAL):\n'
        '* If one campaign page describes multiple benefits or steps,\n'
        '  create EXACTLY ONE promotion entry for that page.\n'
        '* Consolidate ALL benefits from that page into the single "description" field.\n'
        '* Do NOT split a single campaign page into multiple entries.\n'
        '* If the same URL already appears twice in your output, remove the duplicate.\n'
        '* ONE URL → ONE JSON object. No exceptions.\n\n'
        'RULE 3 — STRICT DEDUPLICATION (case-insensitive):\n'
        '* Before adding ANY promotion, check the "ALREADY IN DATABASE" list above.\n'
        '* SAME URL already in the list → SKIP.\n'
        '* VERY SIMILAR TITLE for the same bank (ignoring capitalisation, e.g.\n'
        '  "One-Stop" vs "One-stop") → SKIP.\n'
        '* When uncertain: SKIP rather than risk a duplicate.\n\n'
        f'RULE 4 — START DATE GATE:\n'
        f'* start_date determined and BEFORE {today} → "is_new_today": false.\n'
        f'* start_date >= {today} OR unknown → "is_new_today": true.\n\n'
        'RULE 5 — ACTIVE / EXPIRED:\n'
        f'* end_date < {today}   → "active": false\n'
        f'* end_date >= {today}  → "active": true\n'
        '* No end_date / Ongoing → "active": true\n\n'
        'RULE 6 — SOURCE LINK:\n'
        '* Use the most specific URL for each promotion.\n'
        '* Source URLs appear as === SOURCE: [URL] === markers in the text above.\n'
        '* If the only available URL is a generic listing page (e.g. /promotions),\n'
        '  still assign it — do NOT invent a more specific URL.\n\n'
        '══════════════════════════════════════════════════\n'
        'OUTPUT FORMAT — return ONLY a valid JSON array:\n'
        '══════════════════════════════════════════════════\n'
        '[\n'
        '  {\n'
        '    "title":        "exact title copied from the source page",\n'
        f'    "bank_name":    "{bank_name}",\n'
        '    "types":        ["迎新 Welcome"],\n'
        '    "highlight":    "one-sentence benefit summary",\n'
        '    "description":  "full description covering ALL benefits on this page",\n'
        '    "period":       "DD MMM YYYY to DD MMM YYYY  OR  Ongoing",\n'
        '    "start_date":   "YYYY-MM-DD  OR  null",\n'
        '    "end_date":     "YYYY-MM-DD  OR  null",\n'
        '    "quota":        "eligibility / who qualifies",\n'
        '    "cost":         "minimum spend or deposit  OR  null",\n'
        '    "tc_link":      "https://most-specific-source-url",\n'
        '    "is_bau":       false,\n'
        '    "active":       true,\n'
        '    "is_new_today": false\n'
        '  }\n'
        ']\n\n'
        f'Valid type values: {valid_types_str}\n\n'
        'Return [] if no promotions found.  Return ONLY the JSON array — no prose.'
    )

# ── Core AI call ──────────────────────────────────────────────────────────────

def _call_ai(prompt: str, model: str = 'gpt-4o', seed: int = 42) -> str:
    client = _get_client()
    resp   = client.chat.completions.create(
        model    = model,
        messages = [
            {
                'role':    'system',
                'content': (
                    'You are a meticulous data-extraction assistant. '
                    'You extract bank promotion data and return clean JSON only. '
                    'You NEVER invent or rename promotion titles — you copy them verbatim. '
                    'You NEVER split one campaign page into multiple entries — '
                    'one URL always maps to exactly one JSON object. '
                    'You NEVER create duplicate entries — you always check the existing '
                    'database list before adding anything. '
                    'When in doubt about whether something is a duplicate, SKIP it. '
                    'You return valid JSON arrays with no additional prose.'
                ),
            },
            {'role': 'user', 'content': prompt},
        ],
        temperature = 0.05,
        max_tokens  = 4096,
        seed        = seed,
    )
    return resp.choices[0].message.content or '[]'


def _parse_ai_json(raw: str, bank_name: str) -> list[dict]:
    raw = raw.strip()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            for key in ('promotions', 'data', 'results', 'items', 'output'):
                if isinstance(parsed.get(key), list):
                    return parsed[key]
            if 'title' in parsed:
                return [parsed]
    except json.JSONDecodeError:
        pass

    m = re.search(r'\[.*\]', raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass

    print(f'    ⚠  Could not parse AI response for {bank_name}')
    return []


def _validate_promotion(p: dict, bank_name: str, today: str) -> Optional[dict]:
    title = (p.get('title') or '').strip()
    if not title or len(title) < 3:
        return None

    p['bank_name'] = bank_name

    raw_t = p.get('types') or []
    if isinstance(raw_t, str):
        raw_t = [raw_t]
    valid_t    = [t for t in raw_t if t in VALID_TYPES]
    p['types'] = valid_t if valid_t else ['Others 其他']

    p['is_bau'] = bool(p.get('is_bau', False))
    p['active'] = bool(p.get('active', True))

    for df in ('start_date', 'end_date'):
        val = p.get(df)
        if val:
            try:
                datetime.strptime(str(val)[:10], '%Y-%m-%d')
                p[df] = str(val)[:10]
            except ValueError:
                p[df] = None
        else:
            p[df] = None

    if p['end_date'] and p['end_date'] < today:
        p['active'] = False

    p.pop('is_new_today', None)
    return p

# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def init_ai() -> bool:
    """
    Initialise the OpenAI client and confirm the API key is present.
    Returns True when AI is ready, False otherwise.
    """
    if not HAS_OPENAI:
        print('  ⚠️  openai package not installed — AI features disabled')
        return False
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print('  ⚠️  OPENAI_API_KEY not set — AI features disabled')
        return False
    try:
        _get_client()
        print('  ✅ OpenAI client initialised')
        return True
    except Exception as exc:
        print(f'  ❌ OpenAI init failed: {exc}')
        return False


def analyze_promotions(
    bank_id:     str,
    bank_name:   str,
    text:        str,
    screenshot:  Optional[bytes] = None,
    default_url: str = '',
) -> list[dict]:
    """
    Extract structured promotions for ONE bank from its scraped page text.
    Returns a validated list ready for ai_dedup_titles + ai_match_against_existing.
    """
    today = datetime.now().strftime('%Y-%m-%d')

    prompt = _build_extraction_prompt(
        bank_name         = bank_name,
        scraped_text      = text,
        existing_for_bank = [],
        today             = today,
    )

    try:
        raw      = _call_ai(prompt)
        raw_list = _parse_ai_json(raw, bank_name)
    except Exception as exc:
        print(f'    ❌ AI extraction error for {bank_name}: {exc}')
        return []

    validated: list[dict] = []
    for p in raw_list:
        if not p.get('tc_link') and default_url:
            p['tc_link'] = default_url
        v = _validate_promotion(p, bank_name, today)
        if v:
            validated.append(v)

    # Smart URL-aware merge (handles generic listing URLs safely)
    validated = _merge_same_url_promos(validated)

    print(f'    📋 {len(validated)} promotions extracted for {bank_name}')
    return validated


def ai_dedup_titles(titles: list[str], bank_name: str) -> dict:
    """
    Two-pass dedup within a freshly-extracted batch.
    Pass 1 — exact match after aggressive normalisation (catches case variants).
    Pass 2 — similarity ratio >= threshold.
    Returns {duplicate_index: canonical_index}.
    """
    if not titles:
        return {}

    to_remove: dict[int, int] = {}
    norms = [_normalize_for_exact(t) for t in titles]

    # Pass 1: exact normalised match
    for i in range(len(titles)):
        if i in to_remove:
            continue
        for j in range(i + 1, len(titles)):
            if j in to_remove:
                continue
            if norms[i] and norms[j] and norms[i] == norms[j]:
                to_remove[j] = i
                print(
                    f'    ♻  Exact-dedup [{bank_name}]: '
                    f'"{titles[j][:55]}" ≡ "{titles[i][:55]}"'
                )

    # Pass 2: similarity ratio
    for i in range(len(titles)):
        if i in to_remove:
            continue
        for j in range(i + 1, len(titles)):
            if j in to_remove:
                continue
            ratio = _similarity(titles[i], titles[j])
            if ratio >= _DUPLICATE_TITLE_THRESHOLD:
                to_remove[j] = i
                print(
                    f'    ♻  Fuzzy-dedup [{bank_name}]: '
                    f'"{titles[j][:50]}" ≈ "{titles[i][:50]}"  '
                    f'(ratio={ratio:.2f})'
                )

    return to_remove


def ai_match_against_existing(
    promos:      list[dict],
    existing_db: list[dict],
    bank_name:   str,
) -> dict:
    """
    Match newly extracted promos against existing DB rows using URL + title similarity.
    Returns {promo_index: db_id}.
    """
    if not promos or not existing_db:
        return {}

    match_map: dict[int, Any] = {}

    for i, new_p in enumerate(promos):
        for ex_p in existing_db:
            ex_copy              = dict(ex_p)
            ex_copy['bank_name'] = ex_copy.get('bank_name') or bank_name
            new_copy             = dict(new_p)
            new_copy['bank_name'] = new_copy.get('bank_name') or bank_name

            if _are_duplicate(new_copy, ex_copy):
                db_id = (
                    ex_p.get('id') or
                    ex_p.get('_id') or
                    ex_p.get('promo_id') or
                    ex_p.get('promotion_id')
                )
                if db_id is not None:
                    match_map[i] = db_id
                    print(
                        f'    🔗 DB-match [{bank_name}]: '
                        f'"{(new_p.get("title") or "")[:50]}" → id={db_id}'
                    )
                break

    return match_map


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGIC INSIGHTS — expanded 6-column output
# ══════════════════════════════════════════════════════════════════════════════

def generate_strategic_insights(
    promos_by_name: dict,
    db_fetch_fn:    Optional[Callable] = None,
    today:          Optional[str]      = None,
) -> dict:
    """
    Generate best-in-category winners and bank-by-bank analysis.

    best_for items include:
        category, bank, detail, standard, similar_banks, why_others_lose, is_bau

    bank_analysis items include:
        focus, strengths (5-6 pts), count, bau_count, expiring_alert,
        vs_za_pros, vs_za_cons
    """
    if today is None:
        today = datetime.now().strftime('%Y-%m-%d')

    all_promotions: list[dict] = []
    for promos in promos_by_name.values():
        all_promotions.extend(promos)

    non_bau_active = [
        p for p in all_promotions
        if p.get('active') and not p.get('is_bau')
        and (not p.get('end_date') or p['end_date'] >= today)
    ]
    bau_active = [
        p for p in all_promotions
        if p.get('is_bau') and p.get('active')
    ]

    if not non_bau_active and not bau_active:
        return {'best_for': [], 'bank_analysis': {}}

    promo_json = json.dumps([
        {
            'bank':      p.get('bank_name'),
            'title':     p.get('title'),
            'types':     p.get('types'),
            'highlight': (p.get('highlight') or '')[:200],
            'period':    p.get('period'),
            'end_date':  p.get('end_date'),
        }
        for p in non_bau_active[:60]
    ], ensure_ascii=False, indent=2)

    bau_json = json.dumps([
        {
            'bank':  p.get('bank_name'),
            'title': p.get('title'),
            'types': p.get('types'),
        }
        for p in bau_active[:25]
    ], ensure_ascii=False)

    active_counts   = Counter(p.get('bank_name') for p in non_bau_active)
    bau_counts      = Counter(p.get('bank_name') for p in bau_active)
    threshold_date  = (
        datetime.strptime(today, '%Y-%m-%d') + timedelta(days=30)
    ).strftime('%Y-%m-%d')
    expiring_counts = Counter(
        p.get('bank_name')
        for p in non_bau_active
        if p.get('end_date') and today <= p['end_date'] <= threshold_date
    )

    counts_json = json.dumps({
        'active':   dict(active_counts),
        'bau':      dict(bau_counts),
        'expiring': dict(expiring_counts),
    })

    categories_str = '\n'.join(
        f'  {i + 1}. {c}' for i, c in enumerate(INSIGHT_CATEGORIES)
    )
    banks_str = ', '.join(EIGHT_BANKS)

    prompt = (
        'Analyse these Hong Kong virtual bank promotions and return strategic insights.\n\n'
        f'TODAY: {today}\n\n'
        f'ACTIVE (non-BAU) PROMOTIONS:\n{promo_json}\n\n'
        f'BAU PERMANENT FEATURES:\n{bau_json}\n\n'
        f'COUNTS (use these exact numbers):\n{counts_json}\n\n'
        '══════════════════════════════════════════\n'
        'RETURN a JSON object with EXACTLY this structure:\n'
        '══════════════════════════════════════════\n'
        '{\n'
        '  "best_for": [\n'
        '    {\n'
        '      "category":       "Investment (Stock/Crypto Trading)",\n'
        '      "bank":           "Winning bank name  OR  None",\n'
        '      "detail":         "Specific reason citing actual offer names and numbers",\n'
        '      "standard":       "One sentence: the exact metric/criterion used to pick the winner",\n'
        '      "similar_banks":  ["BankX", "BankY"],\n'
        '      "why_others_lose":"Why similar_banks do not win — cite their specific offers and gaps",\n'
        '      "is_bau":         false\n'
        '    }\n'
        '  ],\n'
        '  "bank_analysis": {\n'
        '    "ZA Bank": {\n'
        '      "focus":          "One sentence on current promotional theme",\n'
        '      "strengths":      ["s1 with specifics", "s2", "s3", "s4", "s5", "s6"],\n'
        '      "count":          5,\n'
        '      "bau_count":      2,\n'
        '      "expiring_alert": "N promotions expiring within 30 days  OR  empty string",\n'
        '      "vs_za_pros":     null,\n'
        '      "vs_za_cons":     null\n'
        '    },\n'
        '    "Mox Bank": {\n'
        '      "focus":          "...",\n'
        '      "strengths":      ["s1", "s2", "s3", "s4", "s5"],\n'
        '      "count":          4,\n'
        '      "bau_count":      1,\n'
        '      "expiring_alert": "",\n'
        '      "vs_za_pros":     "Specific advantages over ZA Bank — name actual promotions or rates",\n'
        '      "vs_za_cons":     "Specific areas where ZA Bank currently leads — be precise"\n'
        '    }\n'
        '  }\n'
        '}\n\n'
        f'COVER ALL {len(INSIGHT_CATEGORIES)} categories in best_for:\n'
        f'{categories_str}\n\n'
        f'INCLUDE ALL 8 banks in bank_analysis: {banks_str}\n\n'
        'RULES:\n'
        '  • strengths must have 5–6 bullet points per bank with concrete specifics.\n'
        '  • similar_banks must NOT include the winning bank itself.\n'
        '  • why_others_lose must reference real offer names/rates from the data above.\n'
        '  • standard must be one short sentence explaining the evaluation criterion.\n'
        '  • vs_za_pros / vs_za_cons: name the actual promotions or rates — no vague language.\n'
        '  • Use exact counts from the COUNTS object above.\n'
        '  • Return ONLY JSON — no prose, no markdown fences.'
    )

    try:
        raw = _call_ai(prompt, model='gpt-4o')
        raw = raw.strip()
        # Strip any accidental markdown fences
        if raw.startswith('```'):
            raw = re.sub(r'^```[a-z]*\n?', '', raw)
            raw = re.sub(r'\n?```$', '', raw)
        data = json.loads(raw)

        # Back-fill any fields the AI omitted
        for item in data.get('best_for', []):
            item.setdefault('standard',        'AI evaluates based on best overall value.')
            item.setdefault('similar_banks',   [])
            item.setdefault('why_others_lose', '')

        for bname, bdata in data.get('bank_analysis', {}).items():
            bdata.setdefault('strengths',      [])
            bdata.setdefault('vs_za_pros',     None)
            bdata.setdefault('vs_za_cons',     None)
            bdata.setdefault('expiring_alert', '')

        return data

    except Exception as exc:
        print(f'  ❌ Strategic insights error: {exc}')
        return {'best_for': [], 'bank_analysis': {}}


# ── Legacy all-in-one entry point ─────────────────────────────────────────────

def extract_promotions(
    scraped_data:        dict,
    existing_promotions: list[dict],
    today:               Optional[str] = None,
) -> tuple[list[dict], list[str]]:
    """Legacy single-call pipeline kept for backwards compatibility."""
    if today is None:
        today = datetime.now().strftime('%Y-%m-%d')

    all_logs: list[str] = []

    existing_keys: set[tuple[str, str]] = {
        (p.get('bank_name', ''), (p.get('title') or '').lower())
        for p in existing_promotions
    }
    bau_to_add: list[dict] = []
    for bau in STATIC_BAU_PROMOTIONS:
        key = (bau['bank_name'], bau['title'].lower())
        if key not in existing_keys:
            entry               = dict(bau)
            entry['created_at'] = today
            entry['last_seen']  = today
            bau_to_add.append(entry)
            all_logs.append(f'STATIC BAU: [{bau["bank_name"]}] {bau["title"]}')

    existing_by_bank: dict[str, list[dict]] = {}
    for p in existing_promotions:
        bn = p.get('bank_name') or p.get('bank') or 'Unknown'
        existing_by_bank.setdefault(bn, []).append(p)

    all_reconciled:  list[dict] = []
    processed_banks: set[str]   = set()

    for bank_id, bank_data in scraped_data.items():
        bank_name    = bank_data.get('bank_name', bank_id)
        scraped_text = bank_data.get('text') or ''

        if not scraped_text:
            all_logs.append(f'SKIP (no text): {bank_name}')
            all_reconciled.extend(existing_by_bank.get(bank_name, []))
            continue

        print(f'\n  🤖 AI extraction: {bank_name}...')
        processed_banks.add(bank_name)

        existing_for_bank = existing_by_bank.get(bank_name, [])

        prompt = _build_extraction_prompt(
            bank_name         = bank_name,
            scraped_text      = scraped_text,
            existing_for_bank = existing_for_bank,
            today             = today,
        )

        try:
            raw      = _call_ai(prompt)
            raw_list = _parse_ai_json(raw, bank_name)
        except Exception as exc:
            all_logs.append(f'AI_ERROR: {bank_name} — {exc}')
            print(f'    ❌ AI error for {bank_name}: {exc}')
            all_reconciled.extend(existing_for_bank)
            continue

        validated: list[dict] = []
        for p in raw_list:
            v = _validate_promotion(p, bank_name, today)
            if v:
                validated.append(v)

        validated        = deduplicate_promotions(validated)
        reconciled, logs = reconcile_with_existing(validated, existing_for_bank, today)
        all_logs.extend(logs)
        all_reconciled.extend(reconciled)
        print(f'    ✓  {bank_name}: {len(validated)} extracted → {len(reconciled)} reconciled')

    for bank_name, promos in existing_by_bank.items():
        if bank_name not in processed_banks:
            all_reconciled.extend(promos)

    all_reconciled.extend(bau_to_add)
    all_reconciled = deduplicate_promotions(all_reconciled)
    all_reconciled = revalidate_expired(all_reconciled, today)

    return all_reconciled, all_logs