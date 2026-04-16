# scripts/ai_helper.py

"""
AI extraction, deduplication, validation and strategic-insights generation.

Key design decisions
────────────────────
1. URL-based dedup first   — one source URL → one promotion (merge sub-features)
2. Title-similarity dedup  — fuzzy match prevents near-identical entries
3. Exact-title enforcement — prompt explicitly forbids AI from renaming promos
4. start_date gate          — promotions whose start_date < today are NOT flagged
                              as "new today" even if first detected today
5. Expired re-validation    — if end_date >= today the promo is kept active;
                              if found in active list it is a duplicate → remove
6. Static BAU injections    — Ant Bank BAU entries are maintained here so they
                              are always present regardless of scrape quality
"""

import json
import os
import re
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Optional

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

_DUPLICATE_TITLE_THRESHOLD = 0.76   # SequenceMatcher ratio — above this = duplicate
_DUPLICATE_DESC_THRESHOLD  = 0.88

VALID_TYPES = [
    '迎新 Welcome', '消費 Spending', '投資 Investment', '旅遊 Travel',
    '保險 Insurance', '貸款 Loan', '活期存款 Savings', '定期存款 TDeposit',
    '外匯 FX', '推薦 Referral', '新資金 New Funds', 'Others 其他',
]

# ── Static BAU promotions ─────────────────────────────────────────────────────
#
#  Add permanent bank features here.  They are merged into data.json on every
#  run if not already present.  is_bau=True entries are excluded from all
#  "new today / new this week" detection.
#
STATIC_BAU_PROMOTIONS: list[dict] = [
    # ── Ant Bank ──────────────────────────────────────────────────
    {
        'bank_name': 'Ant Bank',
        'title':     'SME Loan Cash Rebate Promotion',
        'highlight': 'SME customers enjoy cash rebates on eligible business loan products.',
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
        'bank_name': 'Ant Bank',
        'title':     '100% Insurance Premium Rebate',
        'highlight': "Enjoy 100% rebate on the first month's premium for eligible insurance products.",
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
        'bank_name': 'Ant Bank',
        'title':     'Insurance Products with up to 3.6% Annualized Rate',
        'highlight': 'Ant Bank insurance savings products offer up to 3.6% p.a. annualized return.',
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

# ── String helpers ────────────────────────────────────────────────────────────

def _similarity(a: str, b: str) -> float:
    """SequenceMatcher ratio — 0.0 to 1.0."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _normalize_url(url: str) -> str:
    """
    Canonical URL for dedup comparison.
    Strips protocol, trailing slash, and lowercases.
    Query params are kept because Fusion Bank uses them to distinguish pages.
    """
    if not url:
        return ''
    url = url.lower().strip().rstrip('/')
    url = re.sub(r'^https?://', '', url)
    return url


def _score_completeness(p: dict) -> int:
    """Score a promotion by information richness — used to pick the best when merging."""
    return (
        len(p.get('description') or '') +
        len(p.get('highlight') or '') +
        len(p.get('title') or '') * 2 +
        len(p.get('quota') or '') +
        len(p.get('cost') or '') +
        (20 if p.get('end_date')   else 0) +
        (20 if p.get('start_date') else 0) +
        (10 if p.get('tc_link')    else 0)
    )

# ── Deduplication ─────────────────────────────────────────────────────────────

def _are_duplicate(p1: dict, p2: dict) -> bool:
    """
    Return True if p1 and p2 appear to be the same promotion.

    Rules (evaluated in priority order):
      1. Different bank → never duplicates.
      2. Same normalized source URL → duplicate.
      3. Title similarity >= threshold (same bank) → duplicate.
    """
    b1 = (p1.get('bank_name') or p1.get('bank') or '').lower().strip()
    b2 = (p2.get('bank_name') or p2.get('bank') or '').lower().strip()
    if not b1 or not b2 or b1 != b2:
        return False

    # Rule 2 — same source URL
    u1 = _normalize_url(p1.get('tc_link') or p1.get('url') or '')
    u2 = _normalize_url(p2.get('tc_link') or p2.get('url') or '')
    if u1 and u2 and u1 == u2:
        return True

    # Rule 3 — very similar title
    t1 = (p1.get('title') or p1.get('name') or '').strip()
    t2 = (p2.get('title') or p2.get('name') or '').strip()
    if t1 and t2 and _similarity(t1, t2) >= _DUPLICATE_TITLE_THRESHOLD:
        return True

    return False


def _merge_group(group: list[dict]) -> dict:
    """
    Merge a group of duplicates into one canonical entry.
    Keep the most complete entry; union all category types.
    """
    best = max(group, key=_score_completeness)
    all_types: list[str] = []
    seen_t: set[str]     = set()
    for p in group:
        for t in (p.get('types') or []):
            if t not in seen_t:
                seen_t.add(t)
                all_types.append(t)
    best['types'] = all_types or ['Others 其他']
    return best


def deduplicate_promotions(promotions: list[dict]) -> list[dict]:
    """
    Two-pass deduplication:
      Pass 1 — group by normalized source URL → merge each group into one entry.
      Pass 2 — pairwise title-similarity check → remove near-duplicates.

    When two entries would be merged, the more complete one is kept and the
    less complete one is discarded (with a log message).
    """
    if not promotions:
        return []

    # Pass 1: URL grouping
    url_groups: dict[str, list[dict]] = {}
    no_url_list: list[dict]           = []

    for p in promotions:
        url = _normalize_url(p.get('tc_link') or p.get('url') or '')
        if url:
            url_groups.setdefault(url, []).append(p)
        else:
            no_url_list.append(p)

    pass1: list[dict] = []
    for url, group in url_groups.items():
        if len(group) == 1:
            pass1.append(group[0])
        else:
            merged = _merge_group(group)
            pass1.append(merged)
            print(
                f'    🔗 URL-dedup: merged {len(group)} entries '
                f'-> 1  [{url[:55]}]'
            )
    pass1.extend(no_url_list)

    # Pass 2: Title-similarity pairwise
    final: list[dict] = []
    for p in pass1:
        found_dup = False
        for i, existing in enumerate(final):
            if _are_duplicate(p, existing):
                found_dup = True
                # Keep the more complete entry
                if _score_completeness(p) > _score_completeness(existing):
                    final[i] = p
                title_preview = (p.get('title') or '')[:60]
                bank_name     = p.get('bank_name', '?')
                print(f'    ♻  Title-dedup removed: "{title_preview}" [{bank_name}]')
                break
        if not found_dup:
            final.append(p)

    return final

# ── New-today / new-this-week helpers ────────────────────────────────────────

def is_new_today(promo: dict, today: str) -> bool:
    """
    A promotion qualifies as 'new today' only when ALL conditions hold:
      1. first detected today (created_at date == today)
      2. start_date is on or after today — OR start_date is unknown/ongoing
         (prevents old campaigns discovered for the first time from appearing
          as newly launched)
    """
    created = (promo.get('created_at') or '')[:10]
    if created != today:
        return False
    start = (promo.get('start_date') or '')[:10]
    if start and start < today:
        return False       # Promotion started before today — not a new launch
    return True


def is_new_this_week(promo: dict, today: str) -> bool:
    """
    A promotion qualifies for the 'this week' section (days 2-7 before today)
    when ALL conditions hold:
      1. first detected between 6 days ago and yesterday (not today)
      2. start_date is within that same 6-day window — OR start_date unknown
    """
    created = (promo.get('created_at') or '')[:10]
    if not created:
        return False
    try:
        today_dt   = datetime.strptime(today, '%Y-%m-%d')
        created_dt = datetime.strptime(created, '%Y-%m-%d')
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

# ── Expired re-validation ─────────────────────────────────────────────────────

def revalidate_expired(
    promotions: list[dict],
    today:      str,
) -> list[dict]:
    """
    Walk every promotion marked active=False.
    If its end_date is still >= today, it should NOT be expired — re-activate it.
    Additionally, if a promo that is marked expired is an exact duplicate of an
    already-active entry, remove the expired copy entirely.

    This resolves the Mox batch that were wrongly marked expired.
    """
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

        # Re-check end date
        end = (p.get('end_date') or '')[:10]
        if end and end >= today:
            # Valid end date — not actually expired
            p_fixed       = dict(p)
            p_fixed['active'] = True
            title_preview = (p.get('title') or '')[:60]
            bank_label    = p.get('bank_name', '?')
            print(
                f'    🔄 Re-activated (valid end_date {end}): '
                f'"{title_preview}" [{bank_label}]'
            )
            corrected.append(p_fixed)
            continue

        # No end date — assume ongoing if page URL still returns content
        if not end:
            url = _normalize_url(p.get('tc_link') or p.get('url') or '')
            if url and url in active_set:
                # Already represented by an active entry → true duplicate, drop it
                title_preview = (p.get('title') or '')[:60]
                bank_label    = p.get('bank_name', '?')
                print(
                    f'    🗑  Expired+duplicate removed: '
                    f'"{title_preview}" [{bank_label}]'
                )
                continue
            # Otherwise keep it but don't re-activate without scrape confirmation
            corrected.append(p)
            continue

        corrected.append(p)

    return corrected

# ── Reconcile newly extracted vs existing ────────────────────────────────────

def reconcile_with_existing(
    newly_extracted:  list[dict],
    existing_promos:  list[dict],
    today:            str,
) -> tuple[list[dict], list[str]]:
    """
    Merge newly extracted promotions with the existing database for one bank.

    Logic:
    - If new promotion matches an existing one -> update last_seen, refresh fields.
    - If new promotion is genuinely new        -> set created_at = today.
    - Existing promotions not seen this run:
        end_date >= today  -> keep active (valid by date)
        no end_date        -> keep (ongoing)
        end_date < today   -> mark expired

    Returns (reconciled_list, log_messages).
    """
    logs:         list[str]  = []
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
            # Refresh mutable fields if changed
            for fld in ('description', 'highlight', 'period', 'end_date', 'start_date',
                        'quota', 'cost', 'types'):
                if new_p.get(fld) and new_p.get(fld) != ex_p.get(fld):
                    updated[fld] = new_p[fld]
            reconciled.append(updated)
        else:
            # Genuinely new
            new_p.setdefault('created_at', today)
            new_p['last_seen'] = today
            new_p.setdefault('active', True)
            reconciled.append(new_p)
            logs.append(
                f'NEW: [{new_p.get("bank_name")}] {new_p.get("title", "?")}'
            )

    # Handle existing entries not matched by this scrape run
    for idx, ex_p in enumerate(existing_promos):
        if idx in matched_idxs:
            continue
        end = (ex_p.get('end_date') or '')[:10]
        if end and end < today:
            expired_p = dict(ex_p)
            expired_p['active'] = False
            reconciled.append(expired_p)
            logs.append(
                f'EXPIRED: [{ex_p.get("bank_name")}] {ex_p.get("title", "?")}'
            )
        else:
            kept_p = dict(ex_p)
            kept_p['last_seen'] = today
            reconciled.append(kept_p)

    return reconciled, logs

# ── AI prompt builder ─────────────────────────────────────────────────────────

def _build_extraction_prompt(
    bank_name:          str,
    scraped_text:       str,
    existing_for_bank:  list[dict],
    today:              str,
) -> str:
    """
    Build the extraction prompt sent to GPT-4o.

    The prompt encodes four hard rules that directly address the recurring
    issues this system has experienced:
      R1 — Exact title copying         (prevents wrong naming like "Tesla Loan")
      R2 — One promo per source URL    (prevents splitting combined campaigns)
      R3 — Strict dedup vs database    (prevents re-adding existing entries)
      R4 — Start-date gate for "new"   (prevents old promos appearing as new)
    """
    if not existing_for_bank:
        db_lines = '  (none — this is a fresh bank)'
    else:
        db_lines = '\n'.join(
            '  * [URL: {}]  "{}"'.format(
                p.get('tc_link') or p.get('url') or 'no-url',
                p.get('title') or '?'
            )
            for p in existing_for_bank[:30]
        )

    valid_types_str = ', '.join(VALID_TYPES)

    return (
        f'You are extracting structured bank promotion data for {bank_name}.\n\n'
        f'TODAY: {today}\n\n'
        f'PROMOTIONS ALREADY IN THE DATABASE FOR {bank_name}\n'
        f'(You MUST NOT re-add any of these — check carefully before adding anything):\n'
        f'{db_lines}\n\n'
        f'SCRAPED WEBSITE TEXT (multiple pages, each prefixed with === SOURCE: [URL] ===):\n'
        f'{scraped_text[:32000]}\n\n'
        '══════════════════════════════════════════════════\n'
        'EXTRACTION RULES — READ ALL BEFORE WRITING OUTPUT\n'
        '══════════════════════════════════════════════════\n\n'
        'RULE 1 — EXACT TITLE (most common error — please follow strictly):\n'
        '* Copy the promotion title VERBATIM from the source page text.\n'
        '* Do NOT rename, simplify, embellish, or invent a title.\n'
        '* WRONG: "WeLab Bank Referral Reward for Tesla Loan"\n'
        '    (AI invented this; the page is about a general R-Friend loan referral)\n'
        '* RIGHT:  "WeLab Bank Personal Loan \'R-Friend Referral\' Campaign"\n'
        '    (copied from the page title)\n'
        '* WRONG: "Foreign Exchange Time Deposit Interest Rate Boost"\n'
        '    (AI invented this by merging two Fusion Bank concepts)\n'
        '* RIGHT:  Use the actual campaign name exactly as written on the page.\n\n'
        'RULE 2 — ONE PROMOTION PER SOURCE URL:\n'
        '* If one campaign page describes multiple benefits (e.g. time deposit rate PLUS\n'
        '  fund cash reward), create EXACTLY ONE promotion covering ALL benefits.\n'
        '* Do NOT split a single campaign page into multiple promotion entries.\n'
        '* WRONG: Two separate entries both pointing to the same URL.\n'
        '* RIGHT:  One entry with description covering all benefits on that page.\n\n'
        'RULE 3 — STRICT DEDUPLICATION (second most common error):\n'
        '* Before adding ANY promotion, check the "ALREADY IN DATABASE" list above.\n'
        '* If the SAME URL already appears in that list -> SKIP, do not add again.\n'
        '* If a VERY SIMILAR TITLE for the same bank already appears -> SKIP.\n'
        '* When uncertain: SKIP rather than risk adding a duplicate.\n\n'
        f'RULE 4 — START DATE GATE:\n'
        f'* If a promotion start_date can be determined and it is BEFORE {today},\n'
        f'  set "is_new_today": false — it is not a new launch even if scraped today.\n'
        f'* Only set "is_new_today": true when start_date >= {today} OR unknown.\n\n'
        f'RULE 5 — ACTIVE / EXPIRED:\n'
        f'* end_date < {today}   -> "active": false\n'
        f'* end_date >= {today}  -> "active": true\n'
        '* No end_date / Ongoing -> "active": true\n'
        '* If the source page has substantial content and no expiry notice -> "active": true\n\n'
        'RULE 6 — SOURCE LINK:\n'
        '* Use the most specific URL for each promotion (campaign detail page > home page).\n'
        '* Source URLs appear as === SOURCE: [URL] === markers in the text above.\n\n'
        '══════════════════════════════════════════════════\n'
        'OUTPUT FORMAT — return ONLY a valid JSON array:\n'
        '══════════════════════════════════════════════════\n'
        '[\n'
        '  {\n'
        '    "title":       "exact title copied from the source page",\n'
        f'    "bank_name":   "{bank_name}",\n'
        '    "types":       ["迎新 Welcome"],\n'
        '    "highlight":   "one-sentence benefit summary",\n'
        '    "description": "full description covering ALL benefits on this page",\n'
        '    "period":      "DD MMM YYYY to DD MMM YYYY  OR  Ongoing",\n'
        '    "start_date":  "YYYY-MM-DD  OR  null",\n'
        '    "end_date":    "YYYY-MM-DD  OR  null",\n'
        '    "quota":       "eligibility / who qualifies",\n'
        '    "cost":        "minimum spend or deposit  OR  null",\n'
        '    "tc_link":     "https://most-specific-source-url",\n'
        '    "is_bau":      false,\n'
        '    "active":      true,\n'
        '    "is_new_today": false\n'
        '  }\n'
        ']\n\n'
        f'Valid type values: {valid_types_str}\n\n'
        'Return [] if no promotions found.  Return ONLY the JSON array — no prose.'
    )


# ── AI call ───────────────────────────────────────────────────────────────────

def _call_ai(
    prompt: str,
    model:  str = 'gpt-4o',
    seed:   int = 42,
) -> str:
    """
    Call the OpenAI chat completion API and return raw response text.
    Low temperature (0.05) minimises hallucination of titles/names.
    """
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
                    'You NEVER create duplicate entries — you always check the existing '
                    'database before adding anything. '
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
    """Parse AI response that may be a bare array or wrapped in an object."""
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

    # Last-resort: extract first JSON array from text
    m = re.search(r'\[.*\]', raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass

    print(f'    ⚠  Could not parse AI response for {bank_name}')
    return []


def _validate_promotion(p: dict, bank_name: str, today: str) -> Optional[dict]:
    """
    Normalise and validate one promotion dict.
    Returns None (discard) if the entry is clearly invalid.
    """
    title = (p.get('title') or '').strip()
    if not title or len(title) < 3:
        return None

    p['bank_name'] = bank_name

    # Normalise types
    raw_t = p.get('types') or []
    if isinstance(raw_t, str):
        raw_t = [raw_t]
    valid_t = [t for t in raw_t if t in VALID_TYPES]
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

    # If end_date is in the past, mark inactive
    if p['end_date'] and p['end_date'] < today:
        p['active'] = False

    # Remove transient is_new_today flag (computed dynamically in frontend/email)
    p.pop('is_new_today', None)

    return p

# ── Main extraction entry point ───────────────────────────────────────────────

def extract_promotions(
    scraped_data:        dict,
    existing_promotions: list[dict],
    today:               str = None,
) -> tuple[list[dict], list[str]]:
    """
    Extract and reconcile promotions from all scraped bank data.

    Args:
        scraped_data:        Output of run_scraper() — keyed by bank_id.
        existing_promotions: Current data.json promotions list.
        today:               Date string YYYY-MM-DD (defaults to HKT today).

    Returns:
        (all_promotions, log_messages)
    """
    if today is None:
        today = datetime.now().strftime('%Y-%m-%d')

    all_logs: list[str] = []

    # ── Inject missing static BAU entries ────────────────────────
    existing_keys: set[tuple[str, str]] = {
        (p.get('bank_name', ''), (p.get('title') or '').lower())
        for p in existing_promotions
    }
    bau_to_add: list[dict] = []
    for bau in STATIC_BAU_PROMOTIONS:
        key = (bau['bank_name'], bau['title'].lower())
        if key not in existing_keys:
            entry = dict(bau)
            entry['created_at'] = today
            entry['last_seen']  = today
            bau_to_add.append(entry)
            all_logs.append(f'STATIC BAU: [{bau["bank_name"]}] {bau["title"]}')

    # ── Group existing by bank ────────────────────────────────────
    existing_by_bank: dict[str, list[dict]] = {}
    for p in existing_promotions:
        bn = p.get('bank_name') or p.get('bank') or 'Unknown'
        existing_by_bank.setdefault(bn, []).append(p)

    all_reconciled: list[dict] = []
    processed_banks: set[str]  = set()

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

        # Validate each extracted promotion
        validated: list[dict] = []
        for p in raw_list:
            v = _validate_promotion(p, bank_name, today)
            if v:
                validated.append(v)

        # Dedup within this batch
        validated = deduplicate_promotions(validated)

        # Reconcile with existing database entries for this bank
        reconciled, logs = reconcile_with_existing(validated, existing_for_bank, today)
        all_logs.extend(logs)
        all_reconciled.extend(reconciled)
        print(
            f'    ✓  {bank_name}: {len(validated)} extracted '
            f'-> {len(reconciled)} reconciled'
        )

    # Carry forward banks that were not scraped this run
    for bank_name, promos in existing_by_bank.items():
        if bank_name not in processed_banks:
            all_reconciled.extend(promos)

    # Add static BAU promotions that were missing
    all_reconciled.extend(bau_to_add)

    # Global dedup pass across ALL banks
    all_reconciled = deduplicate_promotions(all_reconciled)

    # Re-validate expired promotions (fixes wrongly-expired Mox promotions etc.)
    all_reconciled = revalidate_expired(all_reconciled, today)

    return all_reconciled, all_logs

# ── Strategic insights ────────────────────────────────────────────────────────

def generate_strategic_insights(
    all_promotions: list[dict],
    today:          str = None,
) -> dict:
    """
    Generate best-in-category winners and bank-by-bank analysis.
    Called by main.py; result is stored in data.json as "strategic_insights".
    """
    if today is None:
        today = datetime.now().strftime('%Y-%m-%d')

    non_bau_active = [
        p for p in all_promotions
        if p.get('active') and not p.get('is_bau')
        and (not p.get('end_date') or p['end_date'] >= today)
    ]
    bau_active = [p for p in all_promotions if p.get('is_bau') and p.get('active')]

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

    from collections import Counter
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

    prompt = (
        'Analyse these Hong Kong virtual bank promotions and return strategic insights.\n\n'
        f'TODAY: {today}\n\n'
        f'ACTIVE (non-BAU) PROMOTIONS:\n{promo_json}\n\n'
        f'BAU PERMANENT FEATURES:\n{bau_json}\n\n'
        f'COUNTS (use these exact numbers):\n{counts_json}\n\n'
        'Return a JSON object with EXACTLY this structure:\n'
        '{\n'
        '  "best_for": [\n'
        '    {\n'
        '      "category": "Investment (Stock/Crypto Trading)",\n'
        '      "bank": "Bank Name or None",\n'
        '      "detail": "Specific reason with actual numbers/offer names",\n'
        '      "is_bau": false\n'
        '    }\n'
        '  ],\n'
        '  "bank_analysis": {\n'
        '    "ZA Bank": {\n'
        '      "focus": "One sentence on current promotional theme",\n'
        '      "strengths": ["strength 1 with specifics", "strength 2", "strength 3"],\n'
        '      "count": 5,\n'
        '      "bau_count": 2,\n'
        '      "expiring_alert": "3 promotions expiring within 30 days",\n'
        '      "vs_za_pros": null,\n'
        '      "vs_za_cons": null\n'
        '    },\n'
        '    "Mox Bank": {\n'
        '      "focus": "...",\n'
        '      "strengths": ["..."],\n'
        '      "count": 4,\n'
        '      "bau_count": 1,\n'
        '      "expiring_alert": "",\n'
        '      "vs_za_pros": "Stronger travel and device promos",\n'
        '      "vs_za_cons": "Fewer investment promotions"\n'
        '    }\n'
        '  }\n'
        '}\n\n'
        'Cover ALL 8 categories in best_for:\n'
        '  Investment (Stock/Crypto Trading), Fund Investment, Spending/CashBack,\n'
        '  Welcome Bonus, Travel, Loan APR, FX/Multi-Currency, Referral Bonus\n\n'
        'Include ALL 8 banks in bank_analysis:\n'
        '  ZA Bank, Mox Bank, WeLab Bank, livi bank, PAObank, Airstar Bank,\n'
        '  Fusion Bank, Ant Bank\n\n'
        'Use the exact counts from the COUNTS object above.\n'
        'Return ONLY JSON.'
    )

    try:
        raw  = _call_ai(prompt, model='gpt-4o')
        data = json.loads(raw)
        return data
    except Exception as exc:
        print(f'  ❌ Strategic insights error: {exc}')
        return {'best_for': [], 'bank_analysis': {}}