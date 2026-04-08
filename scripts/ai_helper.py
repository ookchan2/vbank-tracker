# scripts/ai_helper.py

import asyncio
import concurrent.futures
import json
import os
import re

_api_key     = None
_bot_name    = "Claude-3-7-Sonnet"
AI_AVAILABLE = False

MODELS_TO_TRY = [
    "Claude-3-7-Sonnet",
    "Claude-3-5-Sonnet",
    "GPT-4o",
    "Perplexity-Pro-Search",
]

ALLOWED_CATEGORIES = [
    "迎新", "消費", "投資", "旅遊", "保險",
    "貸款", "存款", "外匯", "推薦", "新資金", "Others"
]

# ← CHANGED: hardcoded BAU overrides — always flag regardless of AI classification
# Format: { bank_id_lowercase: ["substring to match in title (lowercase)"] }
BAU_OVERRIDES: dict[str, list[str]] = {
    "za": [
        "new crypto customer fee waiver",
    ],
}

# ── Extraction prompt ─────────────────────────────────────────────────────────

_PROMPT_TMPL = """\
You are a specialist at extracting bank promotion data from website text.

Bank: BANK_NAME_PLACEHOLDER
Source URL: URL_PLACEHOLDER

╔══════════════════════════════════════════════════════════════════════╗
║  CRITICAL RULES — read carefully before extracting                  ║
║                                                                      ║
║  1. Extract EVERY SINGLE promotion you can find.                    ║
║     If you see 25 promotions → return exactly 25 objects.           ║
║                                                                      ║
║  2. Do NOT merge multiple promotions into one entry.                ║
║                                                                      ║
║  3. name and highlight must be in English.                          ║
║                                                                      ║
║  4. For start_date / end_date: look for any date mentioned near     ║
║     the promotion (e.g. "Valid until 30 Jun 2025",                  ║
║     "From 1 Jan 2025 to 31 Mar 2025", "Ends 31 Dec 2025").         ║
║     Always use YYYY-MM-DD format.  Use null only if truly absent.  ║
║                                                                      ║
║  5. is_bau: set true ONLY for permanent product features with       ║
║     NO end date and NO special eligibility condition, e.g.:         ║
║       ✅ BAU: "Free Instant FPS Transfers" (always available)       ║
║       ✅ BAU: "Multi-Currency Savings Account" (product feature)    ║
║       ✅ BAU: "New Crypto Customer Fee Waiver" (ZA Bank, permanent) ║
║       ❌ NOT BAU: "New Customer Bonus" (new customers only)         ║
║       ❌ NOT BAU: "Limited-Time Fee Waiver" (has end date)          ║
║       ❌ NOT BAU: Any promotion with a promo code                   ║
╚══════════════════════════════════════════════════════════════════════╝

ALLOWED CATEGORY TAGS (Chinese, pick 1-3 per promotion):
  迎新 / 消費 / 投資 / 旅遊 / 保險 / 貸款 / 存款 / 外匯 / 推薦 / 新資金 / Others

REQUIRED OUTPUT: A valid JSON array — NO other text, NO markdown fences.

Schema for each object:
{
  "name":        "Full descriptive English name of the promotion",
  "types":       ["category1", "category2"],
  "is_bau":      false,
  "start_date":  "YYYY-MM-DD or null",
  "end_date":    "YYYY-MM-DD or null",
  "period":      "Human-readable period, e.g. '1 Jan 2025 to 31 Mar 2025' or 'Ongoing'",
  "highlight":   "One-line key benefit starting with an emoji",
  "description": "2-3 sentences describing this specific promotion in detail.",
  "quota":       "Eligibility or quota info (e.g. First 1000 customers / New customers only / No cap)",
  "cost":        "Minimum spend or required cost, or Free",
  "tc_link":     "URL_PLACEHOLDER"
}

WEBSITE TEXT TO ANALYSE:
────────────────────────────────────────────────────────────────────────
TEXT_PLACEHOLDER
────────────────────────────────────────────────────────────────────────
Remember: return ONLY the JSON array starting with [ and ending with ]."""


def _build_prompt(bank_name: str, url: str, text: str) -> str:
    return (
        _PROMPT_TMPL
        .replace('BANK_NAME_PLACEHOLDER', bank_name)
        .replace('URL_PLACEHOLDER',       url)
        .replace('TEXT_PLACEHOLDER',      text)
    )


# ── Poe async core ────────────────────────────────────────────────────────────

async def _async_call(messages: list, bot_name: str) -> str:
    try:
        import fastapi_poe as fp
        poe_messages = [
            fp.ProtocolMessage(role=m['role'], content=m['content'])
            for m in messages
        ]
        response_text = ''
        async for partial in fp.get_bot_response(
            messages=poe_messages,
            bot_name=bot_name,
            api_key=_api_key,
        ):
            response_text += partial.text
        return response_text.strip()
    except Exception as e:
        print(f'  ⚠️  Poe async call error ({bot_name}): {e}')
        return ''


def _run_async(coro) -> str:
    try:
        asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result(timeout=120)
    except RuntimeError:
        return asyncio.run(coro)
    except Exception as e:
        print(f'  ⚠️  _run_async error: {e}')
        return ''


def _call(messages: list) -> str:
    if not AI_AVAILABLE or _api_key is None:
        return ''
    try:
        result = _run_async(_async_call(messages, _bot_name))
        print(f'  [DEBUG] AI ({_bot_name}) returned {len(result)} chars')
        if len(result) < 50:
            print(f'  [DEBUG] Full response: {repr(result)}')
        return result
    except Exception as e:
        print(f'  ⚠️  Call error: {e}')
        return ''


# ── Init ──────────────────────────────────────────────────────────────────────

def init_ai() -> bool:
    global _api_key, _bot_name, AI_AVAILABLE
    try:
        import fastapi_poe  # noqa
        key = os.environ.get('POE_API_KEY', '').strip()
        if not key:
            print('⚠️  POE_API_KEY not set — AI disabled')
            return False
        _api_key = key
        for model in MODELS_TO_TRY:
            print(f'  🔍 Testing model: {model} ...')
            try:
                test = _run_async(
                    _async_call([{'role': 'user', 'content': 'Reply OK only.'}], model)
                )
            except Exception as e:
                print(f'  ❌ {model} error: {e}')
                test = ''
            if test:
                _bot_name    = model
                AI_AVAILABLE = True
                print(f'✅ Poe ready: {_bot_name}')
                return True
            print(f'  ❌ {model} failed, trying next...')
        print('❌ All models failed — AI disabled')
        AI_AVAILABLE = False
        return False
    except ImportError:
        print('❌ fastapi-poe not installed')
        AI_AVAILABLE = False
        return False
    except Exception as e:
        print(f'❌ AI init failed: {e}')
        AI_AVAILABLE = False
        return False


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _parse_array(raw: str) -> list:
    if not raw:
        return []
    raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
    raw = re.sub(r'\s*```$',          '', raw, flags=re.MULTILINE)
    raw = raw.strip()
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        pass
    m = re.search(r'(\[.*\])', raw, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(1))
            return data if isinstance(data, list) else [data]
        except Exception:
            pass
    for suffix in ('}]', ']'):
        try:
            data = json.loads(raw + suffix)
            return data if isinstance(data, list) else [data]
        except Exception:
            pass
    print(f'  ⚠️  JSON parse failed. First 200 chars: {raw[:200]}')
    return []


def _parse_object(raw: str) -> dict | None:
    if not raw:
        return None
    raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
    raw = re.sub(r'\s*```$',          '', raw, flags=re.MULTILINE)
    raw = raw.strip()
    m = re.search(r'(\{.*\})', raw, re.DOTALL)
    if m:
        raw = m.group(1)
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError as e:
        print(f'  ⚠️  JSON object parse failed: {e}. First 200 chars: {raw[:200]}')
        return None


def _trim_text(text: str, max_chars: int = 18000) -> str:
    if len(text) <= max_chars:
        return text
    keep = max_chars // 2
    return (
        text[:keep]
        + f'\n\n…[{len(text) - max_chars:,} chars trimmed]…\n\n'
        + text[-keep:]
    )


def _stamp(promos: list, bank_id: str, bank_name: str, default_url: str) -> list:
    for p in promos:
        p['bank']    = bank_id
        p['bName']   = bank_name
        p.setdefault('link',        default_url)
        p.setdefault('tc_link',     default_url)
        p.setdefault('types',       ['Others'])
        p.setdefault('is_bau',      False)
        p.setdefault('start_date',  None)
        p.setdefault('end_date',    None)
        p.setdefault('period',      'Ongoing')
        p.setdefault('highlight',   '')
        p.setdefault('description', '')
        p.setdefault('quota',       'Check official website')
        p.setdefault('cost',        'Check official website')
        if not p.get('title') and p.get('name'):
            p['title'] = p['name']
    return promos


# ← CHANGED: force-set is_bau=True for promos matching BAU_OVERRIDES
def _apply_bau_overrides(promos: list, bank_id: str) -> list:
    """Force-set is_bau=True for promotions matching hardcoded BAU_OVERRIDES."""
    overrides = [o.lower() for o in BAU_OVERRIDES.get(bank_id.lower(), [])]
    if not overrides:
        return promos
    for p in promos:
        title = (p.get('name') or p.get('title') or '').lower()
        if any(override in title for override in overrides):
            if not p.get('is_bau'):
                p['is_bau'] = True
                print(f'    🔒 BAU override: {p.get("name") or p.get("title")}')
    return promos


# ── Public API ────────────────────────────────────────────────────────────────

def analyze_promotions(bank_id: str,
                       bank_name: str,
                       text: str = '',
                       screenshot: bytes = None,
                       default_url: str = '') -> list:

    if not AI_AVAILABLE:
        return []

    clean = _trim_text(text.strip() if text else '')
    results: list = []

    if len(clean) >= 200:
        prompt = _build_prompt(bank_name=bank_name, url=default_url, text=clean)

        for attempt in range(2):
            raw    = _call([{'role': 'user', 'content': prompt}])
            parsed = _parse_array(raw)
            if parsed:
                results = parsed
                bau_count = sum(1 for p in parsed if p.get('is_bau'))
                print(
                    f'  📝 Text → {len(results)} promotions for {bank_name} '
                    f'({bau_count} BAU)'
                )
                break
            if attempt == 0:
                print(f'  🔄 Retry AI for {bank_name}...')
        else:
            print(f'  ❌ Both attempts failed for {bank_name}')
    else:
        print(f'  ⚠️  Text too short ({len(clean)} chars) for {bank_name}')

    results = _stamp(results, bank_id, bank_name, default_url)
    results = _apply_bau_overrides(results, bank_id)  # ← CHANGED
    print(f'  ✅ Total: {len(results)} promotions for {bank_name}')
    return results


def ai_dedup_titles(titles: list[str], bank_name: str) -> dict[int, int]:
    """
    Ask the LLM to find semantic duplicates that formula-based dedup cannot catch.
    Returns {duplicate_index → canonical_index}  (0-based, canonical = the one to keep).
    """
    if not AI_AVAILABLE or len(titles) < 2:
        return {}

    numbered = "\n".join(f"{i}. {t}" for i, t in enumerate(titles))

    prompt = f"""You are a strict deduplication assistant for a Hong Kong virtual bank promotions database.
Bank: {bank_name}

Your task: Find titles that describe THE SAME underlying product or promotion.
When genuinely uncertain → mark as DUPLICATE. It is always better to merge than to leave duplicates.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SYNONYM RULES — these are ALWAYS the same promotion:

Product synonyms:
  "余額+"  =  "Deposit Plus"  =  "Balance+"
  Any title with "余額+", "Deposit Plus", or "Balance+" → same product

Fee synonyms:
  "Zero Fee" = "0% Fee" = "Fee Waiver" = "No Fee" = "Commission-Free"

Speed synonyms:
  "Quick" = "Fast" = "Instant" = "Express" = "Immediate"

Crypto synonyms:
  "Crypto Trading Fee Waiver" = "0% Crypto Platform Fee" = "Zero Fee Cryptocurrency Trading"
  = "Digital Asset Trading Fee Exemption"

Fund subscription synonyms:
  "Zero Subscription Fee on All Funds" = "Zero-Fee Fund Subscription & Switching"
  = "Featured Funds with Zero Subscription Fees" = "Zero Fee Investment Funds"

Mox × csl:
  "Best-in-Town Plan Offer" = "Best-in-Town Device Plans with Instalments" = anything + "Best-in-Town"

Trip.com:
  "Trip.com Annual Discount" = "Trip.com x Mox Credit Card Year-Round Promotion"
  = "Trip.com Year-Round Exclusive Discount"

Payroll:
  "Payroll Switching Benefits" = "Payroll Switch Benefits" = "Payroll Deposit Benefit"

SWIFT / Payment Connect:
  "Zero Fee SWIFT Transfers" = "Zero-Fee Payment Connect" = "Payment Connect Zero Fee Transfers"

WeLab Global Wallet FX:
  "WeLab Global Wallet Exchange Rate Promotion" = "WeLab Global Wallet - Best Exchange Rates"
  = "Global Remittance Service" = "WeLab Global Wallet Best FX Rates"

Promo codes: if two titles share the same promo code (ignoring trailing year digits),
  e.g. MOXBILL25 and MOXBILL26 → SAME campaign. MOXHKT25 in both titles → SAME.

Account opening:
  "Quick Account Opening" = "Account Opening in 3 Minutes" = "Mobile Account Opening in 5 Minutes"

24/7 banking:
  "24/7 Mobile Banking Services" = "24/7 Digital Banking Services" = "24×7 Banking Services"

Insurance with rate:
  "3.6% Annualized Rate Promotion" = "Insurance Products with Annual Rate up to 3.6%"
  = "Insurance Products with 3.6% Annual Rate" = "Insurance Products with Premium Rebate"

GoSave:
  "GoSave 2.0 High Interest Savings" = "GoSave 2.0 Enhanced Savings"

liviSave:
  "liviSave Preferential Interest Rate" = "liviSave Preferential Savings Rate"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return ONLY valid compact JSON — no markdown, no code fences, no explanation:
{{"groups":[{{"keep_index":0,"duplicate_indices":[1,2],"reason":"one sentence"}}]}}
If there are NO duplicates, return exactly: {{"groups":[]}}

Titles to evaluate (0-indexed):
{numbered}"""

    try:
        raw = _call([{'role': 'user', 'content': prompt}])
        if not raw:
            return {}
        raw = re.sub(r'^```[a-z]*\n?', '', raw.strip())
        raw = re.sub(r'\n?```$',       '', raw.strip())
        data = json.loads(raw)
        dup_map = {
            int(dup): int(g['keep_index'])
            for g in data.get('groups', [])
            for dup in g.get('duplicate_indices', [])
        }
        if dup_map:
            print(f'  🤖 ai_dedup_titles [{bank_name}]: {len(dup_map)} duplicate(s) flagged')
        return dup_map
    except Exception as exc:
        print(f'  ⚠️  ai_dedup_titles [{bank_name}]: {exc!r} — skipping AI pass')
        return {}


def ai_match_against_existing(
    new_promos:      list[dict],
    existing_promos: list[dict],
    bank_name:       str,
) -> dict[int, int]:
    """
    Compare freshly-scraped promos against promos already in the DB.
    Returns {new_promo_index: existing_db_id} for every semantic match.
    """
    if not AI_AVAILABLE or not new_promos or not existing_promos:
        return {}

    new_lines = '\n'.join(
        f'[NEW-{i}] {(p.get("name") or p.get("title") or "").strip()}'
        for i, p in enumerate(new_promos)
    )
    ex_lines = '\n'.join(
        f'[DB-{p["id"]}] {(p.get("title") or "").strip()}'
        for p in existing_promos
    )

    prompt = f"""You are a strict deduplication assistant for a Hong Kong virtual bank promotions database.
Bank: {bank_name}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MATCHING RULES — mark as MATCH in all these cases:

1. Product name synonyms:
   "余額+" = "Deposit Plus" = "Balance+" → always the same product
   Any two titles that BOTH reference one of these → MATCH

2. Fee synonyms:
   "Zero Fee" = "0% Fee" = "Fee Waiver" = "No Fee" = "Commission-Free"

3. Promo codes:
   If both titles contain the same promo code (ignoring trailing 2-digit year suffix)
   e.g. MOXBILL25 ↔ MOXBILL26 → MATCH (same recurring campaign)
   e.g. MOXHKT25 ↔ MOXHKT25  → MATCH

4. Crypto fee promotions:
   Any title about crypto + fee waiver/removal → MATCH each other

5. Best-in-Town:
   Any title containing "Best-in-Town" for the same bank → MATCH

6. Trip.com:
   "Trip.com Annual Discount" ↔ "Trip.com x Mox Credit Card Year-Round Promotion" → MATCH

7. SWIFT / Payment Connect:
   "SWIFT Transfers" ↔ "Payment Connect" → MATCH (same ZA Bank product)

8. WeLab Global Wallet FX:
   Any WeLab Global Wallet + FX/exchange/remittance title → MATCH

9. Payroll switch:
   Any payroll + switch/deposit/benefit → MATCH

10. When uncertain → declare MATCH (prevents duplicate rows)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

NEWLY SCRAPED (this run):
{new_lines}

ALREADY IN DATABASE:
{ex_lines}

For each [NEW-N] that matches a [DB-ID], output that pair.
Respond ONLY with compact JSON. Key = new index (string). Value = DB id (string).
Example: {{"0": "47", "3": "112"}}
If no matches: {{}}
No explanation. No markdown. No code fences."""

    try:
        raw = _call([{'role': 'user', 'content': prompt}])
        if not raw:
            return {}
        raw = re.sub(r'^```[a-z]*\n?', '', raw.strip())
        raw = re.sub(r'\n?```$',       '', raw.strip())
        data = json.loads(raw)
        result: dict[int, int] = {
            int(k): int(v)
            for k, v in data.items()
            if str(k).isdigit() and str(v).isdigit()
        }
        msg = f'{len(result)} match(es)' if result else '0 matches — all appear genuinely new'
        print(f'  🤖 ai_match_against_existing [{bank_name}]: {msg}')
        return result
    except Exception as exc:
        print(f'  ⚠️  ai_match_against_existing [{bank_name}]: {exc!r} — skipping')
        return {}


def generate_strategic_insights(promotions_by_bank: dict) -> dict | None:
    if not AI_AVAILABLE:
        print('⚠️  AI not available — skipping strategic insights')
        return None

    bank_summaries = []
    for bank_name, promos in sorted(promotions_by_bank.items()):
        if not promos:
            continue
        lines = []
        for p in promos:
            title     = (p.get('name') or p.get('title') or 'N/A')[:80]
            highlight = (p.get('highlight') or p.get('description') or '')[:120]
            period    = (p.get('period') or 'Ongoing')[:60]
            raw_types = p.get('types') or 'General'
            ptype     = (', '.join(raw_types) if isinstance(raw_types, list)
                         else str(raw_types))[:40]
            lines.append(f'  [{ptype}] {title}: {highlight} | {period}')
        bank_summaries.append(f'## {bank_name} ({len(promos)} active)\n' + '\n'.join(lines))

    if not bank_summaries:
        print('⚠️  No promotions data — skipping strategic insights')
        return None

    promotions_text = '\n\n'.join(bank_summaries)

    prompt = f"""You are a Hong Kong virtual bank analyst. \
Analyze these active promotions and return strategic insights as JSON.

{promotions_text}

Return this EXACT JSON structure (no markdown, no code fences):
{{
  "best_for": [
    {{"category": "Investment",        "bank": "BankName", "detail": "specific detail with numbers"}},
    {{"category": "Spending/CashBack", "bank": "BankName", "detail": "specific % or HKD amount"}},
    {{"category": "Welcome Bonus",     "bank": "BankName", "detail": "HKD amount"}},
    {{"category": "Travel",            "bank": "BankName", "detail": "specific benefit"}},
    {{"category": "Loan APR",          "bank": "BankName", "detail": "X.XX% APR"}},
    {{"category": "FX/Multi-Currency", "bank": "BankName", "detail": "specific detail"}},
    {{"category": "Fund Investment",   "bank": "BankName", "detail": "specific detail"}},
    {{"category": "Referral Bonus",    "bank": "BankName", "detail": "HKD amount"}}
  ],
  "bank_analysis": {{
    "ZA Bank": {{
      "focus": "short keywords",
      "strengths": ["s1", "s2", "s3"],
      "expiring_alert": "",
      "vs_za_pros": null,
      "vs_za_cons": null
    }},
    "OtherBank": {{
      "focus": "keywords",
      "strengths": ["s1", "s2", "s3"],
      "expiring_alert": "",
      "vs_za_pros": "pros vs ZA Bank",
      "vs_za_cons": "cons vs ZA Bank"
    }}
  }}
}}"""

    raw = _call([{'role': 'user', 'content': prompt}])
    if not raw:
        print('❌ Strategic insights: empty response from AI')
        return None

    result = _parse_object(raw)
    if result is None:
        print('❌ Strategic insights: JSON parse failed')
        return None

    name_lookup = {k.lower(): k for k in promotions_by_bank}
    for bname in result.get('bank_analysis', {}):
        matched_key = name_lookup.get(bname.lower())
        result['bank_analysis'][bname]['count'] = (
            len(promotions_by_bank[matched_key]) if matched_key else 0
        )

    print(f'✅ Strategic insights generated via {_bot_name}')
    return result