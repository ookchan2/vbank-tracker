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

# ← hardcoded BAU overrides — always flag regardless of AI classification
BAU_OVERRIDES: dict[str, list[str]] = {
    "za": [
        "new crypto customer fee waiver",
    ],
}

# ── Extraction prompt ─────────────────────────────────────────────────────────
# FIX 1a: Added Rule 7 — footnotes are real promotions, always extract them
# FIX 1b: Added Rule 8 — nav/menu/footer items are NOT promotions, never extract

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
║       ✅ BAU: "$0 Fund Subscription Fee Mode" (WeLab, no end date)  ║
║       ❌ NOT BAU: "New Customer Bonus" (new customers only)         ║
║       ❌ NOT BAU: "Limited-Time Fee Waiver" (has end date)          ║
║       ❌ NOT BAU: Any promotion with a promo code                   ║
║                                                                      ║
║  6. CATEGORY TAGGING RULES:                                         ║
║     • Any referral / invite-a-friend / 推薦 program → tag 推薦     ║
║     • Any fund / 基金 / unit trust subscription fee promo → 投資   ║
║     • Any stock / crypto / securities trading fee promo → 投資     ║
║                                                                      ║
║  7. ⚠️  FOOTNOTES ARE REAL PROMOTIONS — ALWAYS EXTRACT THEM        ║
║     Lines starting with  *  †  #  ¹  ²  are often the most         ║
║     important promotion terms, NOT just legal disclaimers.          ║
║     REQUIRED: scan EVERY footnote line and ask yourself:            ║
║       "Does this mention a fee waiver, discount, reward, or         ║
║        eligibility period?" → If YES, extract it as a promotion.   ║
║                                                                      ║
║     REAL EXAMPLE you must not miss:                                 ║
║       "*From now until 31 Jul 2026 ... retail banking users who    ║
║        have activated investment fund trading services with ZA Bank ║
║        can enjoy 0% fund subscription fee offer and redemption fee  ║
║        waivers for all funds."                                      ║
║       → Extract as: name="ZA Bank 0% Fund Subscription Fee Offer   ║
║         until 31 Jul 2026", types=["投資"], is_bau=false,          ║
║         end_date="2026-07-31"                                       ║
║                                                                      ║
║  8. ⛔ DO NOT EXTRACT THESE — they are NOT promotions:             ║
║     • Navigation / menu items                                       ║
║       (e.g. "Travel Offers", "Download App", "Get an Account",     ║
║        "Help Center", "About Us", "Travel with ZA Card" as menu)   ║
║     • Section headings without a concrete benefit amount            ║
║     • Pure risk disclaimers / legal boilerplate                     ║
║     • Generic product feature names with no specific reward         ║
║     • Footer links (Terms, Privacy Policy, Contact Us, etc.)       ║
║                                                                      ║
║     ❌ BAD extraction (nav item): "Travel with ZA Card"            ║
║     ✅ GOOD extraction (real deal): "Trip.com 8% off + 2% CashBack"║
╚══════════════════════════════════════════════════════════════════════╝

ALLOWED CATEGORY TAGS (Chinese, pick 1-3 per promotion):
  迎新 / 消費 / 投資 / 旅遊 / 保險 / 貸款 / 存款 / 外匯 / 推薦 / 新資金 / Others

REQUIRED OUTPUT: A valid JSON array — NO other text, NO markdown fences.

Schema for each object:
{{
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
}}

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


# ── FIX 3 + FIX 4a: Evidence gate constants ──────────────────────────────────
#
# FIX 3  (previous): basic evidence gate — rejects vague/hallucinated details
# FIX 4a (NEW):      broadened _CONCRETE_EVIDENCE_RE to recognise patterns the
#                    old regex missed, causing valid Investment / FX winners to
#                    be incorrectly rejected:
#
#   OLD gap: "$0 platform fee"   → no match  (only HKD / % / date were checked)
#   NEW fix: "\$\s*0\b"         → matches "$0"
#            "platform\s*fee"   → matches "platform fee"
#            "trading\s*fee"    → matches "trading fee"
#            "fee\s*waiver"     → matches "fee waiver"
#            "zero[\s-]fee"     → matches "zero fee" / "zero-fee"
#            "free\s+stock"     → matches "free stocks" (PowerDraw etc.)
#            "payment\s+connect"→ matches ZA Bank's named FX product
#            "global\s+wallet"  → matches WeLab / ZA named FX product

_VAGUE_DETAIL_PATTERNS: list[str] = [
    r'special\s+\w+[-\s]related\s+promotions?',
    r'year[- ]round\s+\w+\s+offers?\s+with\s+special',
    r'^\s*various\b',
    r'competitive\s+features',
    r'\bservices?\s+available\s*$',
    r'no\s+\w+\s+promotions?\s+available',
]

# FIX 4a — broadened concrete-evidence regex
_CONCRETE_EVIDENCE_RE = re.compile(
    r'HKD\s*[\d,]+'                    # HKD amount         e.g. HKD300, HKD8,888
    r'|\$\s*0\b'                       # $0 anything        e.g. $0 platform fee
    r'|\d+\.?\d*\s*%'                  # percentage         e.g. 0%, 1.18%, 20%
    r'|\d{1,2}\s+[A-Za-z]+\s+20\d\d'  # named date         e.g. 31 Jul 2026
    r'|20\d\d-\d\d-\d\d'              # ISO date           e.g. 2026-07-31
    r'|trip\.com'                      # named partner
    r'|asia\s*miles'                   # Asia Miles
    r'|\bapr\b'                        # APR rate
    r'|subscription\s*fee'             # fund subscription fee
    r'|platform\s*fee'                 # NEW: e.g. $0 platform fee (crypto)
    r'|trading\s*fee'                  # NEW: e.g. $0 trading fee
    r'|fee\s*waiver'                   # NEW: e.g. fee waiver
    r'|zero[\s-]fee'                   # NEW: zero fee / zero-fee
    r'|free\s+stock'                   # NEW: free stocks (PowerDraw, card reward)
    r'|payment\s+connect'              # NEW: ZA Bank named FX product
    r'|global\s+wallet'                # NEW: WeLab / ZA named FX product
    r'|commission'                     # commission waiver
    r'|cashback|cash\s*back',          # cashback reward
    re.IGNORECASE,
)

# ── FIX 4b: Category → keywords for cross-check ──────────────────────────────
#
# Used by _cross_check_best_for_from_strengths() to scan bank_analysis.strengths
# when the LLM correctly identified a strength but still wrote "None" in best_for.
# Each list covers all realistic surface forms the LLM might write.

_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    'Investment (Stock/Crypto Trading)': [
        'crypto', 'bitcoin', 'virtual asset', 'digital asset',
        'stock', 'securities', 'brokerage', 'ipo',
        'trading fee', 'platform fee', '$0', 'commission',
        'powerdraw', 'free stock',
    ],
    'Fund Investment': [
        'fund', '基金', 'mutual fund', 'unit trust',
        'subscription fee', '$0認購費', '認購費', '轉換費',
        'fund fee', 'zero-fee fund', '0% fund', '$0 fund',
        'fund subscription', 'fund trading fee',
    ],
    'Referral Bonus': [
        'referral', '推薦', 'invite', '多友多賞',
        'refer a friend', 'referral code', '推薦碼',
        'referral reward', 'invite a friend', 'invite bonus',
    ],
    'FX/Multi-Currency': [
        'fx', 'forex', 'exchange rate', 'multi-currency',
        'global wallet', 'payment connect', 'remittance',
        'international transfer', 'swift', 'foreign currency',
        'welab global', 'fps transfer',
    ],
    'Travel': [
        'trip.com', 'asia miles', 'flight', 'hotel',
        'travel insurance', 'lounge', 'agoda',
        'booking.com', 'travel cashback', 'travel reward',
    ],
    'Spending/CashBack': [
        'cashback', 'cash back', 'spending reward',
        'merchant', 'card reward', 'rebate', 'card spending',
    ],
    'Welcome Bonus': [
        'welcome', 'new customer', 'account opening',
        'sign up', 'onboarding', 'welcome gift',
        'hkd8,888', 'hkd888', 'join bonus',
    ],
    'Loan APR': [
        'loan', 'apr', 'instant loan', 'personal loan',
        'interest rate', '1.18%', 'tax loan', 'tax season',
    ],
}


def _validate_best_for_evidence(best_for: list) -> list:
    """
    FIX 3: Post-LLM safety guard.

    Rejects any best_for winner whose "detail" field:
      (a) matches a known vague/hallucinated pattern, OR
      (b) contains no concrete verifiable fact

    Sets bank → "None" for rejected entries and logs every rejection.
    Already-None entries pass through untouched.
    """
    validated    = []
    reject_count = 0

    for entry in best_for:
        detail = (entry.get('detail')   or '').strip()
        bank   = (entry.get('bank')     or '').strip()
        cat    = (entry.get('category') or '').strip()

        # Already None / empty — pass through unchanged
        if bank.lower() in ('none', '', 'n/a'):
            validated.append(entry)
            continue

        is_vague     = any(
            re.search(pat, detail, re.IGNORECASE)
            for pat in _VAGUE_DETAIL_PATTERNS
        )
        has_evidence = bool(_CONCRETE_EVIDENCE_RE.search(detail))

        if is_vague or not has_evidence:
            reason = 'vague pattern matched' if is_vague else 'no concrete fact found'
            print(
                f'  🚫 Evidence gate REJECTED [{cat}] winner "{bank}" '
                f'({reason}) → detail was: "{detail}"'
            )
            validated.append({
                **entry,
                'bank':   'None',
                'detail': f'No verified {cat} promotion with concrete details found',
                'is_bau': False,
            })
            reject_count += 1
        else:
            validated.append(entry)

    if reject_count:
        print(f'  🚫 Evidence gate total: {reject_count} vague winner(s) nullified')

    return validated


def _cross_check_best_for_from_strengths(
    result:             dict,
    promotions_by_bank: dict,
) -> dict:
    """
    FIX 4b — Consistency reconciliation pass.

    Closes the disconnect between bank_analysis and best_for:

      SYMPTOM: LLM writes "$0 crypto platform fee" as a ZA Bank strength,
               but best_for Investment slot still shows "None".

      ROOT CAUSE: The LLM generates both sections in one pass but does not
                  reliably cross-reference them.  The evidence gate may also
                  have rejected a valid winner because the detail lacked a
                  recognised concrete-fact pattern (fixed by FIX 4a, but
                  this function is a belt-and-suspenders safety net).

    Algorithm:
      1. Find every best_for slot still showing bank = "None"
      2. Scan ALL banks' bank_analysis.strengths for keywords matching
         that category (via _CATEGORY_KEYWORDS)
      3. Prefer the first candidate whose strength text passes
         _CONCRETE_EVIDENCE_RE; otherwise use the first keyword match
      4. Fill in the slot, log it, and infer is_bau from promo data

    Called AFTER _validate_best_for_evidence() — only fills genuine gaps,
    never overrides a valid winner.
    """
    best_for      = result.get('best_for', [])
    bank_analysis = result.get('bank_analysis', {})

    if not bank_analysis:
        return result

    filled = 0
    for i, entry in enumerate(best_for):
        cat  = (entry.get('category') or '').strip()
        bank = (entry.get('bank')     or '').strip()

        # Only process None / empty slots
        if bank.lower() not in ('none', '', 'n/a'):
            continue

        keywords = _CATEGORY_KEYWORDS.get(cat, [])
        if not keywords:
            continue

        # ── Scan each bank's strengths list for a keyword match ───────────
        candidates: list[tuple[str, str]] = []   # (bank_name, strength_text)
        for bname, bdata in bank_analysis.items():
            strengths: list = bdata.get('strengths') or []
            for s in strengths:
                s_lower = s.lower()
                if any(kw.lower() in s_lower for kw in keywords):
                    candidates.append((bname, s))

        if not candidates:
            continue   # genuinely no evidence — leave as None

        # Prefer candidate with concrete evidence; fall back to first match
        best = next(
            (c for c in candidates if _CONCRETE_EVIDENCE_RE.search(c[1])),
            candidates[0],
        )
        best_bank, best_detail = best

        # Infer is_bau from the actual promotion objects
        bank_promos  = promotions_by_bank.get(best_bank, [])
        is_bau_guess = any(
            p.get('is_bau') and
            any(
                kw.lower() in (p.get('name') or p.get('title') or '').lower()
                for kw in keywords
            )
            for p in bank_promos
        )

        print(
            f'  🔁 Strength cross-check FILLED [{cat}] → {best_bank}: '
            f'"{best_detail[:80]}"'
        )
        best_for[i] = {
            **entry,
            'bank':   best_bank,
            'detail': best_detail,
            'is_bau': is_bau_guess,
        }
        filled += 1

    if filled:
        print(
            f'  🔁 Cross-check total: {filled} slot(s) filled from '
            f'bank_analysis.strengths'
        )

    result['best_for'] = best_for
    return result


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
    results = _apply_bau_overrides(results, bank_id)
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
  = "$0 Fund Trading Fee Mode" = "$0基金買賣收費" = "Zero Fund Subscription Fee"
  = "0% Fund Subscription Fee"

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

Referral programs:
  "Referral Bonus" = "Invite a Friend" = "多友多賞" = "推薦計劃" = "Friend Referral Program"
  = "Refer a Friend" = any title with "推薦碼" or "referral code" + HKD amount

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

10. Fund zero-fee:
    "$0 Fund Trading Fee" ↔ "Zero Fund Subscription Fee" ↔ "0% Fund Subscription Fee"
    ↔ "$0基金買賣" ↔ "Zero-Fee Fund Subscription & Switching" → MATCH (same campaign)

11. Referral programs:
    "多友多賞" ↔ "Mox Referral Programme" ↔ "Refer a Friend HKD300" → MATCH
    Any two titles referencing the same bank's referral/invite-a-friend program → MATCH

12. When uncertain → declare MATCH (prevents duplicate rows)
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


# ── Strategic insights helpers ────────────────────────────────────────────────

def _build_bank_summary_lines(promos: list) -> list[str]:
    """
    Convert a list of promotions into summary lines for the insights prompt.
    Each line is tagged with BAU status and types so the AI can cross-reference
    Chinese type tags (推薦, 投資 …) against English best_for categories.
    """
    lines = []
    for p in promos:
        title     = (p.get('name') or p.get('title') or 'N/A')[:80]
        highlight = (p.get('highlight') or p.get('description') or '')[:120]
        period    = (p.get('period') or 'Ongoing')[:60]
        raw_types = p.get('types') or ['General']
        ptype     = (', '.join(raw_types) if isinstance(raw_types, list)
                     else str(raw_types))[:40]
        bau_tag   = ' [BAU - Permanent Feature]' if p.get('is_bau') else ''
        lines.append(f'  [{ptype}]{bau_tag} {title}: {highlight} | {period}')
    return lines


# ── FIX A: Input diagnostic ───────────────────────────────────────────────────
#
# Runs FIRST inside generate_strategic_insights() before the LLM is ever called.
# Prints a full breakdown of what each bank contributed to the input dict, and
# runs a category-coverage check so you can immediately see WHICH categories
# will output "None" and WHY (no data vs. data present but LLM missed it).

# Categories the diagnostic checks for coverage, with keyword hints
_DIAGNOSTIC_CATEGORIES: list[tuple[str, list[str]]] = [
    ('Investment (Stock/Crypto Trading)', ['投資', 'crypto', 'stock', '$0', 'commission']),
    ('Fund Investment',                   ['投資', 'fund', '基金', '$0認購費', 'subscription fee']),
    ('Referral Bonus',                    ['推薦', 'referral', '多友多賞', 'invite']),
    ('Travel',                            ['旅遊', 'trip', 'travel', 'asia miles']),
    ('Spending/CashBack',                 ['消費', 'cashback', 'cash back', 'rebate']),
    ('Welcome Bonus',                     ['迎新', 'welcome', 'new customer']),
    ('Loan APR',                          ['貸款', 'loan', 'apr']),
    ('FX/Multi-Currency',                 ['外匯', 'fx', 'multi-currency', 'global wallet']),
]


def _diagnose_input_data(promotions_by_bank: dict) -> dict[str, list[str]]:
    """
    FIX A — Input diagnostic.

    Logs a detailed breakdown of what generate_strategic_insights() received
    BEFORE the LLM is called.  Returns a dict of bank → list of type/keyword
    tags found, for any downstream use.

    Printed output shows:
      • Per-bank: active count, BAU count, type tags found
      • ⚠️  SPARSE warning if a bank has < _SPARSE_THRESHOLD promos
      • Per-category: which banks cover it, or ❌ NO DATA (will → None)

    This is the FIRST thing to check when a category shows "None":
      – Is any bank passing data for that category at all?
      – Is the relevant bank being filtered out before this function is called?
    """
    print()
    print('=' * 70)
    print('📊  INSIGHTS INPUT DIAGNOSTIC')
    print('=' * 70)

    bank_tag_map: dict[str, list[str]] = {}

    for bank, promos in sorted(promotions_by_bank.items()):
        bau_promos     = [p for p in promos if p.get('is_bau')]
        non_bau_promos = [p for p in promos if not p.get('is_bau')]

        # Collect all type tags + keywords from text fields for display
        all_tags: set[str] = set()
        for p in promos:
            raw = p.get('types') or []
            tags = raw if isinstance(raw, list) else [str(raw)]
            all_tags.update(tags)
            for field in ('name', 'title', 'highlight', 'description'):
                val = (p.get(field) or '').lower()
                if val:
                    all_tags.add(val[:40])   # truncate long free-text

        # Keep only short, meaningful tags for the summary line
        tag_display = ', '.join(
            t for t in sorted(all_tags)
            if 1 < len(t) <= 12 and t not in ('', 'others', 'general')
        ) or '⚠️  NONE'

        sparse_flag = (
            '  ⚠️  SPARSE — may cause None slots'
            if len(promos) < _SPARSE_THRESHOLD
            else '  ✅'
        )
        print(
            f'  📊 {bank:<20}: {len(non_bau_promos):>2} active'
            f' + {len(bau_promos):>2} BAU'
            f' = {len(promos):>2} total'
            f'  | tags: {tag_display[:55]}'
            f'{sparse_flag}'
        )
        bank_tag_map[bank] = list(all_tags)

    # ── Category coverage check ───────────────────────────────────────────
    print()
    print('  CATEGORY COVERAGE CHECK:')
    for cat_name, kw_list in _DIAGNOSTIC_CATEGORIES:
        covered_by: list[str] = []
        for bank, promos in promotions_by_bank.items():
            for p in promos:
                # Build a combined searchable text from all fields + types
                types_str = ' '.join(
                    p.get('types') if isinstance(p.get('types'), list)
                    else [str(p.get('types') or '')]
                )
                text = ' '.join([
                    types_str,
                    (p.get('name')        or ''),
                    (p.get('title')       or ''),
                    (p.get('highlight')   or ''),
                    (p.get('description') or ''),
                ]).lower()
                if any(kw.lower() in text for kw in kw_list):
                    covered_by.append(bank)
                    break   # one match per bank is enough

        if covered_by:
            print(f'    ✅ {cat_name:<42} → {", ".join(covered_by)}')
        else:
            print(f'    ❌ {cat_name:<42} → NO DATA — will output None')

    print('=' * 70)
    print()
    return bank_tag_map


# ── FIX B: Sparse-data guard + DB supplement ─────────────────────────────────
#
# If a bank has fewer than _SPARSE_THRESHOLD promotions in the current scrape
# batch, the insights generator will produce degraded / None analysis for it.
# supplement_from_db() lets the caller pass a DB-fetch function as a safety net
# so that sparse banks are automatically topped up from the database.

# Minimum number of promotions a bank must have before the insights generator
# can be expected to produce meaningful analysis for it.
_SPARSE_THRESHOLD = 3


def _check_sparse_banks(promotions_by_bank: dict) -> list[str]:
    """
    FIX B (part 1) — Returns a list of bank names that have fewer than
    _SPARSE_THRESHOLD promotions.  Logs a warning for each sparse bank.

    The caller should pass these banks' data through supplement_from_db()
    before calling generate_strategic_insights().
    """
    sparse = [
        bank for bank, promos in promotions_by_bank.items()
        if len(promos) < _SPARSE_THRESHOLD
    ]
    if sparse:
        print(
            f'  ⚠️  SPARSE BANKS DETECTED: {sparse}\n'
            f'     Each has < {_SPARSE_THRESHOLD} promotions in the current input.\n'
            f'     Pass db_fetch_fn to generate_strategic_insights() to auto-supplement.'
        )
    return sparse


def supplement_from_db(
    promotions_by_bank:  dict,
    db_fetch_fn,          # Callable[[str], list[dict]]
    min_promos_per_bank: int = _SPARSE_THRESHOLD,
) -> dict:
    """
    FIX B (part 2) — Sparse-data supplement.

    If a bank has fewer than min_promos_per_bank promotions in the current
    scrape batch, pull the rest from the DB so the insights generator always
    gets a full picture.

    Args:
        promotions_by_bank:  Dict produced by the scraper (may be sparse).
        db_fetch_fn:         Callable: db_fetch_fn(bank_name: str) → list[dict]
                             Should return ALL active + BAU promotions for that
                             bank from the database.
        min_promos_per_bank: Supplement any bank below this count.

    Returns:
        Updated promotions_by_bank with DB rows merged in for sparse banks.

    Typical usage inside the scheduler:

        def fetch_from_db(bank_name: str) -> list[dict]:
            from models import Promotion
            from datetime import date
            today = date.today()
            rows = Promotion.query.filter(
                Promotion.bank_name == bank_name,
                Promotion.is_hidden == False,
                db.or_(
                    Promotion.is_bau == True,
                    Promotion.end_date >= today,
                    Promotion.end_date == None,
                )
            ).all()
            return [r.to_dict() for r in rows]

        enriched = supplement_from_db(promotions_by_bank, fetch_from_db)
        insights = generate_strategic_insights(enriched)
    """
    supplemented_total = 0

    for bank, promos in promotions_by_bank.items():
        if len(promos) >= min_promos_per_bank:
            continue

        try:
            db_promos = db_fetch_fn(bank)
        except Exception as exc:
            print(f'  ⚠️  supplement_from_db: DB fetch failed for "{bank}": {exc}')
            continue

        if not db_promos:
            print(f'  ⚠️  supplement_from_db: no DB rows found for "{bank}"')
            continue

        # De-duplicate: only add DB rows whose normalised title isn't already
        # present in the current batch
        existing_titles = {
            (p.get('name') or p.get('title') or '').strip().lower()
            for p in promos
        }
        added = 0
        for dp in db_promos:
            dt = (dp.get('name') or dp.get('title') or '').strip().lower()
            if dt and dt not in existing_titles:
                promos.append(dp)
                existing_titles.add(dt)
                added += 1

        promotions_by_bank[bank] = promos
        supplemented_total += added

        if added:
            print(
                f'  🔄 supplement_from_db: "{bank}" was sparse '
                f'({len(promos) - added} promo(s)) → added {added} from DB '
                f'→ now {len(promos)} total'
            )
        else:
            print(
                f'  🔄 supplement_from_db: "{bank}" still sparse after DB check '
                f'(DB had no new titles to add)'
            )

    if supplemented_total:
        print(f'  🔄 supplement_from_db: {supplemented_total} DB row(s) merged in total')

    return promotions_by_bank


# ── Strategic insights — main entry point ────────────────────────────────────

def generate_strategic_insights(
    promotions_by_bank: dict,
    db_fetch_fn=None,    # Optional[Callable[[str], list[dict]]]  — see FIX B
) -> dict | None:
    """
    Generate strategic insights from ALL promotions including BAU.

    Args:
        promotions_by_bank:
            Dict of bank_name → list of ALL promos (BAU + time-limited).
            *** MUST include BAU items. ***
            If you filter to is_bau=False before calling this function,
            ZA Bank's crypto / stock / fund strengths will disappear from
            the analysis and the bank_analysis focus will degrade (e.g. to
            "gamification" instead of "investment and crypto").

        db_fetch_fn:
            Optional callable used by FIX B to supplement sparse banks.
            Signature: db_fetch_fn(bank_name: str) → list[dict]
            Pass None (default) to skip the DB supplement step entirely.

    Processing pipeline:
        FIX A  → _diagnose_input_data()             — log input before LLM
        FIX B  → _check_sparse_banks()              — detect sparse banks
                  supplement_from_db()              — fill from DB if needed
        Prompt → build + call LLM
        FIX 3  → _validate_best_for_evidence()      — evidence gate
        FIX 4b → _cross_check_best_for_from_strengths() — fill None slots
        Post   → attach counts, log summary

    Returns:
        Parsed dict with keys "best_for" and "bank_analysis", or None on error.
    """
    if not AI_AVAILABLE:
        print('⚠️  AI not available — skipping strategic insights')
        return None

    # ── FIX A: Diagnose input BEFORE the LLM is ever called ──────────────
    _diagnose_input_data(promotions_by_bank)

    # ── FIX B: Detect and optionally supplement sparse banks ─────────────
    sparse_banks = _check_sparse_banks(promotions_by_bank)
    if sparse_banks:
        if db_fetch_fn is not None:
            promotions_by_bank = supplement_from_db(promotions_by_bank, db_fetch_fn)
            # Re-run diagnostic so you can see the before/after difference
            print('  📊 POST-SUPPLEMENT DIAGNOSTIC:')
            _diagnose_input_data(promotions_by_bank)
        else:
            print(
                '  ⚠️  Sparse banks found but no db_fetch_fn was provided.\n'
                '     To auto-supplement, pass db_fetch_fn=your_db_query_fn\n'
                '     to generate_strategic_insights().'
            )

    # ── Build per-bank summary lines for the prompt ───────────────────────
    bank_summaries = []
    for bank_name, promos in sorted(promotions_by_bank.items()):
        if not promos:
            continue
        non_bau_count = sum(1 for p in promos if not p.get('is_bau'))
        bau_count     = len(promos) - non_bau_count
        lines         = _build_bank_summary_lines(promos)
        bank_summaries.append(
            f'## {bank_name} ({non_bau_count} time-limited promos'
            f' + {bau_count} BAU permanent features)\n' + '\n'.join(lines)
        )

    if not bank_summaries:
        print('⚠️  No promotions data — skipping strategic insights')
        return None

    promotions_text = '\n\n'.join(bank_summaries)

    # ── Build the prompt (Sections 1-7 all present) ───────────────────────
    prompt = f"""You are a Hong Kong virtual bank analyst. \
Analyze these active promotions and return strategic insights as JSON.

{promotions_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 1 — BAU ITEMS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Items tagged [BAU - Permanent Feature] are ALWAYS-AVAILABLE with no expiry.
You MUST include BAU items when evaluating "best_for" category winners.
A permanent zero-fee or zero-commission feature is often the strongest
competitive advantage — do NOT skip it just because it has no end date.
Treat [BAU - Permanent Feature] items as equally eligible as time-limited
promotions for all "best_for" slots.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 2 — CHINESE TYPE TAG → ENGLISH CATEGORY MAPPING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The promotion lines above use Chinese category tags inside [ ].
Use this mapping to decide which best_for category each promotion belongs to:

  [推薦]           → "Referral Bonus"
                     (referral / invite-a-friend / 推薦碼 programs)
  [投資] + fund    → "Fund Investment"
                     (any mention of 基金, fund subscription, $0認購費,
                      zero fund fee, unit trust, 認購費, 轉換費)
  [投資] + stock   → "Investment (Stock/Crypto Trading)"
  [投資] + crypto  → "Investment (Stock/Crypto Trading)"
  [消費]           → "Spending/CashBack"
  [迎新]           → "Welcome Bonus"
  [旅遊]           → "Travel"
  [貸款]           → "Loan APR"
  [外匯]           → "FX/Multi-Currency"
  [新資金]         → "Welcome Bonus" or "Spending/CashBack" (pick most relevant)

A single promotion may carry multiple tags — evaluate ALL of them.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 3 — STRICT CATEGORY DEFINITIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

• Investment (Stock/Crypto Trading)
    → Pick ONLY promotions about STOCK TRADING or CRYPTO TRADING.
    → Examples: brokerage commission waiver, crypto trading fee waiver,
      stock cashback, securities transfer bonus, IPO subscription reward,
      lifetime $0 commission on stocks, $0 platform fee for crypto.
    → BAU zero-fee crypto/stock trading features qualify — include them.
    → DO NOT pick: time deposit, savings, insurance, or fund promotions here.

• Fund Investment
    → Pick ONLY promotions about MUTUAL FUND or UNIT TRUST
      subscriptions / switching / platform fees.
    → QUALIFYING KEYWORDS (any of these = qualifies):
        "0% subscription fee", "$0 fund", "zero fund subscription",
        "fund fee waiver", "$0認購費", "零認購費", "基金認購費",
        "$0轉換費", "零轉換費", "fund switching", "unit trust fee",
        "$0 fund trading fee", "zero-fee fund", "fund platform fee waiver",
        "WeLab $0 fund", "ZA Bank fund subscription"
    → BAU zero-fee fund features qualify — include them.
    → DO NOT pick: stock brokerage, crypto, or savings promotions here.

• Spending/CashBack
    → Credit/debit card cashback or merchant spending reward promotions.

• Welcome Bonus
    → New customer account opening welcome cash or gift rewards.
    → Must be a concrete HKD amount or tangible reward.

• Travel
    → Travel insurance, flight/hotel discounts, Asia Miles, trip.com promos.

• Loan APR
    → Personal loan or instant loan with the lowest specific APR rate quoted.

• FX/Multi-Currency
    → Foreign exchange rate promotions, global wallet, international remittance.
    → BAU always-on FX features qualify — include them.

• Referral Bonus
    → Referral / invite-a-friend programs with a stated reward amount.
    → QUALIFYING KEYWORDS (any of these = qualifies):
        "referral", "推薦", "invite a friend", "推薦碼", "referral code",
        "多友多賞", "refer a friend", "HKD XXX per referral",
        "referral reward", "推薦獎賞", "invite bonus"
    → ANY promotion tagged [推薦] in its types automatically qualifies here.
    → The winner must have a concrete HKD reward amount stated.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 4 — MANDATORY WINNER SELECTION RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  CRITICAL: You MUST select a winner for EVERY category as long as ANY
promotion in the data could plausibly qualify, including BAU items.

Output "None" ONLY when there is absolutely zero evidence of any promotion
across ALL banks that relates to that category.

CHECKLIST — before writing "None" for any category, verify:

  Fund Investment  →  Search for ANY line containing:
    fund / 基金 / $0認購費 / zero subscription / 0% fund / fund fee
    If found → pick the best one. Do NOT write "None".

  Referral Bonus   →  Search for ANY line tagged [推薦] OR containing:
    referral / 推薦 / invite / 多友多賞 / HKD NNN per referral
    If found → pick the bank with the highest stated HKD reward. Do NOT write "None".

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 5 — KNOWN QUALIFIERS (real examples you must recognise)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
These are real promotions that MUST be correctly classified:

  ✅ Investment (Stock/Crypto Trading) winners:
       ZA Bank   "$0 platform fee for crypto trading" [BAU]
                 → qualifies because "$0 platform fee" is a concrete fact
       ZA Bank   "Free stocks with ZA Card spending"
                 → qualifies because "free stock" is a concrete reward
       ZA Bank   "Free stocks through PowerDraw"
                 → qualifies; PowerDraw = ZA Bank named product

  ✅ Fund Investment winners:
       ZA Bank   "0% Fund Subscription Fee for All Funds until 31 Jul 2026"
                 → qualifies because it has "0% subscription fee" + "funds"
       WeLab Bank "$0 Fund Trading Fee Mode / $0認購費 $0轉換費" [BAU]
                 → qualifies even though BAU; permanent zero-fee fund model

  ✅ Referral Bonus winners:
       Mox       "多友多賞 Referral Programme — HKD300 per successful referral"
                 → qualifies; tagged [推薦]; concrete HKD300 reward stated

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 6 — EVIDENCE GATE: NO HALLUCINATION ALLOWED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The "detail" field for each best_for winner MUST contain at least ONE
concrete verifiable fact from the promotion data listed above.

  ✅ VALID details (contain specific evidence):
       "$0 platform fee for crypto trading (permanent)"
       "Free stocks via PowerDraw + free stocks with ZA Card spending"
       "0% fund subscription fee until 31 Jul 2026"
       "HKD300 per successful referral, no cap"
       "1.18% APR on Tax Season Instant Loan"
       "Up to 8% off + 2% CashBack on Trip.com until 2026-12-31"
       "Zero-fee Payment Connect international transfers"

  ❌ INVALID details (vague, no concrete fact — NEVER use):
       "Year-Round Travel Offers with special travel-related promotions"
       "Various fund investment promotions available"
       "Investment services with competitive features"
       "Competitive FX rates for customers"
       "Travel benefits for cardholders"

RULE: If your detail string does NOT contain at least one of:
  • A specific HKD/USD amount   (e.g. HKD300, HKD8,888)
  • A specific percentage        (e.g. 0%, 1.18%, 8%)
  • A $0 or zero-fee fact        (e.g. $0 platform fee, zero-fee transfer)
  • A specific date              (e.g. until 31 Jul 2026)
  • A named concrete product     (e.g. PowerDraw, Payment Connect, Trip.com)
  • A commission/fee keyword     (e.g. commission, subscription fee, cashback)
→ Set bank to "None" for that category.
  A truthful "None" is ALWAYS better than a hallucinated winner.

TRAVEL category special rule:
  Only pick a Travel winner if the promotion text EXPLICITLY states a
  discount %, cashback %, Asia Miles earn rate, or a named travel partner
  (Trip.com, Agoda, airline name, etc.) with a concrete benefit amount.
  Navigation menu items like "Travel Offers" or "Travel with ZA Card"
  are NOT travel promotions — never use them as evidence for a winner.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 7 — SELF-CONSISTENCY CHECK
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BEFORE writing your final JSON, perform this mandatory cross-check:

  Step 1 — For each bank you list in bank_analysis, read every item in
           its "strengths" array.

  Step 2 — For each strength, identify the matching best_for category
           using the SECTION 2 mapping:
             "crypto / stock / $0 platform fee / commission"  → Investment
             "fund / 基金 / subscription fee / $0認購費"      → Fund Investment
             "referral / 推薦 / 多友多賞 / invite"           → Referral Bonus
             "FX / exchange rate / global wallet / SWIFT"     → FX/Multi-Currency
             "cashback / spending / card reward"              → Spending/CashBack

  Step 3 — If you have written "None" for that best_for category but the
           strength bullet clearly describes that product → CONTRADICTION.
           Resolve it: replace "None" with the bank name and use the
           strength text as the detail.

  ⚠️  EXAMPLE OF THE CONTRADICTION YOU MUST AVOID:
       bank_analysis.ZA Bank.strengths = ["$0 platform fee for crypto trading"]
       best_for Investment = {{"bank": "None", ...}}   ← WRONG

       Correct answer:
       best_for Investment = {{
         "bank": "ZA Bank",
         "detail": "$0 platform fee for crypto trading (permanent BAU)",
         "is_bau": true
       }}

  RULE: It is NEVER acceptable to list a strength AND write None for the
        matching category unless a different bank has a provably better offer.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return this EXACT JSON structure (no markdown, no code fences):
{{
  "best_for": [
    {{"category": "Investment (Stock/Crypto Trading)", "bank": "BankName", "detail": "specific stock/crypto detail — must include $0, %, or named product", "is_bau": false}},
    {{"category": "Spending/CashBack",                "bank": "BankName", "detail": "specific % or HKD amount",                                              "is_bau": false}},
    {{"category": "Welcome Bonus",                    "bank": "BankName", "detail": "HKD amount",                                                            "is_bau": false}},
    {{"category": "Travel",                           "bank": "BankName", "detail": "specific benefit with % or named partner (e.g. Trip.com 8% off)",       "is_bau": false}},
    {{"category": "Loan APR",                         "bank": "BankName", "detail": "X.XX% APR",                                                            "is_bau": false}},
    {{"category": "FX/Multi-Currency",                "bank": "BankName", "detail": "specific detail with named product or %",                               "is_bau": false}},
    {{"category": "Fund Investment",                  "bank": "BankName", "detail": "specific fund subscription detail with 0% or $0",                       "is_bau": false}},
    {{"category": "Referral Bonus",                   "bank": "BankName", "detail": "HKD amount per referral",                                               "is_bau": false}}
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
}}

IMPORTANT for "is_bau" field in best_for entries:
  Set true  if the winning promotion is a [BAU - Permanent Feature].
  Set false if it is a time-limited promotion.

FINAL REMINDER:
  • Complete SECTION 7 self-consistency check before writing final JSON.
  • "Fund Investment" and "Referral Bonus" entries MUST have a real bank name,
    not "None", if any qualifying promotion exists in the data above.
  • Every "detail" field MUST pass the SECTION 6 evidence gate.
  • Re-read the promotion list one more time before finalising your answer."""

    # ── Call the LLM ──────────────────────────────────────────────────────
    raw = _call([{'role': 'user', 'content': prompt}])
    if not raw:
        print('❌ Strategic insights: empty response from AI')
        return None

    result = _parse_object(raw)
    if result is None:
        print('❌ Strategic insights: JSON parse failed')
        return None

    # ── FIX 3: Python evidence gate ───────────────────────────────────────
    result['best_for'] = _validate_best_for_evidence(result.get('best_for', []))

    # ── FIX 4b: Cross-check — fill None slots from bank_analysis.strengths ─
    result = _cross_check_best_for_from_strengths(result, promotions_by_bank)

    # ── Post-process: attach promo counts to bank_analysis ────────────────
    name_lookup = {k.lower(): k for k in promotions_by_bank}
    for bname in result.get('bank_analysis', {}):
        matched_key = name_lookup.get(bname.lower())
        if matched_key:
            all_promos     = promotions_by_bank[matched_key]
            non_bau_promos = [p for p in all_promos if not p.get('is_bau')]
            result['bank_analysis'][bname]['count']     = len(non_bau_promos)
            result['bank_analysis'][bname]['bau_count'] = (
                len(all_promos) - len(non_bau_promos)
            )
        else:
            result['bank_analysis'][bname]['count']     = 0
            result['bank_analysis'][bname]['bau_count'] = 0

    # ── Diagnostic summary log ────────────────────────────────────────────
    bau_wins  = sum(1 for b in result.get('best_for', []) if b.get('is_bau'))
    none_wins = sum(
        1 for b in result.get('best_for', [])
        if (b.get('bank') or '').lower() in ('none', '', 'n/a')
    )
    if none_wins:
        none_cats = [
            b['category'] for b in result.get('best_for', [])
            if (b.get('bank') or '').lower() in ('none', '', 'n/a')
        ]
        print(
            f'  ⚠️  {none_wins} best_for slot(s) still None after all fixes: {none_cats}\n'
            f'     ↳ Check the diagnostic above — these categories had no input data.'
        )

    print(
        f'✅ Strategic insights generated via {_bot_name} '
        f'({bau_wins} BAU winner(s), {none_wins} None slot(s))'
    )
    return result