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

# FIX: added 定期存款 so time-deposit promotions can be classified
ALLOWED_CATEGORIES = [
    "迎新", "消費", "投資", "旅遊", "保險",
    "貸款", "存款", "定期存款", "外匯", "推薦", "新資金", "Others"
]

# ── Bank-specific BAU overrides ───────────────────────────────────────────────
BAU_OVERRIDES: dict[str, list[str]] = {
    "za": [
        "new crypto customer fee waiver",
    ],
}

# ── Global BAU overrides ──────────────────────────────────────────────────────
BAU_GLOBAL_OVERRIDES: list[str] = [
    "account opening in 3 minutes",
    "account opening in 5 minutes",
    "quick account opening",
    "mobile account opening",
    "open account in minutes",
    "open an account in minutes",
    "sign up in the time it takes",
    "open account in the time",
    "24/7 mobile banking",
    "24/7 digital banking",
    "24×7 banking",
]

# ── Extraction prompt ─────────────────────────────────────────────────────────
# NOTE: this is a plain string (no f-prefix).
# FIX: changed {{ / }} → { / } — double-braces appeared literally in the AI
#      prompt since this is NOT an f-string, causing the schema example to
#      show "{{" and "}}" instead of valid JSON delimiters.

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
║       ✅ BAU: "Account Opening in 3 Minutes" (any bank, UX claim)  ║
║       ✅ BAU: "Quick Account Opening" (any bank, UX claim)         ║
║       ✅ BAU: "Mobile Account Opening in 5 Minutes" (UX claim)     ║
║       ✅ BAU: "24/7 Digital Banking Services" (always-on feature)  ║
║       ❌ NOT BAU: "New Customer Bonus" (new customers only)         ║
║       ❌ NOT BAU: "Limited-Time Fee Waiver" (has end date)          ║
║       ❌ NOT BAU: Any promotion with a promo code                   ║
║                                                                      ║
║  6. CATEGORY TAGGING RULES:                                         ║
║     • Any referral / invite-a-friend / 推薦 program → tag 推薦     ║
║     • Any fund / 基金 / unit trust subscription fee promo → 投資   ║
║     • Any stock / crypto / securities trading fee promo → 投資     ║
║     • Any fixed / time deposit promo → tag 定期存款                ║
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
  迎新 / 消費 / 投資 / 旅遊 / 保險 / 貸款 / 存款 / 定期存款 / 外匯 / 推薦 / 新資金 / Others

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
        p['bank']      = bank_id
        p['bName']     = bank_name
        # FIX: also set bank_name key for direct DB/JSON compatibility
        p['bank_name'] = bank_name

        # FIX: setdefault won't override an empty string already set by the AI;
        # use explicit guard so the default URL is used when tc_link/link is falsy.
        if not p.get('link'):    p['link']    = default_url
        if not p.get('tc_link'): p['tc_link'] = default_url

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
    """
    Force-set is_bau=True for promotions matching:
      • BAU_OVERRIDES[bank_id]  — bank-specific patterns
      • BAU_GLOBAL_OVERRIDES    — patterns that apply to ALL banks
    """
    bank_specific = [o.lower() for o in BAU_OVERRIDES.get(bank_id.lower(), [])]
    global_list   = [o.lower() for o in BAU_GLOBAL_OVERRIDES]
    all_overrides = bank_specific + global_list
    if not all_overrides:
        return promos
    for p in promos:
        title = (p.get('name') or p.get('title') or '').lower()
        if any(override in title for override in all_overrides):
            if not p.get('is_bau'):
                p['is_bau'] = True
                print(f'    🔒 BAU override: {p.get("name") or p.get("title")}')
    return promos


# ── Evidence gate constants ───────────────────────────────────────────────────

_VAGUE_DETAIL_PATTERNS: list[str] = [
    r'special\s+\w+[-\s]related\s+promotions?',
    r'year[- ]round\s+\w+\s+offers?\s+with\s+special',
    r'^\s*various\b',
    r'competitive\s+features',
    r'\bservices?\s+available\s*$',
    r'no\s+\w+\s+promotions?\s+available',
]

_CONCRETE_EVIDENCE_RE = re.compile(
    r'HKD\s*[\d,]+'
    r'|\$\s*0\b'
    r'|\d+\.?\d*\s*%'
    r'|\d{1,2}\s+[A-Za-z]+\s+20\d\d'
    r'|20\d\d-\d\d-\d\d'
    r'|trip\.com'
    r'|asia\s*miles'
    r'|\bapr\b'
    r'|subscription\s*fee'
    r'|platform\s*fee'
    r'|trading\s*fee'
    r'|fee\s*waiver'
    r'|zero[\s-]fee'
    r'|free\s+stock'
    r'|payment\s+connect'
    r'|global\s+wallet'
    r'|commission'
    r'|cashback|cash\s*back',
    re.IGNORECASE,
)

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
    # FIX: added Time Deposit category
    'Time Deposit / 定期存款': [
        '定期存款', 'time deposit', 'fixed deposit', 'td rate',
        'term deposit', '存款利率', 'deposit rate',
    ],
    # FIX: added New Funds category for 新資金 promotions
    'New Funds / 新資金': [
        '新資金', 'new fund', 'fresh fund', 'new money',
        'fund transfer bonus', 'fund injection',
    ],
}


def _validate_best_for_evidence(best_for: list) -> list:
    validated    = []
    reject_count = 0

    for entry in best_for:
        detail = (entry.get('detail')   or '').strip()
        bank   = (entry.get('bank')     or '').strip()
        cat    = (entry.get('category') or '').strip()

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
    best_for      = result.get('best_for', [])
    bank_analysis = result.get('bank_analysis', {})

    if not bank_analysis:
        return result

    filled = 0
    for i, entry in enumerate(best_for):
        cat  = (entry.get('category') or '').strip()
        bank = (entry.get('bank')     or '').strip()

        if bank.lower() not in ('none', '', 'n/a'):
            continue

        keywords = _CATEGORY_KEYWORDS.get(cat, [])
        if not keywords:
            continue

        candidates: list[tuple[str, str]] = []
        for bname, bdata in bank_analysis.items():
            strengths: list = bdata.get('strengths') or []
            for s in strengths:
                s_lower = s.lower()
                if any(kw.lower() in s_lower for kw in keywords):
                    candidates.append((bname, s))

        if not candidates:
            continue

        best = next(
            (c for c in candidates if _CONCRETE_EVIDENCE_RE.search(c[1])),
            candidates[0],
        )
        best_bank, best_detail = best

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

    clean   = _trim_text(text.strip() if text else '')
    results: list = []

    if len(clean) >= 200:
        prompt = _build_prompt(bank_name=bank_name, url=default_url, text=clean)

        for attempt in range(2):
            raw    = _call([{'role': 'user', 'content': prompt}])
            parsed = _parse_array(raw)
            if parsed:
                results   = parsed
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

Account opening (all BAU — treat as same feature if they appear twice):
  "Quick Account Opening" = "Account Opening in 3 Minutes" = "Mobile Account Opening in 5 Minutes"
  = "Sign Up in Minutes" = "Open Account Instantly"

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

12. Account opening BAU:
    "Account Opening in 3 Minutes" ↔ "Quick Account Opening" ↔ "Mobile Account Opening in 5 Minutes"
    → MATCH (same BAU feature)

13. When uncertain → declare MATCH (prevents duplicate rows)
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


# FIX: added 定期存款 / Time Deposit entry
_DIAGNOSTIC_CATEGORIES: list[tuple[str, list[str]]] = [
    ('Investment (Stock/Crypto Trading)', ['投資', 'crypto', 'stock', '$0', 'commission']),
    ('Fund Investment',                   ['投資', 'fund', '基金', '$0認購費', 'subscription fee']),
    ('Referral Bonus',                    ['推薦', 'referral', '多友多賞', 'invite']),
    ('Travel',                            ['旅遊', 'trip', 'travel', 'asia miles']),
    ('Spending/CashBack',                 ['消費', 'cashback', 'cash back', 'rebate']),
    ('Welcome Bonus',                     ['迎新', 'welcome', 'new customer']),
    ('Loan APR',                          ['貸款', 'loan', 'apr']),
    ('FX/Multi-Currency',                 ['外匯', 'fx', 'multi-currency', 'global wallet']),
    ('Time Deposit / 定期存款',           ['定期存款', 'time deposit', 'fixed deposit', 'td rate']),
]


def _diagnose_input_data(promotions_by_bank: dict) -> dict[str, list[str]]:
    print()
    print('=' * 70)
    print('📊  INSIGHTS INPUT DIAGNOSTIC')
    print('=' * 70)

    bank_tag_map: dict[str, list[str]] = {}

    for bank, promos in sorted(promotions_by_bank.items()):
        bau_promos     = [p for p in promos if p.get('is_bau')]
        non_bau_promos = [p for p in promos if not p.get('is_bau')]

        all_tags: set[str] = set()
        for p in promos:
            raw = p.get('types') or []
            tags = raw if isinstance(raw, list) else [str(raw)]
            all_tags.update(tags)
            for field in ('name', 'title', 'highlight', 'description'):
                val = (p.get(field) or '').lower()
                if val:
                    all_tags.add(val[:40])

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

    print()
    print('  CATEGORY COVERAGE CHECK:')
    for cat_name, kw_list in _DIAGNOSTIC_CATEGORIES:
        covered_by: list[str] = []
        for bank, promos in promotions_by_bank.items():
            for p in promos:
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
                    break

        if covered_by:
            print(f'    ✅ {cat_name:<42} → {", ".join(covered_by)}')
        else:
            print(f'    ❌ {cat_name:<42} → NO DATA — will output None')

    print('=' * 70)
    print()
    return bank_tag_map


_SPARSE_THRESHOLD = 3


def _check_sparse_banks(promotions_by_bank: dict) -> list[str]:
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
    db_fetch_fn,
    min_promos_per_bank: int = _SPARSE_THRESHOLD,
) -> dict:
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
    db_fetch_fn=None,
) -> dict | None:
    if not AI_AVAILABLE:
        print('⚠️  AI not available — skipping strategic insights')
        return None

    _diagnose_input_data(promotions_by_bank)

    sparse_banks = _check_sparse_banks(promotions_by_bank)
    if sparse_banks:
        if db_fetch_fn is not None:
            promotions_by_bank = supplement_from_db(promotions_by_bank, db_fetch_fn)
            print('  📊 POST-SUPPLEMENT DIAGNOSTIC:')
            _diagnose_input_data(promotions_by_bank)
        else:
            print(
                '  ⚠️  Sparse banks found but no db_fetch_fn was provided.\n'
                '     To auto-supplement, pass db_fetch_fn=your_db_query_fn\n'
                '     to generate_strategic_insights().'
            )

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
  [推薦]           → "Referral Bonus"
  [投資] + fund    → "Fund Investment"
  [投資] + stock   → "Investment (Stock/Crypto Trading)"
  [投資] + crypto  → "Investment (Stock/Crypto Trading)"
  [消費]           → "Spending/CashBack"
  [迎新]           → "Welcome Bonus"
  [旅遊]           → "Travel"
  [貸款]           → "Loan APR"
  [外匯]           → "FX/Multi-Currency"
  [定期存款]       → "Time Deposit / 定期存款"
  [新資金]         → "Welcome Bonus" or "Spending/CashBack" (pick most relevant)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 3 — STRICT CATEGORY DEFINITIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Investment (Stock/Crypto Trading)
    → Stock or crypto trading fee waivers, brokerage commission, IPO rewards.
• Fund Investment         → Fund subscription or switching fee promotions.
• Spending/CashBack       → Card cashback or merchant spending rewards.
• Welcome Bonus           → New customer account opening cash/gift rewards.
• Travel                  → Travel insurance, flight/hotel discounts, Asia Miles.
• Loan APR                → Personal loan with the lowest specific APR quoted.
• FX/Multi-Currency       → FX rate promotions, global wallet, remittance.
• Referral Bonus          → Referral programs with a stated HKD reward amount.
• Time Deposit / 定期存款 → Fixed/time deposit rate promotions.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 4 — MANDATORY WINNER SELECTION RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Output "None" ONLY when there is absolutely zero evidence of any promotion
across ALL banks that relates to that category.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 5 — KNOWN QUALIFIERS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✅ ZA Bank "$0 platform fee for crypto trading" [BAU] → Investment winner
  ✅ ZA Bank "0% Fund Subscription Fee until 31 Jul 2026" → Fund Investment winner
  ✅ WeLab "$0 Fund Trading Fee Mode" [BAU] → Fund Investment winner
  ✅ Mox "多友多賞 HKD300 per referral" → Referral Bonus winner

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 6 — EVIDENCE GATE: NO HALLUCINATION ALLOWED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The "detail" field MUST contain at least ONE concrete verifiable fact:
  • Specific HKD/USD amount, percentage, $0/zero-fee fact, specific date,
    named concrete product, or commission/fee/cashback keyword.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 7 — SELF-CONSISTENCY CHECK
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Before writing final JSON: verify that every "None" in best_for is NOT
contradicted by a matching strength in bank_analysis for the same category.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return this EXACT JSON structure (no markdown, no code fences):
{{
  "best_for": [
    {{"category": "Investment (Stock/Crypto Trading)", "bank": "BankName", "detail": "specific detail", "is_bau": false}},
    {{"category": "Spending/CashBack",                "bank": "BankName", "detail": "specific % or HKD amount", "is_bau": false}},
    {{"category": "Welcome Bonus",                    "bank": "BankName", "detail": "HKD amount", "is_bau": false}},
    {{"category": "Travel",                           "bank": "BankName", "detail": "specific benefit with % or named partner", "is_bau": false}},
    {{"category": "Loan APR",                         "bank": "BankName", "detail": "X.XX% APR", "is_bau": false}},
    {{"category": "FX/Multi-Currency",                "bank": "BankName", "detail": "specific detail with named product or %", "is_bau": false}},
    {{"category": "Fund Investment",                  "bank": "BankName", "detail": "specific fund subscription detail with 0% or $0", "is_bau": false}},
    {{"category": "Referral Bonus",                   "bank": "BankName", "detail": "HKD amount per referral", "is_bau": false}},
    {{"category": "Time Deposit / 定期存款",          "bank": "BankName", "detail": "X.XX% p.a. or specific rate", "is_bau": false}}
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

    result['best_for'] = _validate_best_for_evidence(result.get('best_for', []))
    result = _cross_check_best_for_from_strengths(result, promotions_by_bank)

    name_lookup = {k.lower(): k for k in promotions_by_bank}
    for bname in result.get('bank_analysis', {}):
        matched_key = name_lookup.get(bname.lower())
        if matched_key:
            all_p     = promotions_by_bank[matched_key]
            non_bau_p = [p for p in all_p if not p.get('is_bau')]
            result['bank_analysis'][bname]['count']     = len(non_bau_p)
            result['bank_analysis'][bname]['bau_count'] = len(all_p) - len(non_bau_p)
        else:
            result['bank_analysis'][bname]['count']     = 0
            result['bank_analysis'][bname]['bau_count'] = 0

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