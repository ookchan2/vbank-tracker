# scripts/ai_helper.py

import asyncio
import concurrent.futures
import json
import os
import re
import time
from datetime import datetime
from typing import Optional

_api_key     = None
_bot_name    = "Claude-3-7-Sonnet"
AI_AVAILABLE = False

_ENV_BOT_NAME = os.environ.get('POE_BOT_NAME', '').strip()

MODELS_TO_TRY = (
    [_ENV_BOT_NAME] if _ENV_BOT_NAME else [
        "Claude-3-7-Sonnet",
        "Claude-3-5-Sonnet",
        "GPT-4o",
        "Perplexity-Pro-Search",
    ]
)

ALLOWED_CATEGORIES = [
    "迎新", "消費", "投資", "旅遊", "保險",
    "貸款", "存款", "外匯", "推薦", "新資金", "Others"
]

# ── BAU overrides ─────────────────────────────────────────────────────────────

BAU_OVERRIDES: dict[str, list[str]] = {
    "za": ["new crypto customer fee waiver"],
}

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

# ── Non-bank content patterns (Issue 1 fix) ───────────────────────────────────
# If ANY of these phrases appear in a promotion's title/description/tc_link,
# the promotion is NOT a bank product and must be filtered out.

_NON_BANK_CONTENT_PATTERNS: list[str] = [
    # Government / disaster-relief pages
    'wang fuk court', 'wangfuk', 'support fund for',
    'inland revenue', 'ird.gov.hk', 'gov.hk/taxdeduction',
    'tax deduction for donation', 'donation receipt',
    'charity donation', 'relief fund', 'disaster relief',
    'taipofire', 'fire support', 'fire.gov',
    # Generic third-party non-bank indicators
    'approved charitable donation',
    'inland revenue ordinance',
    'bank of china (hong kong) account number 012-875',
]

# Domains that are NOT bank domains — used to flag mismatched tc_links
_NON_BANK_DOMAINS: list[str] = [
    'gov.hk', 'ird.gov.hk', 'taipofire', 'police.gov.hk',
    'welfare.gov.hk', 'charities', 'redcross',
]


def _filter_bank_relevant_promotions(promos: list, bank_name: str) -> list:
    """
    Remove promotions that clearly do NOT belong to the bank:
      - Government programs (tax deduction notices, relief funds)
      - Charity / third-party donation drives
      - Any entry whose tc_link resolves to a non-bank domain
    """
    filtered = []
    removed  = 0

    for p in promos:
        title       = (p.get('name')        or p.get('title')  or '').lower()
        description = (p.get('description') or '').lower()
        highlight   = (p.get('highlight')   or '').lower()
        tc_link     = (p.get('tc_link')     or p.get('link')   or '').lower()

        combined = f'{title} {description} {highlight} {tc_link}'

        # Check non-bank content patterns
        offending = next(
            (pat for pat in _NON_BANK_CONTENT_PATTERNS if pat in combined),
            None,
        )

        # Check if tc_link points to a clearly non-bank domain
        bad_domain = next(
            (d for d in _NON_BANK_DOMAINS if d in tc_link),
            None,
        )

        if offending or bad_domain:
            reason = f'pattern "{offending}"' if offending else f'non-bank domain in tc_link "{bad_domain}"'
            print(
                f'  🚫 Non-bank filter REMOVED [{bank_name}]: '
                f'"{p.get("name") or p.get("title")}" — {reason}'
            )
            removed += 1
        else:
            filtered.append(p)

    if removed:
        print(f'  🚫 Non-bank filter: {removed} non-bank promotion(s) removed for {bank_name}')
    return filtered


# ── Extraction prompt ─────────────────────────────────────────────────────────

_PROMPT_TMPL = """\
You are a specialist at extracting bank promotion data from website text.

Bank: BANK_NAME_PLACEHOLDER
Source URL: URL_PLACEHOLDER
Today's Date: TODAY_DATE_PLACEHOLDER

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
║     the promotion. Always use YYYY-MM-DD format.                    ║
║     Use null only if truly absent.                                  ║
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
║     • Any travel / flight / hotel / 旅遊 promo → tag 旅遊          ║
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
║  8. ⛔ DO NOT EXTRACT THESE — they are NOT bank promotions:        ║
║     • Navigation / menu items                                       ║
║     • Section headings without a concrete benefit amount            ║
║     • Pure risk disclaimers / legal boilerplate                     ║
║     • Generic product feature names with no specific reward         ║
║     • Footer links (Terms, Privacy Policy, Contact Us, etc.)       ║
║                                                                      ║
║     ❌ BAD extraction (nav item): "Travel with ZA Card"            ║
║     ✅ GOOD extraction (real deal): "Trip.com 8% off + 2% CashBack"║
║                                                                      ║
║     🚫 CRITICAL: ONLY extract promotions OFFERED BY THE BANK.     ║
║        The bank is BANK_NAME_PLACEHOLDER. Skip anything that is:   ║
║        • A government program, agency notice, or tax department    ║
║          instruction (e.g. Inland Revenue, tax deduction guides)   ║
║        • A charity / disaster-relief / donation drive operated     ║
║          by a third party (e.g. Support Fund for Wang Fuk Court,   ║
║          Red Cross, community funds)                               ║
║        • Content from a non-bank URL (gov.hk, charity sites, etc) ║
║        • A CSR / social-responsibility notice that is NOT a        ║
║          financial benefit offered to the bank's own customers     ║
║        If the content is from a government or charity website,     ║
║        ignore it completely — return [] for that section.          ║
║                                                                      ║
║  9. ⚠️  START DATE GATE (today = TODAY_DATE_PLACEHOLDER):           ║
║     If start_date is found AND start_date < TODAY_DATE_PLACEHOLDER  ║
║     → this promotion launched BEFORE today; it is NOT new today.    ║
║                                                                      ║
║  10. ✅ EXPIRY VALIDATION (today = TODAY_DATE_PLACEHOLDER):         ║
║      If end_date > TODAY_DATE_PLACEHOLDER the promotion is still    ║
║      ACTIVE — it has NOT expired.                                   ║
║      Only set is_bau=false and include the end_date as-is.         ║
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
Remember: return ONLY the JSON array starting with [ and ending with ].
If the text is entirely from a government or charity website, return []."""


def _build_prompt(bank_name: str, url: str, text: str) -> str:
    today = datetime.now().strftime('%Y-%m-%d')
    return (
        _PROMPT_TMPL
        .replace('BANK_NAME_PLACEHOLDER',  bank_name)
        .replace('URL_PLACEHOLDER',        url)
        .replace('TODAY_DATE_PLACEHOLDER', today)
        .replace('TEXT_PLACEHOLDER',       text)
    )


# ── Poe async core ────────────────────────────────────────────────────────────

_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix='poe_ai',
)


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
    except Exception as exc:
        print(f'  ⚠️  Poe async call error ({bot_name}): {exc}')
        return ''


def _run_async(coro) -> str:
    try:
        asyncio.get_running_loop()
        future = _executor.submit(asyncio.run, coro)
        return future.result(timeout=180)
    except RuntimeError:
        return asyncio.run(coro)
    except Exception as exc:
        print(f'  ⚠️  _run_async error: {exc}')
        return ''


def _call(messages: list, label: str = '') -> str:
    if not AI_AVAILABLE or _api_key is None:
        return ''
    t = time.monotonic()
    try:
        result  = _run_async(_async_call(messages, _bot_name))
        elapsed = time.monotonic() - t
        tag     = f' [{label}]' if label else ''
        print(f'  [DEBUG] AI ({_bot_name}){tag} → {len(result)} chars in {elapsed:.1f}s')
        if len(result) < 50:
            print(f'  [DEBUG] Full response: {repr(result)}')
        return result
    except Exception as exc:
        print(f'  ⚠️  Call error: {exc}')
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
            except Exception as exc:
                print(f'  ❌ {model} error: {exc}')
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
    except Exception as exc:
        print(f'❌ AI init failed: {exc}')
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


def _parse_object(raw: str) -> Optional[dict]:
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
    except json.JSONDecodeError as exc:
        print(f'  ⚠️  JSON object parse failed: {exc}. First 200 chars: {raw[:200]}')
        return None


def _trim_text(text: str, max_chars: int = 25_000) -> str:
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


# ── Evidence gate ─────────────────────────────────────────────────────────────

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
    r'|cashback|cash\s*back'
    r'|\bflight\b'
    r'|\bhotel\b'
    r'|\blounge\b'
    r'|travel\s*insur'
    r'|\bagoda\b'
    r'|旅遊',
    re.IGNORECASE,
)

_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    'HK Stock Trading': [
        'hk stock', 'hong kong stock', 'hkex', 'local stock',
        'hk securities', 'hk shares', 'hong kong shares',
        'hk brokerage', 'stock', 'securities', 'brokerage', 'ipo',
        'trading fee', 'platform fee', '$0 commission', 'commission',
        'powerdraw', 'free stock', 'equities', 'share trading',
    ],
    'US Stock Trading': [
        'us stock', 'us equities', 'us securities', 'us shares',
        'american stock', 'nyse', 'nasdaq', 'us market',
        'us brokerage', '$0 commission', 'commission',
        'trading fee', 'platform fee', 'stock', 'equities',
    ],
    'Stock Trading': [
        'stock', 'securities', 'brokerage', 'ipo',
        'trading fee', 'platform fee', '$0 commission', 'commission',
        'powerdraw', 'free stock', 'hk stock', 'us stock',
        'equities', 'share trading',
    ],
    'Crypto Trading': [
        'crypto', 'bitcoin', 'virtual asset', 'digital asset',
        'cryptocurrency', 'btc', 'eth', 'virtual currency',
        'crypto platform fee', 'crypto trading fee',
        'digital asset fee', 'crypto commission',
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
        '旅遊', 'travel', 'airline', 'airport', 'airfare',
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

        if is_vague:
            print(
                f'  ⚠️  Vague-pattern flag [{cat}] "{bank}" — '
                f'evidence present={has_evidence}: "{detail[:70]}"'
            )

        if not has_evidence:
            print(
                f'  🚫 Evidence gate REJECTED [{cat}] winner "{bank}" '
                f'(no concrete fact found) → detail: "{detail}"'
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


# ── Stock trading total-cost validator (Issue 2 fix) ──────────────────────────
#
#  Total cost per trade = Commission fee + Platform fee
#
#  ZA Bank structure (verified from official pages):
#    HK stocks: $0 commission + HKD 18 platform fee (min per trade)
#    US stocks: $0 commission + USD 0.0099/share, min USD 1.99
#
#  Evaluation rule:
#    1. If the selected winner charges commission AND platform fee at levels
#       that exceed ZA Bank's equivalent, override to ZA Bank.
#    2. If a competitor genuinely offers lower TOTAL COST for the trade size
#       described in their detail, keep their win — but ensure the detail
#       includes a full cost breakdown (commission + platform fee).
#    3. "Zero platform fee but charges per-share commission" can legitimately
#       beat ZA Bank for very small US trades (< ~166 shares) — allow this.

_STOCK_CATS  = {'HK Stock Trading', 'US Stock Trading', 'Stock Trading'}
_ZA_NAMES    = {'za bank', 'za', 'za invest'}

# Regex: winner's detail contains per-share or per-trade commission language
_CHARGES_COMMISSION_RE = re.compile(
    r'usd\s*[\d]+\.[\d]+\s*/\s*share'
    r'|usd\s*[\d]+\.[\d]+\s*per\s*share'
    r'|hkd\s*[\d]+\.?[\d]*\s*(per|/)\s*(trade|lot|share|order)'
    r'|0\.\d+%\s*(brokerage|commission)'
    r'|commission\s+(of|at|is|:)\s*[\d]'
    r'|[\d]+\.[\d]+\s*usd\s*per',
    re.IGNORECASE,
)

# Regex: detail also mentions $0 or zero platform fee (PAO-style: commission + $0 platform)
_ZERO_PLATFORM_FEE_RE = re.compile(
    r'zero\s+platform\s+fee'
    r'|\$\s*0\s+platform'
    r'|no\s+platform\s+fee'
    r'|platform\s+fee[\s:]+\$?\s*0\b'
    r'|free\s+platform',
    re.IGNORECASE,
)

# Regex: extract per-share USD commission amount for comparison
_USD_PER_SHARE_RE = re.compile(r'usd\s*([\d]+\.[\d]+)\s*(?:/|per)\s*share', re.IGNORECASE)

# ZA Bank reference platform fees (for total-cost comparison)
_ZA_HK_PLATFORM_FEE_HKD = 18.0    # HKD per trade (minimum)
_ZA_US_PLATFORM_FEE_USD  = 1.99   # USD per trade (minimum, USD 0.0099/share)

# Break-even share count: if a competitor charges X USD/share and $0 platform,
# ZA Bank's USD 1.99 flat platform fee is cheaper above this share count.
def _us_breakeven_shares(per_share_usd: float) -> float:
    if per_share_usd <= 0:
        return float('inf')
    return _ZA_US_PLATFORM_FEE_USD / per_share_usd


def _validate_stock_trading_winners(best_for: list) -> list:
    """
    Post-process stock trading category winners using TOTAL COST logic.

    Total cost = Commission fee + Platform fee

    Rules:
    ① If the winner charges commission AND mentions zero platform fee
       (e.g. PAO Bank: USD 0.012/share + $0 platform for US stocks):
       - Extract the per-share commission rate
       - Calculate break-even share count vs ZA Bank's flat platform fee
       - For US stocks: if per-share rate > USD 0.0099 AND the detail is about
         typical retail trades (200+ shares), ZA Bank's total cost is lower
         → override to ZA Bank with explanation
       - For HK stocks: ZA Bank's $0 commission + HKD 18 flat fee beats
         any per-trade commission above HKD 18 → compare and decide
       - If the competitor's total cost is genuinely lower for the described
         trade size, KEEP their win but ensure detail shows full cost breakdown

    ② If the winner charges BOTH commission AND platform fee at levels that
       exceed ZA Bank's equivalent → always override to ZA Bank.

    ③ If ZA Bank is the winner, no action needed.
    """
    overrides = 0
    for i, entry in enumerate(best_for):
        cat  = (entry.get('category') or '').strip()
        bank = (entry.get('bank')     or '').strip()

        if cat not in _STOCK_CATS:
            continue
        if bank.lower() in _ZA_NAMES:
            # ZA Bank winning — ensure detail mentions both commission AND platform fee
            detail = (entry.get('detail') or '')
            if 'platform fee' not in detail.lower():
                best_for[i] = {
                    **entry,
                    'detail': (
                        detail.rstrip('. ') +
                        '; platform fee applies (HK: HKD 18/trade, US: USD 1.99/trade minimum)'
                    ),
                }
            continue

        detail = (entry.get('detail') or '')
        charges_commission = bool(_CHARGES_COMMISSION_RE.search(detail))
        has_zero_platform  = bool(_ZERO_PLATFORM_FEE_RE.search(detail))

        if not charges_commission:
            # Winner doesn't charge commission — no action needed
            continue

        # Winner charges commission (+ possibly zero platform fee)
        if cat in ('US Stock Trading', 'Stock Trading') and has_zero_platform:
            # Extract per-share rate to compare total costs
            m = _USD_PER_SHARE_RE.search(detail)
            if m:
                per_share = float(m.group(1))
                breakeven = _us_breakeven_shares(per_share)
                # For a "typical" retail US stock trade we use 200 shares as benchmark
                # At 200 shares:
                #   Competitor: 200 × per_share + $0 platform
                #   ZA Bank:    $0 commission  + USD 1.99 platform (flat min)
                competitor_cost_200 = 200 * per_share
                za_cost_200         = _ZA_US_PLATFORM_FEE_USD  # flat minimum

                if competitor_cost_200 > za_cost_200:
                    # ZA Bank cheaper at 200-share benchmark → override
                    print(
                        f'  🔄 US stock total-cost OVERRIDE [{cat}]: '
                        f'"{bank}" @ USD {per_share}/share × 200 = USD {competitor_cost_200:.2f} '
                        f'vs ZA Bank USD {za_cost_200:.2f} platform fee. '
                        f'ZA Bank is cheaper above {breakeven:.0f} shares → overriding.'
                    )
                    best_for[i] = {
                        **entry,
                        'bank':   'ZA Bank',
                        'detail': (
                            f'$0 brokerage commission for US stocks via ZA Invest; '
                            f'platform fee USD 0.0099/share (min USD 1.99/trade). '
                            f'Total cost at 200 shares: USD {za_cost_200:.2f}. '
                            f'{bank} charges USD {per_share}/share commission + $0 platform = '
                            f'USD {competitor_cost_200:.2f} at 200 shares — '
                            f'ZA Bank is cheaper for trades above {breakeven:.0f} shares.'
                        ),
                        'is_bau':  True,
                        'similar_banks': [bank] + [
                            b for b in (entry.get('similar_banks') or [])
                            if b.lower() not in _ZA_NAMES and b != bank
                        ],
                        'why_others_lose': (
                            f'{bank} charges USD {per_share}/share commission (no platform fee). '
                            f'ZA Bank charges $0 commission + USD 1.99 flat platform fee. '
                            f'For trades of {breakeven:.0f}+ shares, ZA Bank total cost is lower. '
                            f'Most retail investors trade 200+ shares, making ZA Bank cheaper overall.'
                        ),
                    }
                    overrides += 1
                else:
                    # Competitor genuinely cheaper for 200-share benchmark — keep win
                    # but ensure detail shows FULL cost breakdown
                    print(
                        f'  ✅ US stock total-cost KEPT [{cat}]: '
                        f'"{bank}" @ USD {per_share}/share × 200 = USD {competitor_cost_200:.2f} '
                        f'< ZA Bank USD {za_cost_200:.2f} for 200-share benchmark. '
                        f'Break-even: {breakeven:.0f} shares.'
                    )
                    # Enrich detail with full cost comparison if not already present
                    if 'total cost' not in detail.lower() and 'vs za' not in detail.lower():
                        best_for[i] = {
                            **entry,
                            'detail': (
                                detail.rstrip('. ') +
                                f'; total cost at 200 shares: USD {competitor_cost_200:.2f} '
                                f'vs ZA Bank USD {za_cost_200:.2f} '
                                f'(break-even: {breakeven:.0f} shares)'
                            ),
                        }
            else:
                # Commission mentioned but can't extract per-share rate — be conservative
                # If no zero-platform detail, treat as commission bank and override
                print(
                    f'  🔄 Stock trading OVERRIDE [{cat}]: '
                    f'"{bank}" charges commission (rate unclear). '
                    f'ZA Bank $0 commission is safer default.'
                )
                best_for[i] = {
                    **entry,
                    'bank':   'ZA Bank',
                    'detail': (
                        '$0 brokerage commission for US stocks via ZA Invest; '
                        'platform fee USD 0.0099/share (min USD 1.99/trade). '
                        f'{bank} charges commission (exact rate unspecified).'
                    ),
                    'is_bau':  True,
                    'similar_banks': [bank] + [
                        b for b in (entry.get('similar_banks') or [])
                        if b.lower() not in _ZA_NAMES and b != bank
                    ],
                    'why_others_lose': (
                        f'{bank} charges per-trade commission; ZA Bank is $0 commission. '
                        'Total cost = commission + platform fee; ZA Bank eliminates commission entirely.'
                    ),
                }
                overrides += 1

        elif cat == 'HK Stock Trading' and charges_commission:
            # For HK stocks: any commission + $0 platform vs ZA $0 commission + HKD 18 platform
            # Most HK stock trades are 1 board lot (e.g., 1000 shares × HKD 10 = HKD 10,000)
            # At 0.03% commission: HKD 3 < HKD 18 → competitor is cheaper
            # At 0.05% commission on HKD 10,000: HKD 5 < HKD 18 → competitor may be cheaper
            # At 0.1% on HKD 20,000: HKD 20 > HKD 18 → ZA Bank cheaper
            # Without knowing exact HK commission rate, we KEEP the winner
            # but ensure the detail mentions full cost breakdown
            if 'platform fee' not in detail.lower() and 'total cost' not in detail.lower():
                best_for[i] = {
                    **entry,
                    'detail': (
                        detail.rstrip('. ') +
                        '; zero platform fee. '
                        'Compare total cost: commission + $0 platform vs ZA Bank '
                        '$0 commission + HKD 18 platform fee.'
                    ),
                }
            print(
                f'  ℹ️  HK stock winner kept [{cat}]: '
                f'"{bank}" charges commission + $0 platform. '
                f'Full cost comparison depends on commission rate vs ZA\'s HKD 18 platform fee.'
            )

    if overrides:
        print(f'  🔄 Stock trading total-cost override: {overrides} winner(s) updated')
    return best_for


def _cross_check_best_for_from_strengths(
    result: dict,
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
        if not keywords and cat in ('HK Stock Trading', 'US Stock Trading'):
            keywords = _CATEGORY_KEYWORDS.get('Stock Trading', [])

        candidates: list[tuple[str, str]] = []
        for bname, bdata in bank_analysis.items():
            for s in (bdata.get('strengths') or []):
                if any(kw.lower() in s.lower() for kw in keywords):
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
            any(kw.lower() in (p.get('name') or p.get('title') or '').lower()
                for kw in keywords)
            for p in bank_promos
        )

        print(
            f'  🔁 Strength cross-check FILLED [{cat}] → {best_bank}: '
            f'"{best_detail[:80]}"'
        )
        best_for[i] = {**entry, 'bank': best_bank, 'detail': best_detail, 'is_bau': is_bau_guess}
        filled += 1

    if filled:
        print(f'  🔁 Cross-check: {filled} slot(s) filled from bank_analysis.strengths')

    result['best_for'] = best_for
    return result


# ── Public API ────────────────────────────────────────────────────────────────

def analyze_promotions(
    bank_id:     str,
    bank_name:   str,
    text:        str = '',
    screenshot:  Optional[bytes] = None,
    default_url: str = '',
) -> list:

    if not AI_AVAILABLE:
        return []

    clean   = _trim_text(text.strip() if text else '')
    results = []

    if len(clean) >= 200:
        prompt = _build_prompt(bank_name=bank_name, url=default_url, text=clean)

        for attempt in range(2):
            raw    = _call([{'role': 'user', 'content': prompt}], label=bank_id)
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

    # ── Issue 1 fix: remove non-bank / government content ─────────────────────
    results = _filter_bank_relevant_promotions(results, bank_name)

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

    for attempt in range(2):
        try:
            raw = _call([{'role': 'user', 'content': prompt}], label=f'dedup/{bank_name}')
            if not raw:
                if attempt == 0:
                    print(f'  🔄 Retry ai_dedup_titles for {bank_name}...')
                    continue
                return {}
            raw  = re.sub(r'^```[a-z]*\n?', '', raw.strip())
            raw  = re.sub(r'\n?```$',       '', raw.strip())
            data = json.loads(raw)
            dup_map = {
                int(dup): int(g['keep_index'])
                for g in data.get('groups', [])
                for dup in g.get('duplicate_indices', [])
            }
            if dup_map:
                print(f'  🤖 ai_dedup_titles [{bank_name}]: {len(dup_map)} duplicate(s)')
            return dup_map
        except Exception as exc:
            if attempt == 0:
                print(f'  ⚠️  ai_dedup_titles [{bank_name}] attempt 1 failed: {exc!r} — retrying')
            else:
                print(f'  ⚠️  ai_dedup_titles [{bank_name}]: {exc!r} — skipping')
                return {}
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

2. Fee synonyms:
   "Zero Fee" = "0% Fee" = "Fee Waiver" = "No Fee" = "Commission-Free"

3. Promo codes:
   Same code stem (ignoring trailing 2-digit year) → MATCH

4. Crypto fee promotions:
   Any title about crypto + fee waiver/removal → MATCH each other

5. Best-in-Town:
   Any title containing "Best-in-Town" for the same bank → MATCH

6. Trip.com:
   "Trip.com Annual Discount" ↔ "Trip.com x Mox Credit Card Year-Round Promotion" → MATCH

7. SWIFT / Payment Connect:
   "SWIFT Transfers" ↔ "Payment Connect" → MATCH

8. WeLab Global Wallet FX:
   Any WeLab Global Wallet + FX/exchange/remittance title → MATCH

9. Payroll switch:
   Any payroll + switch/deposit/benefit → MATCH

10. Fund zero-fee:
    "$0 Fund Trading Fee" ↔ "Zero Fund Subscription Fee" ↔ "0% Fund Subscription Fee" → MATCH

11. Referral programs:
    "多友多賞" ↔ "Mox Referral Programme" ↔ "Refer a Friend HKD300" → MATCH

12. Account opening BAU:
    "Account Opening in 3 Minutes" ↔ "Quick Account Opening" → MATCH

13. When uncertain → declare MATCH
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

    for attempt in range(2):
        try:
            raw = _call(
                [{'role': 'user', 'content': prompt}],
                label=f'match/{bank_name}',
            )
            if not raw:
                if attempt == 0:
                    print(f'  🔄 Retry ai_match_against_existing for {bank_name}...')
                    continue
                return {}
            raw  = re.sub(r'^```[a-z]*\n?', '', raw.strip())
            raw  = re.sub(r'\n?```$',       '', raw.strip())
            data = json.loads(raw)
            result_map: dict[int, int] = {
                int(k): int(v)
                for k, v in data.items()
                if str(k).isdigit() and str(v).isdigit()
            }
            msg = (
                f'{len(result_map)} match(es)'
                if result_map else
                '0 matches — all appear genuinely new'
            )
            print(f'  🤖 ai_match_against_existing [{bank_name}]: {msg}')
            return result_map
        except Exception as exc:
            if attempt == 0:
                print(f'  ⚠️  ai_match_against_existing [{bank_name}] attempt 1: {exc!r} — retrying')
            else:
                print(f'  ⚠️  ai_match_against_existing [{bank_name}]: {exc!r} — skipping')
                return {}
    return {}


# ── Strategic insights helpers ────────────────────────────────────────────────

def _build_bank_summary_lines(promos: list) -> list[str]:
    lines = []
    for p in promos:
        title     = (p.get('name') or p.get('title') or 'N/A')[:80]
        highlight = (p.get('highlight') or p.get('description') or '')[:120]
        period    = (p.get('period') or 'Ongoing')[:60]
        raw_types = p.get('types') or ['General']
        ptype     = (', '.join(raw_types) if isinstance(raw_types, list) else str(raw_types))[:40]
        bau_tag   = ' [BAU - Permanent Feature]' if p.get('is_bau') else ''
        lines.append(f'  [{ptype}]{bau_tag} {title}: {highlight} | {period}')
    return lines


_DIAGNOSTIC_CATEGORIES: list[tuple[str, list[str]]] = [
    ('HK Stock Trading',   ['投資', 'hk stock', 'hong kong stock', 'hkex', 'local stock',
                             'securities', 'brokerage', 'ipo', 'commission', 'trading fee']),
    ('US Stock Trading',   ['投資', 'us stock', 'us equities', 'nasdaq', 'nyse',
                             'american stock', 'commission', 'trading fee']),
    ('Crypto Trading',     ['投資', 'crypto', 'bitcoin', 'virtual asset', 'digital asset', 'cryptocurrency']),
    ('Fund Investment',    ['投資', 'fund', '基金', '$0認購費', 'subscription fee']),
    ('Referral Bonus',     ['推薦', 'referral', '多友多賞', 'invite']),
    ('Travel',             ['旅遊', 'trip', 'travel', 'asia miles', 'flight', 'hotel']),
    ('Spending/CashBack',  ['消費', 'cashback', 'cash back', 'rebate']),
    ('Welcome Bonus',      ['迎新', 'welcome', 'new customer']),
    ('Loan APR',           ['貸款', 'loan', 'apr']),
    ('FX/Multi-Currency',  ['外匯', 'fx', 'multi-currency', 'global wallet']),
]

_SPARSE_THRESHOLD = 3


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
            raw  = p.get('types') or []
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
                    p.get('name', ''),
                    p.get('title', ''),
                    p.get('highlight', ''),
                    p.get('description', ''),
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


def _check_sparse_banks(promotions_by_bank: dict) -> list[str]:
    sparse = [
        bank for bank, promos in promotions_by_bank.items()
        if len(promos) < _SPARSE_THRESHOLD
    ]
    if sparse:
        print(
            f'  ⚠️  SPARSE BANKS: {sparse} (each has < {_SPARSE_THRESHOLD} promos)\n'
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
            print(f'  ⚠️  supplement_from_db: no DB rows for "{bank}"')
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
        print(
            f'  🔄 supplement_from_db: "{bank}" '
            f'{"added " + str(added) + " from DB → now " + str(len(promos)) + " total" if added else "no new titles found in DB"}'
        )

    if supplemented_total:
        print(f'  🔄 supplement_from_db: {supplemented_total} DB row(s) merged total')
    return promotions_by_bank


# ── Strategic insights — main entry point ────────────────────────────────────

def generate_strategic_insights(
    promotions_by_bank: dict,
    db_fetch_fn=None,
) -> Optional[dict]:
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
                '  ⚠️  Sparse banks found but no db_fetch_fn provided.\n'
                '     Pass db_fetch_fn=get_promotions_by_bank_name to auto-supplement.'
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
    today           = datetime.now().strftime('%Y-%m-%d')

    prompt = f"""You are a Hong Kong virtual bank analyst.
Analyze these active promotions and return strategic insights as JSON.
Today's date: {today}

{promotions_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 1 — BAU ITEMS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Items tagged [BAU - Permanent Feature] are ALWAYS-AVAILABLE with no expiry.
You MUST include BAU items when evaluating "best_for" category winners.
A permanent zero-fee or zero-commission feature is often the strongest
competitive advantage — do NOT skip it just because it has no end date.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 2 — DATE VALIDATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Today is {today}.
⚠️  If a promotion has end_date > {today}, it is STILL ACTIVE — never
treat it as expired.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 3 — CHINESE TYPE TAG → ENGLISH CATEGORY MAPPING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  [推薦]                              → "Referral Bonus"
  [投資] + fund/基金                  → "Fund Investment"
  [投資] + hk stock/hkex/securities   → "HK Stock Trading"
  [投資] + us stock/nyse/nasdaq        → "US Stock Trading"
  [投資] + crypto/bitcoin/virtual      → "Crypto Trading"
  [消費]                              → "Spending/CashBack"
  [迎新]                              → "Welcome Bonus"
  [旅遊]                              → "Travel"
  [貸款]                              → "Loan APR"
  [外匯]                              → "FX/Multi-Currency"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 4 — STRICT CATEGORY DEFINITIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• HK Stock Trading  → HKEX-listed stocks: commission + platform fee comparison.
• US Stock Trading  → NYSE/NASDAQ stocks: commission + platform fee comparison.
• Crypto Trading    → crypto/virtual asset trading fees.
• Fund Investment   → fund subscription or switching fee promotions.
• Spending/CashBack → card cashback or merchant spending rewards.
• Welcome Bonus     → new customer account opening cash/gift rewards.
• Travel            → travel insurance, flight/hotel, Asia Miles, Trip.com, lounge.
• Loan APR          → personal loan with lowest specific APR quoted.
• FX/Multi-Currency → FX rate promotions, global wallet, remittance.
• Referral Bonus    → referral programs with a stated HKD reward amount.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 5 — MANDATORY WINNER SELECTION RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Output "None" ONLY when there is absolutely zero evidence of any promotion
across ALL banks that relates to that category.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 6 — EVIDENCE GATE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The "detail" field MUST contain at least ONE concrete verifiable fact:
  • Specific HKD/USD amount, percentage, $0/zero-fee, specific date,
    named concrete product, or trading/commission/fee/cashback keyword.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 7 — SIMILAR BANKS & WHY THEY DON'T WIN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For EVERY best_for entry populate:
• similar_banks: ALL other banks offering a similar promotion in this category
• why_others_lose: specific comparative reason using fees, caps, expiry dates

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 8 — SELF-CONSISTENCY CHECK
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Before writing final JSON: verify that every "None" in best_for is NOT
contradicted by a matching strength in bank_analysis for the same category.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 9 — STOCK TRADING: TOTAL COST ANALYSIS (CRITICAL)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️  THE ONLY CORRECT WAY TO COMPARE STOCK TRADING COSTS:

  Total cost per trade = Commission fee + Platform fee

  A bank that charges $0 commission but HKD 18 platform fee has a
  TOTAL COST of HKD 18 per trade.

  A bank that charges USD 0.012/share commission but $0 platform fee
  has a TOTAL COST of (0.012 × number_of_shares) per trade.

VERIFIED FEE STRUCTURES:

  ZA Bank (via ZA Invest):
    • Commission:    $0 (zero brokerage commission for ALL stocks)
    • Platform fee:  HKD 18/trade minimum (HK stocks)
                     USD 0.0099/share, min USD 1.99/trade (US stocks)
    • Total cost HK: $0 commission + HKD 18 = HKD 18 per trade
    • Total cost US: $0 commission + USD 1.99 min = USD 1.99 per trade (up to ~201 shares)
    ❌ NEVER say "ZA Bank charges commission" — factually incorrect
    ❌ "StockBack" = card spending cashback — NOT a trading fee

  PAO Bank (PAObank):
    • HK stocks: commission charged + $0 platform fee
    • US stocks: USD 0.012/share commission + $0 platform fee
    • Total cost HK: commission + $0 = commission amount
    • Total cost US at 100 shares: 0.012 × 100 = USD 1.20 + $0 = USD 1.20
    • Total cost US at 200 shares: 0.012 × 200 = USD 2.40 + $0 = USD 2.40

WINNER DETERMINATION — compare TOTAL COST:

  HK Stock Trading:
    → ZA Bank total cost = HKD 18 (flat minimum, $0 commission)
    → PAO Bank total cost = commission amount (platform fee = $0)
    → If PAO's per-trade commission < HKD 18 for typical trades → PAO wins
    → If PAO's per-trade commission ≥ HKD 18 → ZA Bank wins
    → Since PAO's exact HK commission rate is not specified as a fixed low rate,
      and ZA Bank's HKD 18 is a known, transparent flat fee,
      ZA Bank wins for HK stocks if no evidence PAO's commission < HKD 18

  US Stock Trading:
    → ZA Bank total cost = USD 1.99 minimum (flat, ≤201 shares)
    → PAO Bank at 100 shares = USD 0.012 × 100 = USD 1.20 → PAO CHEAPER
    → PAO Bank at 200 shares = USD 0.012 × 200 = USD 2.40 → ZA CHEAPER
    → PAO Bank at 166 shares = USD 0.012 × 166 = USD 1.99 = TIED
    → BREAK-EVEN: 166 shares
    → For trades <166 shares: PAO Bank total cost is lower → PAO wins
    → For trades ≥166 shares: ZA Bank total cost is lower → ZA wins
    → RECOMMENDATION: If the data shows PAO's USD 0.012/share for US stocks,
      select the winner based on most common retail trade size.
      A "typical" retail investor buying popular US stocks (e.g. AAPL, NVDA)
      usually trades 100–500 shares. At 200 shares = typical benchmark:
        PAO = USD 2.40 vs ZA = USD 1.99 → ZA Bank wins at 200-share benchmark
      BUT note in the detail that PAO is cheaper for small trades (<166 shares).

MANDATORY DETAIL FORMAT for stock trading winners:
  Always include BOTH commission AND platform fee in the "detail" field.
  Example: "ZA Bank: $0 commission + USD 1.99 platform fee (min) for US stocks.
  Total cost at 200 shares: USD 1.99 vs PAO Bank USD 2.40 (USD 0.012 × 200).
  PAO is cheaper for trades <166 shares; ZA wins for ≥166 shares."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 10 — FORBIDDEN COMPARISONS IN BANK ANALYSIS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ❌ Account opening speed comparisons (baseline UX for ALL virtual banks)
  ❌ Generic app speed / UI claims without specific financial benefit
  ✅ VALID: specific fee savings in HKD/%, named products with concrete amounts
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return this EXACT JSON structure (no markdown, no code fences):
{{
  "best_for": [
    {{
      "category":       "HK Stock Trading",
      "bank":           "ZA Bank",
      "detail":         "$0 commission + HKD 18 platform fee/trade. Total cost HKD 18 vs PAO Bank (commission + $0 platform). ZA wins unless PAO's commission < HKD 18/trade.",
      "is_bau":         true,
      "similar_banks":  ["PAObank"],
      "why_others_lose":"PAObank charges brokerage commission per HK trade; ZA Bank's HKD 18 flat platform fee with $0 commission is transparent and cost-effective for typical board-lot trades."
    }},
    {{
      "category":       "US Stock Trading",
      "bank":           "ZA Bank",
      "detail":         "$0 commission + USD 0.0099/share (min USD 1.99) platform fee. At 200-share benchmark: ZA USD 1.99 vs PAO USD 2.40 (USD 0.012 × 200). Break-even: 166 shares — PAO cheaper below that.",
      "is_bau":         true,
      "similar_banks":  ["PAObank"],
      "why_others_lose":"PAObank charges USD 0.012/share commission + $0 platform. At 200 shares PAO costs USD 2.40 vs ZA USD 1.99. ZA wins for trades ≥166 shares (most retail benchmarks)."
    }},
    {{
      "category":       "Crypto Trading",
      "bank":           "BankName",
      "detail":         "specific crypto fee/platform detail",
      "is_bau":         false,
      "similar_banks":  ["BankA"],
      "why_others_lose":"specific reason"
    }},
    {{
      "category":       "Spending/CashBack",
      "bank":           "BankName",
      "detail":         "specific % or HKD amount",
      "is_bau":         false,
      "similar_banks":  [],
      "why_others_lose":"Only bank currently offering this promotion."
    }},
    {{
      "category":       "Welcome Bonus",
      "bank":           "BankName",
      "detail":         "HKD amount",
      "is_bau":         false,
      "similar_banks":  ["BankA"],
      "why_others_lose":"specific reason"
    }},
    {{
      "category":       "Travel",
      "bank":           "BankName",
      "detail":         "specific benefit with named partner or %",
      "is_bau":         false,
      "similar_banks":  [],
      "why_others_lose":"Only bank currently offering this promotion."
    }},
    {{
      "category":       "Loan APR",
      "bank":           "BankName",
      "detail":         "X.XX% APR",
      "is_bau":         false,
      "similar_banks":  ["BankA"],
      "why_others_lose":"BankA APR is higher"
    }},
    {{
      "category":       "FX/Multi-Currency",
      "bank":           "BankName",
      "detail":         "specific detail with named product or %",
      "is_bau":         false,
      "similar_banks":  [],
      "why_others_lose":"Only bank currently offering this promotion."
    }},
    {{
      "category":       "Fund Investment",
      "bank":           "BankName",
      "detail":         "specific fund subscription detail with 0% or $0",
      "is_bau":         false,
      "similar_banks":  ["BankA"],
      "why_others_lose":"BankA limits zero-fee to selected funds only"
    }},
    {{
      "category":       "Referral Bonus",
      "bank":           "BankName",
      "detail":         "HKD amount per referral",
      "is_bau":         false,
      "similar_banks":  ["BankA"],
      "why_others_lose":"BankA pays less per referral"
    }}
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
      "vs_za_pros": "pros vs ZA Bank (NO account opening speed comparisons)",
      "vs_za_cons": "cons vs ZA Bank"
    }}
  }}
}}"""

    raw = _call([{'role': 'user', 'content': prompt}], label='insights')
    if not raw:
        print('❌ Strategic insights: empty response from AI')
        return None

    result = _parse_object(raw)
    if result is None:
        print('❌ Strategic insights: JSON parse failed')
        return None

    # ── Post-processing pipeline ──────────────────────────────────────────────
    # Step 1: Evidence gate — reject vague/unverified winners
    result['best_for'] = _validate_best_for_evidence(result.get('best_for', []))

    # Step 2: Stock trading total-cost validator (Issue 2 fix)
    result['best_for'] = _validate_stock_trading_winners(result.get('best_for', []))

    # Step 3: Cross-check None slots from bank_analysis strengths
    result = _cross_check_best_for_from_strengths(result, promotions_by_bank)

    name_lookup = {k.lower(): k for k in promotions_by_bank}
    for bname in result.get('bank_analysis', {}):
        matched_key = name_lookup.get(bname.lower())
        if matched_key:
            all_p      = promotions_by_bank[matched_key]
            non_bau    = [p for p in all_p if not p.get('is_bau')]
            result['bank_analysis'][bname]['count']     = len(non_bau)
            result['bank_analysis'][bname]['bau_count'] = len(all_p) - len(non_bau)
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
            f'     ↳ Check diagnostic above — these categories had no input data.'
        )

    print(
        f'✅ Strategic insights generated via {_bot_name} '
        f'({bau_wins} BAU winner(s), {none_wins} None slot(s))'
    )
    return result