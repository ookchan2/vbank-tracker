# scripts/ai_helper.py
#
# ★ BANK NAME CHANGES:
#   Airstar Bank → EleBank  (bank_id 'airstar' unchanged for DB compat)
#   PAObank      → PADB     (bank_id 'pao' unchanged)
#
# ★ ELEBANK STOCK TRADING FEES (non-fractional, injected into AI prompts):
#   HK stocks: $0 commission + HKD 15 platform fee per order
#   US stocks: USD 0.0049/share commission (min USD 0.99/order)
#              + USD 0.005/share platform fee (min USD 1.00/order)
#              = USD 0.0099/share total, min USD 1.99/order (tied with ZA Bank)

import asyncio
import concurrent.futures
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Claude API configuration (via Anthropic SDK)
try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None

AI_AVAILABLE = False
_ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '').strip()

# Fallback models list
MODELS_TO_TRY = [
    "claude-3.7-sonnet",
    "claude-3-5-sonnet",
]

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

# ── Non-bank content patterns ─────────────────────────────────────────────────

_NON_BANK_CONTENT_PATTERNS: list[str] = [
    'taipofire.gov.hk',
    'taxdeduction.html',
    'hab033',
    'cefs.gov.hk',
    'wang fuk court',
    'wangfuk',
    '宏福苑',
    'support fund for wang fuk',
    'support fund for',
    '大埔宏福苑',
    '援助基金',
    'tai po fire',
    'inland revenue',
    'ird.gov.hk',
    'gov.hk/taxdeduction',
    'tax deduction for donation',
    'tax deduction arrangement',
    'donation receipt',
    'donation acknowledgement',
    'letter of appreciation',
    'government sincerely thanks',
    '政府衷心感謝',
    '捐款致謝',
    '稅務扣除安排',
    'charity donation',
    'relief fund',
    'disaster relief',
    'taipofire',
    'fire support',
    'fire.gov',
    'approved charitable donation',
    'inland revenue ordinance',
    'bank of china (hong kong) account number 012-875',
]

_NON_BANK_DOMAINS: list[str] = [
    'gov.hk',
    'ird.gov.hk',
    'taipofire.gov.hk',
    'taipofire',
    'cefs.gov.hk',
    'police.gov.hk',
    'welfare.gov.hk',
    'charities',
    'redcross',
]


def _filter_bank_relevant_promotions(promos: list, bank_name: str) -> list:
    filtered = []
    removed  = 0

    for p in promos:
        title       = (p.get('name')        or p.get('title')  or '').lower()
        description = (p.get('description') or '').lower()
        highlight   = (p.get('highlight')   or '').lower()
        tc_link     = (p.get('tc_link')     or p.get('link')   or '').lower()

        combined = f'{title} {description} {highlight} {tc_link}'

        offending = next(
            (pat for pat in _NON_BANK_CONTENT_PATTERNS if pat in combined),
            None,
        )
        bad_domain = next(
            (d for d in _NON_BANK_DOMAINS if d in tc_link),
            None,
        )

        if offending or bad_domain:
            reason = (
                f'pattern "{offending}"' if offending
                else f'non-bank domain in tc_link "{bad_domain}"'
            )
            print(
                f'  [BLOCK] Non-bank filter REMOVED [{bank_name}]: '
                f'"{p.get("name") or p.get("title")}" - {reason}'
            )
            removed += 1
        else:
            filtered.append(p)

    if removed:
        print(f'  [BLOCK] Non-bank filter: {removed} non-bank promotion(s) removed for {bank_name}')
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
║        • Content mentioning taipofire.gov.hk, hab033, cefs.gov.hk ║
║          or any gov.hk domain — these are NEVER bank promotions    ║
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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DETAILED EXTRACTION REQUIREMENTS FOR EACH FIELD:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

For EVERY promotion, extract COMPREHENSIVE details:

1. NAME: Full descriptive English name including:
   - Specific cash amounts (e.g., "HKD 800 Cash Reward")
   - Percentage rates (e.g., "Up to 3.88% p.a.")
   - Partner names (e.g., "Trip.com x Mox Credit Card")
   - Time periods if relevant (e.g., "Summer 2026 Promotion")

2. DESCRIPTION (2-3 DETAILED sentences - THIS IS CRITICAL):
   - WHAT: What exactly is being offered (cashback, bonus, fee waiver)
   - WHO: Who is eligible (new customers, existing users, minimum age)
   - HOW MUCH: Specific amounts, percentages, maximum caps
   - WHEN: Validity period, deadline, campaign dates
   - CONDITIONS: Minimum spend, required actions, exclusions
   - EXAMPLE: "New Mox Card holders can earn HKD 300 cashback after spending
     HKD 3,000 within the first 30 days of account opening. This promotion
     is exclusive to first-time Mox customers and runs until 31 Dec 2026."

3. HIGHLIGHT: One-line summary starting with an emoji:
   - 💰 for cash rewards: "💰 Earn HKD 300 welcome bonus"
   - 🎯 for discounts: "🎯 Get 8% off on Trip.com bookings"
   - 📈 for rates: "📈 Enjoy up to 3.88% p.a. interest"
   - 🆓 for freebies: "🆓 Zero platform fees for 90 days"
   - ✈️ for travel: "✈️ Collect 2 Asia Miles per USD 1 spent"

4. QUOTA/ELIGIBILITY (be specific):
   - Customer type: "New customers only" / "Existing customers eligible"
   - Age requirements: "Aged 18 or above"
   - Residency: "HKID holders only"
   - Income: "Minimum monthly income HKD 15,000"
   - Limits: "First 500 applicants" / "While stocks last"
   - Cap: "Maximum 1,000 redemptions"

5. COST/MINIMUM SPEND (exact figures):
   - Minimum spend: "Spend HKD 3,000 to qualify"
   - Required deposit: "Deposit HKD 50,000 for 3 months"
   - Entry fee: "Free to enter" / "HKD 100 application fee"
   - Tiered spending: "Tier 1: HKD 3,000; Tier 2: HKD 10,000"

6. PERIOD: Clear validity period:
   - Specific dates: "15 Mar 2026 to 30 Jun 2026"
   - From-start: "From 1 Apr 2026 to 31 Dec 2026"
   - Ongoing: "Ongoing (no end date)"

7. TC_LINK: Direct link to terms & conditions page

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXAMPLES OF HIGH-QUALITY EXTRACTIONS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLE 1 (Cashback Promotion):
Text: "Mox Credit Card Cashback - Earn 10% cashback on dining and entertainment,
plus 1% on all other purchases. New customers get HKD 300 sign-up bonus after
spending HKD 3,000 in the first month. No annual fee for the first year.
Valid until further notice."

Extract:
{{
  "name": "Mox Credit Card 10% Dining Cashback + HKD 300 Welcome Bonus",
  "types": ["消費", "迎新"],
  "is_bau": false,
  "start_date": null,
  "end_date": null,
  "period": "Ongoing",
  "highlight": "💰 Earn 10% cashback on dining + HKD 300 welcome bonus",
  "description": "Mox Credit Card offers 10% unlimited cashback on dining and entertainment
purchases, plus 1% on all other spending. New cardholders receive HKD 300 bonus
after completing HKD 3,000 spending within the first 30 days. No annual fee for
the first year, then HKD 500/year thereafter. Available to Hong Kong residents
aged 18+ with valid HKID.",
  "quota": "New customers only (first-time Mox Card holders)",
  "cost": "Minimum spend HKD 3,000 within 30 days to unlock HKD 300 bonus",
  "tc_link": "URL_PLACEHOLDER"
}}

EXAMPLE 2 (Time-Limited Deposit Promotion):
Text: "livi GoSave 3.88% Promotion - Open a GoSave account and enjoy 3.88% p.a.
interest rate on deposits up to HKD 500,000. Promotional rate valid for 6 months
from account opening. Minimum deposit HKD 10,000. Existing livi customers eligible.
Offer ends 30 June 2026."

Extract:
{{
  "name": "livi GoSave 3.88% p.a. Promotional Interest Rate (6 Months)",
  "types": ["存款", "新資金"],
  "is_bau": false,
  "start_date": "2026-01-01",
  "end_date": "2026-06-30",
  "period": "1 Jan 2026 to 30 Jun 2026",
  "highlight": "📈 Earn 3.88% p.a. on GoSave deposits for 6 months",
  "description": "livi bank offers 3.88% p.a. promotional interest rate on GoSave fixed
deposit accounts. The promotional rate applies to deposits up to HKD 500,000 for
a tenure of 6 months from account opening. Minimum initial deposit of HKD 10,000
required. Early withdrawal will result in forfeiture of promotional interest.
Available to existing livi customers and new users.",
  "quota": "Existing and new livi customers aged 18+",
  "cost": "Minimum deposit HKD 10,000; Lock-in period: 6 months",
  "tc_link": "URL_PLACEHOLDER"
}}

EXAMPLE 3 (Investment Fee Waiver):
Text: "ZA Bank Fund Investment - Invest in over 50 featured funds with zero
subscription fees. Normally 2-3% fee waived for all funds. Plus get priority
access to new fund launches. Promotion valid until 31 July 2026."

Extract:
{{
  "name": "ZA Bank Zero-Fee Fund Investment Platform (50+ Funds)",
  "types": ["投資"],
  "is_bau": false,
  "start_date": null,
  "end_date": "2026-07-31",
  "period": "Ongoing until 31 Jul 2026",
  "highlight": "🆓 Zero subscription fees on 50+ investment funds",
  "description": "ZA Bank provides commission-free access to over 50 curated investment
funds including equity, bond, and balanced portfolios. Standard subscription fees
of 2-3% are completely waived, saving investors significant costs. Features
include automated portfolio rebalancing, real-time fund performance tracking,
and priority allocation for new fund offerings. Suitable for both beginner and
experienced investors.",
  "quota": "All ZA Bank account holders (no minimum balance requirement)",
  "cost": "Free (zero subscription fees during promotional period)",
  "tc_link": "URL_PLACEHOLDER"
}}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WEBSITE TEXT TO ANALYSE:
────────────────────────────────────────────────────────────────────────
TEXT_PLACEHOLDER
────────────────────────────────────────────────────────────────────────

REMEMBER:
- Return ONLY the JSON array starting with [ and ending with ]
- NO markdown fences, NO explanations, NO additional text
- Extract ALL promotions you find - don't skip any
- Include SPECIFIC numbers, amounts, percentages, dates
- Write DETAILED descriptions (2-3 sentences minimum)
- If a field has no information, use empty string "" or null for dates
- For ongoing promotions with no end date, set end_date to null

If the text is entirely from a government or charity website, return [].
If you see any mention of taipofire.gov.hk, wang fuk court, hab033,
cefs.gov.hk, or tax deduction for donation — skip that content entirely."""

# ★ EleBank-specific fee context injected into the extraction prompt when
#   processing EleBank pages.  The AI uses these exact figures in its
#   'highlight' and 'description' fields and for correct is_bau tagging.
_ELEBANK_FEE_CONTEXT = """
[EleBank Stock Trading Fee Reference — factual, use for exact descriptions]
Non-fractional stock trading standard fee schedule (BAU — always available):
  HK-listed stocks: $0 commission per order + HKD 15 platform fee per order
  US-listed stocks: Commission USD 0.0049 per share (min USD 0.99 per order)
                   + Platform fee USD 0.005 per share (min USD 1.00 per order)
                   = Total USD 0.0099 per share, minimum USD 1.99 per order
These are the standard BAU rates — NOT a limited-time promotional discount.
Extract any TIME-LIMITED fee waivers or discounts on top of these rates as
separate non-BAU promotions with the relevant end_date.
"""


def _build_prompt(bank_name: str, url: str, text: str) -> str:
    today = datetime.now().strftime('%Y-%m-%d')
    prompt = (
        _PROMPT_TMPL
        .replace('BANK_NAME_PLACEHOLDER',  bank_name)
        .replace('URL_PLACEHOLDER',        url)
        .replace('TODAY_DATE_PLACEHOLDER', today)
        .replace('TEXT_PLACEHOLDER',       text)
    )
    # ★ Inject EleBank fee context (covers both new name and legacy 'Airstar' rows)
    if any(n in bank_name.lower() for n in ('elebank', 'ele bank', 'airstar')):
        prompt = prompt + '\n' + _ELEBANK_FEE_CONTEXT
    return prompt


# ── Claude API core ────────────────────────────────────────────────────────────────

def _init_claude() -> bool:
    """Initialize Claude API client."""
    global AI_AVAILABLE

    if Anthropic is None:
        logger.error('anthropic package not installed. Run: pip install anthropic')
        print('[ERROR] anthropic package not installed. Run: pip install anthropic')
        return False

    api_key = _get_anthropic_key()
    if not api_key:
        logger.error('ANTHROPIC_API_KEY not set')
        print('[ERROR] ANTHROPIC_API_KEY not set in environment')
        return False

    try:
        AI_AVAILABLE = True
        logger.info('Claude API initialized')
        print('[OK] Claude API ready')
        return True
    except Exception as exc:
        logger.error(f'Failed to initialize Claude client: {exc}')
        print(f'[ERROR] Claude init failed: {exc}')
        AI_AVAILABLE = False
        return False


def _get_anthropic_key():
    return os.environ.get('ANTHROPIC_API_KEY', '').strip()


def _call(messages: list, label: str = '') -> str:
    """Call Claude API directly, or use autonomous mode if no API key."""
    global AI_AVAILABLE

    # Check for autonomous mode (Claude Code built-in)
    from claude_bridge import is_autonomous_mode, request_ai_analysis

    if is_autonomous_mode():
        print(f'  [AUTONOMOUS] Claude Code mode active - no API needed')
        # In autonomous mode, return a marker that Claude Code will intercept
        # For now, return empty to indicate AI unavailable (fallback to manual)
        return ''

    if not AI_AVAILABLE:
        if Anthropic is not None and _ANTHROPIC_API_KEY:
            _init_claude()
        if not AI_AVAILABLE:
            return ''

    t = time.monotonic()
    try:
        client = Anthropic(api_key=_ANTHROPIC_API_KEY)

        # Convert messages format if needed
        system_message = "You are a helpful AI assistant for HK virtual bank promotions tracking."

        result = client.messages.create(
            model="claude-3-7-sonnet-20250219",
            max_tokens=4096,
            system=system_message,
            messages=messages,
        )

        elapsed = time.monotonic() - t
        tag = f' [{label}]' if label else ''
        response_text = result.content[0].text
        logger.info(f'Claude API call{tag} -> {len(response_text)} chars in {elapsed:.1f}s')
        print(f'  [DEBUG] AI (claude-3.7-sonnet){tag} -> {len(response_text)} chars in {elapsed:.1f}s')
        if len(response_text) < 50:
            print(f'  [DEBUG] Full response: {repr(response_text)}')
        return response_text
    except Exception as exc:
        logger.error(f'Claude API call error: {type(exc).__name__}: {exc}')
        print(f'  [ERR] Claude API call error: {exc}')
        return ''


# ── Init ──────────────────────────────────────────────────────────────────────

def init_ai() -> bool:
    """Initialize AI using Claude API."""
    try:
        if Anthropic is None:
            print('[ERR] anthropic package not installed. Run: pip install anthropic')
            AI_AVAILABLE = False
            return False

        api_key = _get_anthropic_key()
        if not api_key:
            print('[WARNING] ANTHROPIC_API_KEY not set - AI disabled')
            print('   Set it with: export ANTHROPIC_API_KEY=your-key-here')
            AI_AVAILABLE = False
            return False

        return _init_claude()
    except Exception as exc:
        print(f'[ERROR] AI init failed: {exc}')
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
    print(f'  [WARN] JSON parse failed. First 200 chars: {raw[:200]}')
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
        print(f'  [WARN] JSON object parse failed: {exc}. First 200 chars: {raw[:200]}')
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
                print(f'    [BAU] BAU override: {p.get("name") or p.get("title")}')
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

# ★ CHANGED: 'airstar' → 'elebank' (+ kept 'airstar' for legacy data compat)
_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    'HK Stock Trading': [
        'hk stock', 'hong kong stock', 'hkex', 'local stock',
        'hk securities', 'hk shares', 'hong kong shares',
        'hk brokerage', 'stock', 'securities', 'brokerage', 'ipo',
        'trading fee', 'platform fee', '$0 commission', 'commission',
        'powerdraw', 'free stock', 'equities', 'share trading',
        'elebank', 'airstar',
    ],
    'US Stock Trading': [
        'us stock', 'us equities', 'us securities', 'us shares',
        'american stock', 'nyse', 'nasdaq', 'us market',
        'us brokerage', '$0 commission', 'commission',
        'trading fee', 'platform fee', 'stock', 'equities',
        'elebank', 'airstar',
    ],
    'Stock Trading': [
        'stock', 'securities', 'brokerage', 'ipo',
        'trading fee', 'platform fee', '$0 commission', 'commission',
        'powerdraw', 'free stock', 'hk stock', 'us stock',
        'equities', 'share trading',
        'elebank', 'airstar',
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
                f'  [WARN] Vague-pattern flag [{cat}] "{bank}" — '
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
        print(f'  [BLOCK] Evidence gate total: {reject_count} vague winner(s) nullified')
    return validated


# ── Stock trading total-cost validator ────────────────────────────────────────

_STOCK_CATS = {'HK Stock Trading', 'US Stock Trading', 'Stock Trading'}
_ZA_NAMES   = {'za bank', 'za', 'za invest'}

# ★ EleBank (formerly Airstar Bank) — includes legacy names for backward compat
_ELEBANK_NAMES = {
    'elebank', 'ele bank', 'elebank bank',
    'airstar bank', 'airstar', 'airstar invest',   # legacy names
}
_AIRSTAR_NAMES = _ELEBANK_NAMES  # legacy alias

# ★ PADB (formerly PAObank)
_PADB_NAMES = {'padb', 'paobank', 'pao bank', 'pao'}

# ── EleBank stock fee constants ───────────────────────────────────────────────
# HK stocks: $0 commission + HKD 15 platform fee per order
# US stocks: USD 0.0049/share comm (min USD 0.99) + USD 0.005/share platform (min USD 1.00)
#            = USD 0.0099/share total, min USD 1.99/order  ← same as ZA Bank US
_ELEBANK_HK_COMMISSION        = 0.0
_ELEBANK_HK_PLATFORM_FEE      = 15.0   # HKD per order
_ELEBANK_HK_TOTAL_COST        = 15.0   # HKD per order
_ELEBANK_US_COMM_PER_SHARE    = 0.0049 # USD
_ELEBANK_US_COMM_MIN          = 0.99   # USD per order
_ELEBANK_US_PLAT_PER_SHARE    = 0.005  # USD
_ELEBANK_US_PLAT_MIN          = 1.00   # USD per order
_ELEBANK_US_TOTAL_PER_SHARE   = 0.0099 # USD (= comm + platform, same as ZA Bank)
_ELEBANK_US_MIN_TOTAL         = 1.99   # USD per order (0.99 + 1.00 = same as ZA Bank)

# ── ZA Bank fee constants ─────────────────────────────────────────────────────
_ZA_HK_PLATFORM_FEE_HKD  = 18.0   # HKD per order
_ZA_US_PLATFORM_FEE_USD   = 1.99   # USD minimum per order

_CHARGES_COMMISSION_RE = re.compile(
    r'usd\s*[\d]+\.[\d]+\s*/\s*share'
    r'|usd\s*[\d]+\.[\d]+\s*per\s*share'
    r'|hkd\s*[\d]+\.?[\d]*\s*(per|/)\s*(trade|lot|share|order)'
    r'|0\.\d+%\s*(brokerage|commission)'
    r'|commission\s+(of|at|is|:)\s*[\d]'
    r'|[\d]+\.[\d]+\s*usd\s*per',
    re.IGNORECASE,
)

_ZERO_PLATFORM_FEE_RE = re.compile(
    r'zero\s+platform\s+fee'
    r'|\$\s*0\s+platform'
    r'|no\s+platform\s+fee'
    r'|platform\s+fee[\s:]+\$?\s*0\b'
    r'|free\s+platform',
    re.IGNORECASE,
)

_USD_PER_SHARE_RE = re.compile(r'usd\s*([\d]+\.[\d]+)\s*(?:/|per)\s*share', re.IGNORECASE)


def _us_breakeven_shares(per_share_usd: float) -> float:
    if per_share_usd <= 0:
        return float('inf')
    return _ZA_US_PLATFORM_FEE_USD / per_share_usd


def _validate_stock_trading_winners(best_for: list) -> list:
    overrides = 0
    for i, entry in enumerate(best_for):
        cat  = (entry.get('category') or '').strip()
        bank = (entry.get('bank')     or '').strip()

        if cat not in _STOCK_CATS:
            continue

        # ── ZA Bank path ──────────────────────────────────────────────────────
        if bank.lower() in _ZA_NAMES:
            detail = (entry.get('detail') or '')
            if 'platform fee' not in detail.lower():
                best_for[i] = {
                    **entry,
                    'detail': (
                        detail.rstrip('. ') +
                        '; platform fee applies '
                        f'(HK: HKD {_ZA_HK_PLATFORM_FEE_HKD:.0f}/order, '
                        f'US: USD {_ZA_US_PLATFORM_FEE_USD:.2f}/order minimum)'
                    ),
                }
            # Ensure EleBank appears in similar_banks for stock trading
            existing_similar = [s.lower() for s in (best_for[i].get('similar_banks') or [])]
            if not any(n in existing_similar for n in _ELEBANK_NAMES):
                best_for[i] = {
                    **best_for[i],
                    'similar_banks': (
                        ['EleBank'] + list(best_for[i].get('similar_banks') or [])
                    ),
                }
            continue

        # ── EleBank path ──────────────────────────────────────────────────────
        # HK stocks: EleBank total HKD 15 < ZA Bank HKD 18 → accept win
        # US stocks: EleBank total USD 1.99 min = ZA Bank USD 1.99 → accept win (tied)
        if bank.lower() in _ELEBANK_NAMES:
            detail = (entry.get('detail') or '')

            if cat == 'HK Stock Trading':
                # Annotate detail with exact HKD 15 total cost if missing
                if (
                    'hkd 15' not in detail.lower()
                    and 'hkd15' not in detail.lower()
                    and 'platform fee' not in detail.lower()
                ):
                    best_for[i] = {
                        **entry,
                        'detail': (
                            detail.rstrip('. ') +
                            f'; total cost HKD {_ELEBANK_HK_TOTAL_COST:.0f}/order '
                            f'($0 commission + HKD {_ELEBANK_HK_PLATFORM_FEE:.0f} platform fee). '
                            f'Beats ZA Bank (HKD {_ZA_HK_PLATFORM_FEE_HKD:.0f} platform fee/order '
                            f'despite $0 commission) and PADB (charges brokerage commission per trade).'
                        ),
                    }
                print(
                    f'  [OK] HK stock: EleBank total cost HKD {_ELEBANK_HK_TOTAL_COST:.0f}/order — '
                    f'lower than ZA Bank HKD {_ZA_HK_PLATFORM_FEE_HKD:.0f}. Accepted.'
                )
                # Ensure ZA Bank and PADB appear as similar banks
                existing_similar = [s.lower() for s in (best_for[i].get('similar_banks') or [])]
                added_similar    = list(best_for[i].get('similar_banks') or [])
                if not any(n in existing_similar for n in _ZA_NAMES):
                    added_similar = ['ZA Bank'] + added_similar
                if not any(n in existing_similar for n in _PADB_NAMES):
                    added_similar = added_similar + ['PADB']
                best_for[i] = {**best_for[i], 'similar_banks': added_similar}

            elif cat in ('US Stock Trading', 'Stock Trading'):
                # EleBank US total = ZA Bank US total (both USD 0.0099/share, min USD 1.99)
                if 'total cost' not in detail.lower() and 'usd 1.99' not in detail.lower():
                    best_for[i] = {
                        **entry,
                        'detail': (
                            detail.rstrip('. ') +
                            f'; EleBank US stock: '
                            f'USD {_ELEBANK_US_COMM_PER_SHARE}/share commission '
                            f'(min USD {_ELEBANK_US_COMM_MIN}) + '
                            f'USD {_ELEBANK_US_PLAT_PER_SHARE}/share platform fee '
                            f'(min USD {_ELEBANK_US_PLAT_MIN:.2f}) '
                            f'= USD {_ELEBANK_US_TOTAL_PER_SHARE}/share total, '
                            f'min USD {_ELEBANK_US_MIN_TOTAL:.2f}/order '
                            f'(same effective minimum as ZA Bank).'
                        ),
                    }
                print(
                    f'  [OK] US stock: EleBank selected — total cost matches ZA Bank '
                    f'(both min USD {_ELEBANK_US_MIN_TOTAL:.2f}/order). Accepted.'
                )
                existing_similar = [s.lower() for s in (best_for[i].get('similar_banks') or [])]
                added_similar    = list(best_for[i].get('similar_banks') or [])
                if not any(n in existing_similar for n in _ZA_NAMES):
                    added_similar = ['ZA Bank'] + added_similar
                if not any(n in existing_similar for n in _PADB_NAMES):
                    added_similar = added_similar + ['PADB']
                best_for[i] = {**best_for[i], 'similar_banks': added_similar}

            continue
        # ↑ END EleBank path

        detail             = (entry.get('detail') or '')
        charges_commission = bool(_CHARGES_COMMISSION_RE.search(detail))
        has_zero_platform  = bool(_ZERO_PLATFORM_FEE_RE.search(detail))

        if not charges_commission:
            continue

        if cat in ('US Stock Trading', 'Stock Trading') and has_zero_platform:
            m = _USD_PER_SHARE_RE.search(detail)
            if m:
                per_share           = float(m.group(1))
                breakeven           = _us_breakeven_shares(per_share)
                competitor_cost_200 = 200 * per_share
                za_cost_200         = _ZA_US_PLATFORM_FEE_USD  # USD 1.99

                if competitor_cost_200 > za_cost_200:
                    print(
                        f'  [OVERRIDE] US stock total-cost OVERRIDE [{cat}]: '
                        f'"{bank}" @ USD {per_share}/share x 200 = USD {competitor_cost_200:.2f} '
                        f'vs ZA Bank/EleBank USD {za_cost_200:.2f} platform fee. '
                        f'ZA Bank is cheaper above {breakeven:.0f} shares - overriding.'
                    )
                    best_for[i] = {
                        **entry,
                        'bank':   'ZA Bank',
                        'detail': (
                            f'$0 brokerage commission for US stocks via ZA Invest; '
                            f'platform fee USD 0.0099/share (min USD {_ZA_US_PLATFORM_FEE_USD:.2f}/order). '
                            f'Total cost at 200 shares: USD {za_cost_200:.2f}. '
                            f'{bank} charges USD {per_share}/share commission + $0 platform = '
                            f'USD {competitor_cost_200:.2f} at 200 shares — '
                            f'ZA Bank is cheaper for trades above {breakeven:.0f} shares. '
                            f'EleBank also offers USD {_ELEBANK_US_TOTAL_PER_SHARE}/share total '
                            f'(min USD {_ELEBANK_US_MIN_TOTAL:.2f}/order) — same as ZA Bank minimum.'
                        ),
                        'is_bau':  True,
                        'similar_banks': (
                            [bank, 'EleBank'] + [
                                b for b in (entry.get('similar_banks') or [])
                                if b.lower() not in _ZA_NAMES
                                and b != bank
                                and b.lower() not in _ELEBANK_NAMES
                            ]
                        ),
                        'why_others_lose': (
                            f'{bank} charges USD {per_share}/share commission (no platform fee). '
                            f'ZA Bank charges $0 commission + USD {_ZA_US_PLATFORM_FEE_USD:.2f} flat '
                            f'platform fee (minimum). '
                            f'EleBank: USD {_ELEBANK_US_COMM_PER_SHARE}/share commission '
                            f'(min USD {_ELEBANK_US_COMM_MIN}) + '
                            f'USD {_ELEBANK_US_PLAT_PER_SHARE}/share platform fee '
                            f'(min USD {_ELEBANK_US_PLAT_MIN:.2f}) = same USD {_ELEBANK_US_MIN_TOTAL:.2f} minimum. '
                            f'For trades of {breakeven:.0f}+ shares, ZA Bank/EleBank total cost is lower. '
                            f'Most retail investors trade 200+ shares, making ZA Bank/EleBank cheaper overall.'
                        ),
                    }
                    overrides += 1
                else:
                    print(
                        f'  [OK] US stock total-cost KEPT [{cat}]: '
                        f'"{bank}" @ USD {per_share}/share × 200 = USD {competitor_cost_200:.2f} '
                        f'< ZA Bank/EleBank USD {za_cost_200:.2f} for 200-share benchmark. '
                        f'Break-even: {breakeven:.0f} shares.'
                    )
                    if 'total cost' not in detail.lower() and 'vs za' not in detail.lower():
                        best_for[i] = {
                            **entry,
                            'detail': (
                                detail.rstrip('. ') +
                                f'; total cost at 200 shares: USD {competitor_cost_200:.2f} '
                                f'vs ZA Bank/EleBank USD {za_cost_200:.2f} '
                                f'(break-even: {breakeven:.0f} shares)'
                            ),
                        }
                    # Ensure EleBank in similar_banks
                    existing_similar = [s.lower() for s in (best_for[i].get('similar_banks') or [])]
                    if not any(n in existing_similar for n in _ELEBANK_NAMES):
                        best_for[i] = {
                            **best_for[i],
                            'similar_banks': (
                                list(best_for[i].get('similar_banks') or []) + ['EleBank']
                            ),
                        }
            else:
                print(
                    f'  [OVERRIDE] Stock trading OVERRIDE [{cat}]: '
                    f'"{bank}" charges commission (rate unclear). '
                    f'ZA Bank $0 commission is safer default.'
                )
                best_for[i] = {
                    **entry,
                    'bank':   'ZA Bank',
                    'detail': (
                        '$0 brokerage commission for US stocks via ZA Invest; '
                        f'platform fee USD 0.0099/share (min USD {_ZA_US_PLATFORM_FEE_USD:.2f}/order). '
                        f'{bank} charges commission (exact rate unspecified). '
                        f'EleBank also ties ZA Bank: USD {_ELEBANK_US_TOTAL_PER_SHARE}/share total, '
                        f'min USD {_ELEBANK_US_MIN_TOTAL:.2f}/order.'
                    ),
                    'is_bau':  True,
                    'similar_banks': (
                        [bank, 'EleBank'] + [
                            b for b in (entry.get('similar_banks') or [])
                            if b.lower() not in _ZA_NAMES
                            and b != bank
                            and b.lower() not in _ELEBANK_NAMES
                        ]
                    ),
                    'why_others_lose': (
                        f'{bank} charges per-trade commission; ZA Bank is $0 commission. '
                        f'EleBank: USD {_ELEBANK_US_COMM_PER_SHARE}/share commission + '
                        f'USD {_ELEBANK_US_PLAT_PER_SHARE}/share platform = '
                        f'USD {_ELEBANK_US_TOTAL_PER_SHARE}/share total (min USD {_ELEBANK_US_MIN_TOTAL:.2f}) '
                        f'— same minimum as ZA Bank.'
                    ),
                }
                overrides += 1

        elif cat == 'HK Stock Trading' and charges_commission:
            # Other bank charges commission for HK stocks
            # Annotate for comparison against EleBank (HKD 15) and ZA Bank (HKD 18)
            if 'platform fee' not in detail.lower() and 'total cost' not in detail.lower():
                best_for[i] = {
                    **entry,
                    'detail': (
                        detail.rstrip('. ') +
                        '; zero platform fee. '
                        f'Compare total cost: commission + $0 platform vs '
                        f'EleBank HKD {_ELEBANK_HK_TOTAL_COST:.0f} total ($0 commission + '
                        f'HKD {_ELEBANK_HK_PLATFORM_FEE:.0f} platform fee) and '
                        f'ZA Bank HKD {_ZA_HK_PLATFORM_FEE_HKD:.0f} total ($0 commission + '
                        f'HKD {_ZA_HK_PLATFORM_FEE_HKD:.0f} platform fee).'
                    ),
                }
            print(
                f'  [INFO] HK stock winner kept [{cat}]: '
                f'"{bank}" charges commission + $0 platform. '
                f'EleBank (HKD {_ELEBANK_HK_TOTAL_COST:.0f}) and '
                f'ZA Bank (HKD {_ZA_HK_PLATFORM_FEE_HKD:.0f}) offer lower total cost.'
            )
            # Ensure EleBank and ZA Bank both in similar_banks
            existing_similar = [s.lower() for s in (best_for[i].get('similar_banks') or [])]
            added_similar    = list(best_for[i].get('similar_banks') or [])
            if not any(n in existing_similar for n in _ELEBANK_NAMES):
                added_similar = ['EleBank'] + added_similar
            if not any(n in existing_similar for n in _ZA_NAMES):
                added_similar = ['ZA Bank'] + added_similar
            best_for[i] = {**best_for[i], 'similar_banks': added_similar}

    if overrides:
        print(f'  [OVERRIDE] Stock trading total-cost override: {overrides} winner(s) updated')
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
            f'  [FILL] Strength cross-check FILLED [{cat}] -> {best_bank}: '
            f'"{best_detail[:80]}"'
        )
        best_for[i] = {**entry, 'bank': best_bank, 'detail': best_detail, 'is_bau': is_bau_guess}
        filled += 1

    if filled:
        print(f'  [FILL] Cross-check: {filled} slot(s) filled from bank_analysis.strengths')

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
            try:
                raw    = _call([{'role': 'user', 'content': prompt}], label=bank_id)
                parsed = _parse_array(raw)
                if parsed:
                    results   = parsed
                    bau_count = sum(1 for p in parsed if p.get('is_bau'))
                    logger.info(f'Successfully extracted {len(results)} promotions for {bank_name} ({bau_count} BAU)')
                    print(
                        f'  [INFO] Text -> {len(results)} promotions for {bank_name} '
                        f'({bau_count} BAU)'
                    )
                    break
                else:
                    logger.warning(f'AI returned empty result for {bank_name} on attempt {attempt + 1}')
                    if attempt == 0:
                        print(f'  [RETRY] Retry AI for {bank_name}...')
            except Exception as exc:
                logger.error(f'AI extraction error for {bank_name}: {type(exc).__name__}: {exc}')
                if attempt == 0:
                    safe_exc = str(exc).encode('ascii', 'replace').decode('ascii')
                    print(f'  [WARN] AI error for {bank_name}: {safe_exc} - retrying...')
        else:
            logger.error(f'All AI attempts failed for {bank_name}')
            print(f'  [ERR] Both attempts failed for {bank_name}')
    else:
        print(f'  [WARN] Text too short ({len(clean)} chars) for {bank_name}')

    results = _stamp(results, bank_id, bank_name, default_url)
    results = _apply_bau_overrides(results, bank_id)
    results = _filter_bank_relevant_promotions(results, bank_name)

    print(f'  [OK] Total: {len(results)} promotions for {bank_name}')
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

EleBank HK stock trading (all refer to same BAU fee schedule):
  "HK Stock Trading with $0 Commission" = "Lifetime $0 Commissions on HK Stocks"
  = "HK Stock $0 Commission per Order" = "HK Stock $0 Commission + HKD 15 Platform Fee"

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
                    print(f'  [RETRY] Retry ai_dedup_titles for {bank_name}...')
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
                print(f'  [AI] ai_dedup_titles [{bank_name}]: {len(dup_map)} duplicate(s)')
            return dup_map
        except Exception as exc:
            if attempt == 0:
                print(f'  [WARN] ai_dedup_titles [{bank_name}] attempt 1 failed: {exc!r} - retrying')
            else:
                print(f'  [WARN] ai_dedup_titles [{bank_name}]: {exc!r} - skipping')
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

13. EleBank HK stock BAU (formerly Airstar Bank):
    "Lifetime $0 Commissions on HK Stocks" ↔ "HK Stock Trading with $0 Commission"
    ↔ "HK Stock $0 Commission per Order" → MATCH

14. When uncertain → declare MATCH
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
                    print(f'  [RETRY] Retry ai_match_against_existing for {bank_name}...')
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
                '0 matches - all appear genuinely new'
            )
            print(f'  [AI] ai_match_against_existing [{bank_name}]: {msg}')
            return result_map
        except Exception as exc:
            if attempt == 0:
                print(f'  [WARN] ai_match_against_existing [{bank_name}] attempt 1: {exc!r} - retrying')
            else:
                print(f'  [WARN] ai_match_against_existing [{bank_name}]: {exc!r} - skipping')
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


# ★ CHANGED: 'airstar' → 'elebank' (+ legacy 'airstar' kept for existing DB data)
_DIAGNOSTIC_CATEGORIES: list[tuple[str, list[str]]] = [
    ('HK Stock Trading',   ['投資', 'hk stock', 'hong kong stock', 'hkex', 'local stock',
                             'securities', 'brokerage', 'ipo', 'commission', 'trading fee',
                             'elebank', 'airstar']),
    ('US Stock Trading',   ['投資', 'us stock', 'us equities', 'nasdaq', 'nyse',
                             'american stock', 'commission', 'trading fee',
                             'elebank', 'airstar']),
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
    print('[STATS]  INSIGHTS INPUT DIAGNOSTIC')
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
        ) or '[WARN] NONE'

        sparse_flag = (
            '  [WARN] SPARSE — may cause None slots'
            if len(promos) < _SPARSE_THRESHOLD
            else '  [OK]'
        )
        print(
            f'  [STATS] {bank:<20}: {len(non_bau_promos):>2} active'
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
            print(f'    [OK] {cat_name:<42} -> {", ".join(covered_by)}')
        else:
            print(f'    [ERR] {cat_name:<42} -> NO DATA - will output None')

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
            f'  [WARN] SPARSE BANKS: {sparse} (each has < {_SPARSE_THRESHOLD} promos)\n'
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
            print(f'  [WARN] supplement_from_db: DB fetch failed for "{bank}": {exc}')
            continue
        if not db_promos:
            print(f'  [WARN] supplement_from_db: no DB rows for "{bank}"')
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
            f'  [MERGE] supplement_from_db: "{bank}" '
            f'{"added " + str(added) + " from DB -> now " + str(len(promos)) + " total" if added else "no new titles found in DB"}'
        )

    if supplemented_total:
        print(f'  [MERGE] supplement_from_db: {supplemented_total} DB row(s) merged total')
    return promotions_by_bank


# ── Strategic insights — main entry point ────────────────────────────────────

def generate_strategic_insights(
    promotions_by_bank: dict,
    db_fetch_fn=None,
) -> Optional[dict]:
    if not AI_AVAILABLE:
        print('[WARN] AI not available - skipping strategic insights')
        return None

    _diagnose_input_data(promotions_by_bank)

    sparse_banks = _check_sparse_banks(promotions_by_bank)
    if sparse_banks:
        if db_fetch_fn is not None:
            promotions_by_bank = supplement_from_db(promotions_by_bank, db_fetch_fn)
            print('  [STATS] POST-SUPPLEMENT DIAGNOSTIC:')
            _diagnose_input_data(promotions_by_bank)
        else:
            print(
                '  [WARN] Sparse banks found but no db_fetch_fn provided.\n'
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
        print('[WARN] No promotions data - skipping strategic insights')
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
• similar_banks: ALL other banks offering a similar promotion in this category.
  ⚠️  For HK Stock Trading and US Stock Trading you MUST include BOTH
      "ZA Bank" and "EleBank" in similar_banks (whichever one did not win),
      because both offer highly competitive stock trading fees.
• why_others_lose: specific comparative reason using fees, caps, expiry dates.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 8 — SELF-CONSISTENCY CHECK
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Before writing final JSON: verify that every "None" in best_for is NOT
contradicted by a matching strength in bank_analysis for the same category.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 9 — STOCK TRADING: TOTAL COST ANALYSIS (CRITICAL)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️  THE ONLY CORRECT WAY TO COMPARE STOCK TRADING COSTS:

  Total cost per order = Commission fee + Platform fee

  EleBank (formerly Airstar Bank):
    • HK stocks: $0 commission + HKD 15 platform fee per order
                 → total cost HKD 15/order
    • US stocks: USD 0.0049/share commission (min USD 0.99/order)
                 + USD 0.005/share platform fee (min USD 1.00/order)
                 = USD 0.0099/share total, min USD 1.99/order
    ✅ EleBank wins HK Stock Trading:
       HKD 15/order < ZA Bank HKD 18/order.
       Pick EleBank as HK Stock Trading winner whenever EleBank HK stock
       trading data appears in the scraped promotions above.
    ℹ️  EleBank ties ZA Bank for US Stock Trading:
       both cost USD 0.0099/share total with min USD 1.99/order.
    ❌ NEVER say "EleBank charges no platform fee" — HK stocks cost HKD 15/order.

  ZA Bank (via ZA Invest):
    • Commission:   $0 (zero brokerage commission for ALL stocks)
    • Platform fee: HKD 18/order (HK stocks)
                    USD 0.0099/share, min USD 1.99/order (US stocks)
    ✅ Pick ZA Bank as HK Stock Trading winner ONLY when EleBank HK stock
       data is absent from the input above (EleBank is cheaper at HKD 15).
    ✅ Pick ZA Bank as US Stock Trading winner by default when EleBank
       ties (simpler $0-commission fee structure).
    ❌ NEVER say "ZA Bank charges commission" — factually incorrect.

  PADB (formerly PAObank):
    • HK stocks: brokerage commission charged + $0 platform fee
    • US stocks: USD 0.012/share commission + $0 platform fee

WINNER PRIORITY — HK Stock Trading:
  1st choice: EleBank  ($0 commission + HKD 15 platform fee = HKD 15 total)
  2nd choice: ZA Bank  ($0 commission + HKD 18 platform fee = HKD 18 total)
  3rd: PADB/others     (commission-based, total cost varies)
  → Always list both non-winners in similar_banks.

WINNER PRIORITY — US Stock Trading:
  EleBank and ZA Bank are effectively tied (both USD 0.0099/share, min USD 1.99/order):
    ZA Bank:  $0 commission + USD 0.0099/share platform fee, min USD 1.99/order
    EleBank:  USD 0.0049/share comm (min $0.99) + USD 0.005/share platform (min $1.00)
              = USD 0.0099/share total, min USD 1.99/order
  → Default to ZA Bank as winner when tied (simpler $0-commission structure).
  → PADB costs more: USD 0.012/share + $0 platform = USD 2.40 at 200 shares.
  → Always list EleBank in similar_banks for US Stock Trading.

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
      "bank":           "EleBank",
      "detail":         "$0 commission + HKD 15 platform fee per order → total HKD 15/order. Beats ZA Bank (HKD 18 platform fee/order despite $0 commission) and PADB (charges brokerage commission per trade).",
      "is_bau":         true,
      "similar_banks":  ["ZA Bank", "PADB"],
      "why_others_lose":"ZA Bank charges HKD 18 platform fee per order (despite $0 commission), making total cost HKD 18/order. PADB charges brokerage commission per HK trade. EleBank's HKD 15 total cost is the lowest among all 8 banks."
    }},
    {{
      "category":       "US Stock Trading",
      "bank":           "ZA Bank",
      "detail":         "$0 brokerage commission + USD 0.0099/share platform fee (min USD 1.99/order). EleBank ties: USD 0.0049/share commission (min $0.99) + USD 0.005/share platform (min $1.00) = USD 0.0099/share, min USD 1.99/order. PADB charges USD 0.012/share = USD 2.40 at 200 shares.",
      "is_bau":         true,
      "similar_banks":  ["EleBank", "PADB"],
      "why_others_lose":"EleBank effectively ties ZA Bank for US stocks (both min USD 1.99/order; USD 0.0099/share total). ZA Bank chosen as default winner for simpler $0-commission structure. PADB charges USD 0.012/share commission + $0 platform = USD 2.40 at 200 shares — more expensive than ZA Bank/EleBank."
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
    "EleBank": {{
      "focus": "short keywords",
      "strengths": ["$0 commission + HKD 15 platform fee for HK stocks (total HKD 15/order — lowest among all banks)", "s2", "s3"],
      "expiring_alert": "",
      "vs_za_pros": "HK stock total cost HKD 15/order vs ZA Bank HKD 18/order — EleBank saves HKD 3 per HK trade. US stock cost tied with ZA Bank (both USD 0.0099/share, min USD 1.99).",
      "vs_za_cons": "cons vs ZA Bank"
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
        print('[ERR] Strategic insights: empty response from AI')
        return None

    result = _parse_object(raw)
    if result is None:
        print('[ERR] Strategic insights: JSON parse failed')
        return None

    # ── Post-processing pipeline ──────────────────────────────────────────────
    result['best_for'] = _validate_best_for_evidence(result.get('best_for', []))
    result['best_for'] = _validate_stock_trading_winners(result.get('best_for', []))
    result             = _cross_check_best_for_from_strengths(result, promotions_by_bank)

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
            f'  [WARN] {none_wins} best_for slot(s) still None after all fixes: {none_cats}\n'
            f'     ↳ Check diagnostic above — these categories had no input data.'
        )

    print(
        f'[OK] Strategic insights generated '
        f'({bau_wins} BAU winner(s), {none_wins} None slot(s))'
    )
    return result


# ── Product Extraction ───────────────────────────────────────────────────────

def extract_products(bank_id: str, bank_name: str, text: str) -> list[dict]:
    """
    Extract core banking products from scraped website text.

    Products include:
    - Deposit products (savings accounts, time deposits, multi-currency accounts)
    - Card products (debit cards, credit cards)
    - Investment products (US/HK stock trading, fund investment, crypto trading)
    - Loan products (personal loans, mortgage loans)

    Returns a list of product dicts with fields:
    - product_name: Name of the product
    - category: One of 'deposit', 'card', 'investment', 'loan'
    - subcategory: More specific type (e.g., 'savings', 'stocks', 'funds')
    - description: Brief description
    - features: List of key features
    - interest_rate: Interest rate if applicable
    - fees: Fee structure
    - eligibility: Eligibility criteria
    - url: Source URL
    """
    if not AI_AVAILABLE or len(text.strip()) < 500:
        logger.warning(f'Skipping product extraction for {bank_name}: insufficient text or AI unavailable')
        return []

    # Text is already cleaned by scraper, use as-is
    prompt = f"""You are a banking product analyst. Extract all CORE BANKING PRODUCTS from the following bank website content.

TARGET BANK: {bank_name}

PRODUCT CATEGORIES TO IDENTIFY:
1. DEPOSIT PRODUCTS: Savings accounts, time deposits, multi-currency accounts, high-yield accounts, fixed deposits
2. CARD PRODUCTS: Debit cards, credit cards, prepaid cards, virtual cards
3. INVESTMENT PRODUCTS: Stock trading (US/HK), fund investment, cryptocurrency trading, bond investment, robo-advisory
4. LOAN PRODUCTS: Personal loans, mortgage loans, business loans, car loans, education loans

EXTRACTION RULES:
- Focus on PERMANENT product offerings, NOT time-limited promotions
- A product is a core banking service that exists continuously (e.g., "High Yield Savings Account")
- A promotion is a temporary offer (e.g., "0.5% extra interest for new accounts this month")
- If you see both, extract the underlying PRODUCT and note any promotional features separately
- Each product should be unique - don't list the same product twice with different names

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DETAILED EXTRACTION REQUIREMENTS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

For EVERY product, extract COMPREHENSIVE details including:

1. PRODUCT NAME: Clear, specific product name (not generic terms)

2. CATEGORY & SUBCATEGORY:
   - deposit: savings, time-deposit, multi-currency, high-yield, fixed-deposit
   - card: debit-card, credit-card, prepaid-card, virtual-card
   - investment: stocks, funds, crypto, bonds, robo-advisor
   - loan: personal-loan, mortgage, business-loan, car-loan, education-loan

3. DESCRIPTION (2-3 detailed sentences):
   - What the product is and who it's for
   - Key benefits and unique selling points
   - Specific numbers: rates, limits, percentages where available

4. FEATURES (list 3-7 specific features):
   - Interest rates or returns (e.g., "Up to 3.88% p.a.")
   - Fee structures (e.g., "$0 commission", "No annual fee")
   - Limits and thresholds (e.g., "No minimum deposit")
   - Accessibility (e.g., "24/7 mobile banking", "Instant transfers")
   - Special conditions (e.g., "Available to HK ID holders only")
   - Partner services (e.g., "Integration with The Club rewards")

5. INTEREST RATE (if applicable):
   - Exact rate: "3.88% p.a." or "Up to 3.88% p.a."
   - Rate conditions: "For first $500,000 balance" or "With monthly salary transfer"
   - If tiered: "1.8% on first HKD 100k, 3.88% on next HKD 400k"
   - Empty string if N/A (cards/investments)

6. FEES (detailed breakdown):
   - Monthly/annual fees: "HKD 50/month" or "No annual fee"
   - Transaction fees: "HKD 15 per withdrawal"
   - Trading fees: "$0 commission + HKD 18 platform fee"
   - Penalties: "Early withdrawal penalty: 7 days interest"
   - Empty string if free or N/A

7. ELIGIBILITY (specific requirements):
   - Age: "18 years or older"
   - Residency: "HK residents with valid HKID"
   - Income: "Minimum monthly income HKD 15,000"
   - Employment: "Full-time employed or self-employed"
   - Credit: "Good credit standing required"
   - Empty string if standard (HK residents 18+)

8. MINIMUM REQUIREMENTS (if applicable):
   - Initial deposit: "Minimum initial deposit HKD 10,000"
   - Balance: "Minimum balance HKD 5,000 to avoid fees"
   - Transfer amount: "Minimum transfer HKD 100"

9. URL: Source page URL for reference

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXAMPLES OF DETAILED EXTRACTION:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

GOOD EXAMPLE 1 (Deposit Product):
Text: "liviSave Preferential Savings Rate - Enjoy up to 3.88% p.a. on your GoSave account.
No minimum deposit required. Open an account in just 3 minutes with your HKID.
Perfect for parking spare cash with instant access. Available to Hong Kong residents aged 18+."

Extract:
{{
  "product_name": "liviSave Preferential Savings Account",
  "category": "deposit",
  "subcategory": "high-yield",
  "description": "High-yield savings account offering preferential interest rates up to 3.88% p.a.
Designed for flexible savings with no lock-in period and instant access to funds.",
  "features": [
    "Up to 3.88% p.a. preferential interest rate",
    "No minimum deposit requirement",
    "Account opening in 3 minutes",
    "Instant access to funds anytime",
    "Managed via livi mobile app"
  ],
  "interest_rate": "Up to 3.88% p.a.",
  "fees": "",
  "eligibility": "Hong Kong residents aged 18+ with valid HKID",
  "url": ""
}}

GOOD EXAMPLE 2 (Investment Product):
Text: "EleBank Stock Trading - Trade HK and US stocks with zero commission.
HK stocks: $0 commission + HKD 15 platform fee per order.
US stocks: USD 0.0049/share commission + USD 0.005/share platform fee, minimum USD 1.99 per order.
Access real-time market data and trade 24/7 through the EleBank app."

Extract:
{{
  "product_name": "EleBank Stock Trading",
  "category": "investment",
  "subcategory": "stocks",
  "description": "Commission-free stock trading service for HK and US equities with competitive
platform fees. Access global markets through a single integrated platform with real-time quotes.",
  "features": [
    "$0 commission on HK stock trades",
    "HKD 15 platform fee per HK stock order",
    "USD 0.0099/share total cost for US stocks",
    "Minimum USD 1.99 per US stock order",
    "24/7 trading via mobile app",
    "Real-time market data included"
  ],
  "interest_rate": "",
  "fees": "HK stocks: HKD 15/order; US stocks: USD 0.0099/share (min USD 1.99/order)",
  "eligibility": "Existing EleBank account holders",
  "url": ""
}}

GOOD EXAMPLE 3 (Loan Product):
Text: "ZA Bank Personal Loan - Borrow from HKD 5,000 to HKD 500,000 at competitive APR from 8.8%.
Flexible repayment terms from 6 to 60 months. Quick approval in 1 business day.
No collateral or guarantor required for loans up to HKD 200,000."

Extract:
{{
  "product_name": "ZA Bank Personal Loan",
  "category": "loan",
  "subcategory": "personal-loan",
  "description": "Unsecured personal loan with flexible amounts from HKD 5,000 to HKD 500,000
and competitive interest rates. Suitable for various personal financing needs with
quick approval and no collateral requirement.",
  "features": [
    "Loan amounts: HKD 5,000 to HKD 500,000",
    "APR from 8.8% (variable based on credit assessment)",
    "Repayment terms: 6 to 60 months",
    "Quick approval within 1 business day",
    "No collateral required up to HKD 200,000",
    "No guarantor needed"
  ],
  "interest_rate": "From 8.8% APR (variable)",
  "fees": "Processing fee: HKD 500 (deducted from loan amount)",
  "eligibility": "HK residents aged 18-65 with regular income, subject to credit approval",
  "url": ""
}}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CRITICAL INSTRUCTIONS:
- Extract ACTUAL products with REAL details from the website content
- Include specific numbers, rates, fees, and eligibility criteria
- Do NOT invent information - only extract what's explicitly stated
- If a field has no information, use empty string ""
- For investment products, specify exact markets (HK stocks, US stocks, specific crypto pairs)
- For cards, mention card type (Visa/Mastercard), rewards program, annual fees
- For loans, mention APR ranges, loan terms, maximum amounts
- Return ONLY valid JSON array - no markdown, no code fences, no explanation

WEBSITE CONTENT TO ANALYZE:
{text[:40000]}

Return ONLY a valid JSON array of product objects — NO other text, NO markdown fences, NO explanation."""

    try:
        raw = _call([{'role': 'user', 'content': prompt}], label=f'products/{bank_name}')
        if not raw:
            print(f'  [WARN] Product extraction: empty response for {bank_name}')
            return []

        # Try parsing as array first (expected format), then object
        result = _parse_array(raw)
        if not result:
            # Fallback to object parsing for legacy responses
            obj = _parse_object(raw)
            if obj and isinstance(obj, dict):
                result = obj.get('products', [])
            else:
                print(f'  [WARN] Product extraction: JSON parse failed for {bank_name}')
                print(f'  First 200 chars: {raw[:200]}')
                return []

        if not isinstance(result, list):
            print(f'  [WARN] Product extraction: expected list, got {type(result)} for {bank_name}')
            return []

        # Add bank info and validate structure
        products = []
        for p in result:
            if not isinstance(p, dict):
                continue
            products.append({
                'product_name': p.get('product_name', '').strip(),
                'category': p.get('category', '').strip().lower(),
                'subcategory': p.get('subcategory', '').strip(),
                'description': p.get('description', '').strip(),
                'features': p.get('features', []),
                'interest_rate': str(p.get('interest_rate', '')).strip(),
                'fees': str(p.get('fees', '')).strip(),
                'eligibility': str(p.get('eligibility', '')).strip(),
                'url': p.get('url', '').strip(),
            })

        valid_categories = {'deposit', 'card', 'investment', 'loan'}
        products = [p for p in products if p['product_name'] and p['category'] in valid_categories]

        print(f'  [INFO] Product extraction: {len(products)} products found for {bank_name}')
        return products

    except Exception as exc:
        logger.error(f'Product extraction error for {bank_name}: {exc}')
        print(f'  [WARN] Product extraction error for {bank_name}: {exc}')
        return []
