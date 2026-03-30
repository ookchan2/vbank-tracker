# scripts/ai_helper.py

import asyncio
import concurrent.futures
import json
import os
import re

# ── Module-level state ────────────────────────────────────────────────────────
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
    "貸款", "存款", "外匯", "推薦", "長期獎勵", "新資金", "Others"
]

# ── Extraction prompt ─────────────────────────────────────────────────────────
_PROMPT_TMPL = """\
You are a specialist at extracting bank promotion data from website text.

Bank: BANK_NAME_PLACEHOLDER
Source URL: URL_PLACEHOLDER

╔══════════════════════════════════════════════════════════════╗
║  CRITICAL: Extract EVERY SINGLE promotion you can find.     ║
║  • Do NOT merge multiple promotions into one entry          ║
║  • Do NOT skip any promotion, no matter how small           ║
║  • If you see 25 promotions → return 25 objects             ║
║  • Include permanent/ongoing features with promo value      ║
║  • name and highlight must be in English                    ║
╚══════════════════════════════════════════════════════════════╝

ALLOWED CATEGORY TAGS (Chinese, pick 1-3 per promotion):
  迎新 / 消費 / 投資 / 旅遊 / 保險 / 貸款 / 存款 / 外匯 / 推薦 / 長期獎勵 / 新資金 / Others

REQUIRED OUTPUT: A valid JSON array — NO other text, NO markdown fences.

Schema for each object:
{
  "name":        "Full descriptive English name of the promotion",
  "types":       ["category1", "category2"],
  "period":      "e.g. Until 30 Apr 2026 | Ongoing | 2026-01-01 to 2026-06-30",
  "end_date":    "YYYY-MM-DD if an end date is stated, else null",
  "highlight":   "One-line key benefit starting with an emoji",
  "description": "2-3 sentences describing this specific promotion in detail.",
  "quota":       "Eligibility or quota info (e.g. First 1000 customers / No cap / New customers only)",
  "cost":        "Minimum spend or required cost, or Free",
  "tc_link":     "URL_PLACEHOLDER"
}

WEBSITE TEXT TO ANALYSE:
────────────────────────────────────────────────────────────────
TEXT_PLACEHOLDER
────────────────────────────────────────────────────────────────
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
        # 有 running loop → 用新 thread 跑
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result(timeout=120)  # 加 timeout
    except RuntimeError:
        # 沒有 running loop → 直接 run
        return asyncio.run(coro)
    except Exception as e:
        print(f'  ⚠️  _run_async error: {e}')
        return ''


def _call(messages: list) -> str:
    if not AI_AVAILABLE or _api_key is None:
        return ''
    try:
        return _run_async(_async_call(messages, _bot_name))
        print(f'  [DEBUG] AI ({_bot_name}) returned {len(result)} chars')
        if len(result) < 50:
            print(f'  [DEBUG] Full response: {repr(result)}')
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
        p.setdefault('period',      'Ongoing')
        p.setdefault('end_date',    None)
        p.setdefault('highlight',   '')
        p.setdefault('description', '')
        p.setdefault('quota',       'Check official website')
        p.setdefault('cost',        'Check official website')
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

        for attempt in range(2):  # ← retry loop 在 prompt 定義之後
            raw    = _call([{'role': 'user', 'content': prompt}])
            parsed = _parse_array(raw)
            if parsed:
                results = parsed
                print(f'  📝 Text → {len(results)} promotions for {bank_name}')
                break
            if attempt == 0:
                print(f'  🔄 Retry AI for {bank_name}...')
        else:
            # for loop 完整跑完都沒 break → 兩次都失敗
            print(f'  ❌ Both attempts failed for {bank_name}')
    else:
        print(f'  ⚠️  Text too short ({len(clean)} chars) for {bank_name}')

    results = _stamp(results, bank_id, bank_name, default_url)
    print(f'  ✅ Total: {len(results)} promotions for {bank_name}')
    return results


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