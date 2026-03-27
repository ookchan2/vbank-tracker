# scripts/ai_helper.py

import asyncio
import concurrent.futures
import json
import os
import re

# ── 模組級別狀態 ──────────────────────────────────────────────────
_api_key     = None
_bot_name    = "Claude-3-7-Sonnet"
AI_AVAILABLE = False

MODELS_TO_TRY = [
    "Claude-3-7-Sonnet",
    "Claude-3-5-Sonnet",
    "GPT-4o",
    "Perplexity-Pro-Search",
]

# ── 詳細提取 prompt ───────────────────────────────────────────────
# ✅ FIX 1: Use a plain string with manual substitution instead of
#            .format() so that { } in scraped bank text never crash.
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
║  • Name and highlight must be in English                    ║
╚══════════════════════════════════════════════════════════════╝

ALLOWED TYPE TAGS (Chinese, pick 1-3 per promotion):
  迎新 / 消費 / 投資 / 旅遊 / 保險 / 貸款 / 活期存款 /
  定期存款 / 外匯 / 推薦 / 長期獎勵 / 新資金 / Others

REQUIRED OUTPUT: A valid JSON array — NO other text, NO markdown fences.

Schema for each object:
{
  "name":        "Full descriptive English name",
  "types":       ["tag1", "tag2"],
  "period":      "e.g. Until 30 Apr 2026  |  Ongoing",
  "end_date":    "YYYY-MM-DD if stated, else null",
  "highlight":   "One-line key benefit (starts with emoji)",
  "description": "2-3 sentences about this specific promotion.",
  "quota":       "Eligibility or quota information",
  "cost":        "Min spend / cost, or Free",
  "link":        "URL_PLACEHOLDER"
}

WEBSITE TEXT TO ANALYSE:
────────────────────────────────────────────────────────────────
TEXT_PLACEHOLDER
────────────────────────────────────────────────────────────────
Remember: return ONLY the JSON array starting with [ and ending with ]."""


def _build_prompt(bank_name: str, url: str, text: str) -> str:
    """
    ✅ FIX 1: Use plain string replacement instead of .format()
    so { } characters in scraped text never cause KeyError/ValueError.
    """
    return (
        _PROMPT_TMPL
        .replace('BANK_NAME_PLACEHOLDER', bank_name)
        .replace('URL_PLACEHOLDER',       url)
        .replace('TEXT_PLACEHOLDER',      text)
    )


# ── Poe 非同步核心 ────────────────────────────────────────────────

async def _async_call(messages: list, bot_name: str) -> str:
    """非同步調用 Poe bot，返回完整回應文字。"""
    try:
        import fastapi_poe as fp

        poe_messages = [
            fp.ProtocolMessage(role=m['role'], content=m['content'])
            for m in messages
        ]

        response_text = ''
        async for partial in fp.get_bot_response(
            messages = poe_messages,
            bot_name = bot_name,
            api_key  = _api_key,
        ):
            response_text += partial.text

        return response_text.strip()

    except Exception as e:
        print(f'  ⚠️  Poe async call error ({bot_name}): {e}')
        return ''


def _run_async(coro) -> str:
    """
    ✅ FIX 2: Centralised async runner — avoids importing
    concurrent.futures repeatedly in every function.
    Handles both 'inside running loop' and 'no loop' cases.
    """
    try:
        asyncio.get_running_loop()          # raises RuntimeError if no loop
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    except RuntimeError:
        return asyncio.run(coro)


def _call(messages: list) -> str:
    """同步 wrapper，永不 raise。"""
    if not AI_AVAILABLE or _api_key is None:
        return ''
    try:
        return _run_async(_async_call(messages, _bot_name))
    except Exception as e:
        print(f'  ⚠️  Call error: {e}')
        return ''


# ── 初始化 ────────────────────────────────────────────────────────

def init_ai() -> bool:
    """
    依序嘗試 MODELS_TO_TRY，第一個能成功回應的就使用。
    返回 bool。
    """
    global _api_key, _bot_name, AI_AVAILABLE

    try:
        import fastapi_poe  # noqa: F401

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
            else:
                print(f'  ❌ {model} failed, trying next...')

        print('❌ All models failed — AI disabled')
        AI_AVAILABLE = False
        return False

    except ImportError:
        print('❌ fastapi-poe not installed. Run: pip install fastapi-poe')
        AI_AVAILABLE = False
        return False
    except Exception as e:
        print(f'❌ AI init failed: {e}')
        AI_AVAILABLE = False
        return False


# ── 工具函數 ──────────────────────────────────────────────────────

def _parse_array(raw: str) -> list:
    """穩健解析返回的 JSON array。"""
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
    """穩健解析返回的 JSON object。"""
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
    """超長文字：保留前 10K + 後 8K。"""
    if len(text) <= max_chars:
        return text
    return text[:10000] + '\n\n…[middle trimmed for length]…\n\n' + text[-8000:]


def _stamp(promos: list, bank_id: str, bank_name: str, default_url: str) -> list:
    """為每條記錄補充 bank info 和缺失欄位。"""
    for p in promos:
        p['bank']  = bank_id
        p['bName'] = bank_name
        p.setdefault('link',        default_url)
        p.setdefault('types',       ['Others'])
        p.setdefault('period',      'Ongoing')
        p.setdefault('end_date',    None)
        p.setdefault('highlight',   '')
        p.setdefault('description', '')
        p.setdefault('quota',       'Check official website')
        p.setdefault('cost',        'Check official website')
    return promos


# ── 公開函數 ──────────────────────────────────────────────────────

def analyze_promotions(bank_id: str,
                       bank_name: str,
                       text: str = '',
                       screenshot: bytes = None,
                       default_url: str = '') -> list:
    """
    從爬取文字中提取所有促銷活動。
    永遠返回 list（失敗時為空 list），絕不 raise。
    """
    if not AI_AVAILABLE:
        return []

    clean   = _trim_text(text.strip() if text else '')
    results: list = []

    if len(clean) >= 200:
        # ✅ FIX 1: use _build_prompt() — safe against { } in text
        prompt = _build_prompt(
            bank_name = bank_name,
            url       = default_url,
            text      = clean,
        )
        raw    = _call([{'role': 'user', 'content': prompt}])
        parsed = _parse_array(raw)
        if parsed:
            results = parsed
            print(f'  📝 Text → {len(results)} promotions for {bank_name}')
    else:
        print(f'  ⚠️  Text too short ({len(clean)} chars) for {bank_name}')

    # ✅ FIX 4: Single unified vision-skip message
    if screenshot is not None and len(results) < 3:
        print(
            f'  ℹ️  Vision skipped ({bank_name}) — '
            f'{_bot_name} image input not implemented'
        )

    results = _stamp(results, bank_id, bank_name, default_url)
    print(f'  ✅ Total: {len(results)} promotions for {bank_name}')
    return results


def generate_strategic_insights(promotions_by_bank: dict) -> dict | None:
    """
    Generate AI strategic insights comparing each bank to ZA Bank.
    Uses the same Poe _call() as the rest of the file.
    """
    if not AI_AVAILABLE:
        print('⚠️  AI not available — skipping strategic insights')
        return None

    # Build per-bank summaries ----------------------------------------
    bank_summaries = []
    for bank_name, promos in sorted(promotions_by_bank.items()):
        if not promos:
            continue
        lines = []
        for p in promos:
            title     = (p.get('title') or p.get('name') or 'N/A')[:80]
            highlight = (p.get('highlight') or p.get('description') or '')[:120]
            period    = (p.get('period') or p.get('validity') or 'Ongoing')[:60]
            raw_types = p.get('types') or p.get('type') or 'General'
            ptype     = (', '.join(raw_types) if isinstance(raw_types, list)
                         else str(raw_types))[:40]
            lines.append(f'  [{ptype}] {title}: {highlight} | {period}')
        bank_summaries.append(
            f'## {bank_name} ({len(promos)} active)\n' + '\n'.join(lines)
        )

    if not bank_summaries:
        print('⚠️  No promotions data — skipping strategic insights')
        return None

    promotions_text = '\n\n'.join(bank_summaries)

    prompt = f"""You are a Hong Kong virtual bank analyst. \
Analyze these active promotions and return strategic insights as JSON.

{promotions_text}

Return this EXACT JSON structure (populate with real numbers/details from the data above):
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
      "focus": "short keywords mixing EN/中文",
      "strengths": ["strength with numbers", "strength 2", "strength 3"],
      "expiring_alert": "Offer expiring within 30 days, or empty string",
      "vs_za_pros": null,
      "vs_za_cons": null
    }},
    "OtherBank": {{
      "focus": "keywords",
      "strengths": ["strength 1", "strength 2", "strength 3"],
      "expiring_alert": "",
      "vs_za_pros": "clear pros vs ZA Bank",
      "vs_za_cons": "clear cons vs ZA Bank"
    }}
  }}
}}

Rules:
- Include ALL banks from the data above in bank_analysis
- For ZA Bank: vs_za_pros and vs_za_cons MUST be null
- Only include best_for entries you can clearly identify a winner for
- Use specific numbers/amounts from the actual promotions
- Return valid JSON ONLY — no markdown, no code fences, no explanation"""

    raw = _call([{'role': 'user', 'content': prompt}])

    if not raw:
        print('❌ Strategic insights: empty response from AI')
        return None

    result = _parse_object(raw)
    if result is None:
        print('❌ Strategic insights: JSON parse failed')
        return None

    # ✅ FIX 3: Case-insensitive match when injecting ground-truth counts
    name_lookup = {k.lower(): k for k in promotions_by_bank}
    for bname in result.get('bank_analysis', {}):
        matched_key = name_lookup.get(bname.lower())
        result['bank_analysis'][bname]['count'] = (
            len(promotions_by_bank[matched_key]) if matched_key else 0
        )

    print(
        f'✅ Strategic insights generated for '
        f'{len(result.get("bank_analysis", {}))} banks '
        f'via {_bot_name}'
    )
    return result