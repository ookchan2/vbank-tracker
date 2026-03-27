# scripts/ai_helper.py

import asyncio
import json
import os
import re

# ── 模組級別狀態 ──────────────────────────────────────────────────
_api_key     = None
_bot_name    = "Claude-3-7-Sonnet"
AI_AVAILABLE = False

# ✅ 新增：依序嘗試的模型列表（第一個成功就停）
MODELS_TO_TRY = [
    "Claude-3-7-Sonnet",       # 優先：最強，支援長文
    "Claude-3-5-Sonnet",       # Fallback 1
    "GPT-4o",                  # Fallback 2
    "Perplexity-Pro-Search",   # Fallback 3（不支援 Vision，但文字分析 OK）
]

# ── 詳細提取 prompt ───────────────────────────────────────────────
_PROMPT_TMPL = """\
You are a specialist at extracting bank promotion data from website text.

Bank: {bank_name}
Source URL: {url}

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
{{
  "name":        "Full descriptive English name",
  "types":       ["tag1", "tag2"],
  "period":      "e.g. Until 30 Apr 2026  |  Ongoing",
  "end_date":    "YYYY-MM-DD if stated, else null",
  "highlight":   "🎁 One-line key benefit (starts with emoji)",
  "description": "2-3 sentences about this specific promotion.",
  "quota":       "Eligibility or quota information",
  "cost":        "Min spend / cost, or Free",
  "link":        "{url}"
}}

WEBSITE TEXT TO ANALYSE:
────────────────────────────────────────────────────────────────
{text}
────────────────────────────────────────────────────────────────
Remember: return ONLY the JSON array starting with [ and ending with ].\
"""


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
            messages=poe_messages,
            bot_name=bot_name,          # ✅ 改為參數傳入
            api_key=_api_key,
        ):
            response_text += partial.text

        return response_text.strip()

    except Exception as e:
        print(f'  ⚠️  Poe async call error ({bot_name}): {e}')
        return ''


def _call(messages: list) -> str:
    """同步 wrapper，永不 raise。"""
    if not AI_AVAILABLE or _api_key is None:
        return ''
    try:
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, _async_call(messages, _bot_name))
                return future.result()
        except RuntimeError:
            return asyncio.run(_async_call(messages, _bot_name))
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
        import fastapi_poe  # 確認 package 已安裝

        key = os.environ.get('POE_API_KEY', '').strip()
        if not key:
            print('⚠️  POE_API_KEY not set — AI disabled')
            return False

        _api_key = key

        # ✅ 逐一測試模型，找到第一個可用的
        for model in MODELS_TO_TRY:
            print(f'  🔍 Testing model: {model} ...')
            try:
                loop = asyncio.get_running_loop()
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(
                        asyncio.run,
                        _async_call([{'role': 'user', 'content': 'Reply OK only.'}], model)
                    )
                    test = future.result()
            except RuntimeError:
                test = asyncio.run(
                    _async_call([{'role': 'user', 'content': 'Reply OK only.'}], model)
                )

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
        prompt = _PROMPT_TMPL.format(
            bank_name=bank_name,
            url=default_url,
            text=clean,
        )
        raw    = _call([{'role': 'user', 'content': prompt}])
        parsed = _parse_array(raw)
        if parsed:
            results = parsed
            print(f'  📝 Text → {len(results)} promotions for {bank_name}')
    else:
        print(f'  ⚠️  Text too short ({len(clean)} chars) for {bank_name}')

    # ✅ Vision 提示：根據當前模型判斷
    if screenshot is not None and len(results) < 3:
        if _bot_name.startswith("Perplexity"):
            print(
                f'  ℹ️  Vision skipped ({bank_name}) — '
                f'{_bot_name} does not support image input.'
            )
        else:
            print(f'  ℹ️  Vision skipped ({bank_name}) — not implemented for {_bot_name}')

    results = _stamp(results, bank_id, bank_name, default_url)
    print(f'  ✅ Total: {len(results)} promotions for {bank_name}')
    return results