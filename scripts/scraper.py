# scripts/scraper.py

import asyncio
import re
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ── Bank configs ──────────────────────────────────────────────────────────────
BANK_CONFIGS = {
    'za': {
        'name':       'ZA Bank',
        'color':      '#25CD9C',
        'urls': [
            'https://bank.za.group/en/promotion',
            'https://bank.za.group/en',
            'https://bank.za.group/',
        ],
        'link':       'https://bank.za.group/en/promotion',
        'wait_extra': 3000,
    },
    'mox': {
        'name':       'Mox Bank',
        'color':      '#ec4899',
        'urls': [
            'https://mox.com/promotions/',
            'https://mox.com/',
        ],
        'link':       'https://mox.com/promotions/',
        'wait_extra': 3000,
    },
    'livi': {
        'name':       'livi bank',
        'color':      '#f97316',
        'urls': [
            'https://www.livibank.com.hk/',
            'https://www.livibank.com/',
        ],
        'link':       'https://www.livibank.com.hk/',
        'wait_extra': 4000,
    },
    'welab': {
        'name':       'WeLab Bank',
        'color':      '#7c3aed',
        'urls': [
            'https://www.welab.bank/en/feature/',
            'https://www.welab.bank/en/',
            'https://www.welab.bank/',
        ],
        'link':       'https://www.welab.bank/',
        'wait_extra': 3000,
    },
    'pao': {
        'name':       'PAObank',
        'color':      '#0ea5e9',
        'urls': [
            'https://www.pingandb.com/en/',
            'https://www.pingandb.com/tc/',
        ],
        'link':       'https://www.pingandb.com/en/',
        'wait_extra': 5000,
    },
    'airstar': {
        'name':       'Airstar Bank',
        'color':      '#06b6d4',
        'urls': [
            'https://www.airstarbank.com/en-hk/promotion',
            'https://www.airstarbank.com/',
        ],
        'link':       'https://www.airstarbank.com/en-hk/promotion',
        'wait_extra': 3000,
    },
    'fusion': {
        'name':       'Fusion Bank',
        'color':      '#14b8a6',
        'urls': [
            'https://www.fusionbank.com/?lang=en',
            'https://www.fusionbank.com/?lang=zh-HK',
        ],
        'link':       'https://www.fusionbank.com/?lang=en',
        'wait_extra': 6000,
    },
    'ant': {
        'name':       'Ant Bank',
        'color':      '#1677ff',
        'urls': [
            'https://www.antbank.hk/em-plus-offer?lang=en_us',
            'https://www.antbank.hk/em-plus-offer?lang=zh_hk',
            'https://www.antbank.hk/',
        ],
        'link':       'https://www.antbank.hk/em-plus-offer?lang=en_us',
        'wait_extra': 8000,
    },
}

BROWSER_ARGS = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-accelerated-2d-canvas',
    '--no-first-run',
    '--no-zygote',
    '--disable-gpu',
    '--disable-web-security',
    '--disable-features=IsolateOrigins,site-per-process',
]

USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/122.0.0.0 Safari/537.36'
)

_JS_GET_TEXT = '''() => {
    const SKIP = new Set([
        'SCRIPT','STYLE','NAV','HEADER','FOOTER',
        'NOSCRIPT','SVG','IFRAME','HEAD'
    ]);
    function walk(node) {
        if (node.nodeType === 3) return node.textContent || '';
        if (SKIP.has(node.tagName)) return '';
        return Array.from(node.childNodes).map(walk).join(' ');
    }
    const root = document.body || document.documentElement;
    return root ? walk(root) : '';
}'''


# ── Fallback: requests + BeautifulSoup ───────────────────────────────────────

def scrape_with_requests(url: str) -> str | None:
    headers = {
        'User-Agent':      USER_AGENT,
        'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8',
        'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'noscript']):
            tag.decompose()
        text = soup.get_text(separator='\n', strip=True)
        return text if len(text) > 300 else None
    except Exception as e:
        print(f'    ❌ requests failed: {e}')
        return None


# ── Single URL via Playwright ─────────────────────────────────────────────────

async def _try_url(page, url: str, wait_extra: int = 3000):
    try:
        await page.goto(url, timeout=60000, wait_until='domcontentloaded')
        try:
            await page.wait_for_load_state('networkidle', timeout=15000)
        except Exception:
            pass
        await page.wait_for_timeout(wait_extra)
        await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        await page.wait_for_timeout(1500)
        await page.evaluate('window.scrollTo(0, 0)')

        text = await page.evaluate(_JS_GET_TEXT)
        text = re.sub(r'\s+', ' ', text or '').strip()

        screenshot = None
        if len(text) > 300:
            try:
                screenshot = await page.screenshot(full_page=False, type='png')
            except Exception:
                pass
        return text, screenshot

    except Exception as e:
        print(f'    ⚠ Playwright error on {url}: {str(e)[:120]}')
        return '', None


# ── Scrape one bank ───────────────────────────────────────────────────────────

async def _scrape_bank(browser, bank_id: str) -> dict:
    cfg        = BANK_CONFIGS[bank_id]
    wait_extra = cfg.get('wait_extra', 3000)

    context = await browser.new_context(
        viewport={'width': 1366, 'height': 900},
        user_agent=USER_AGENT,
        ignore_https_errors=True,
        extra_http_headers={
            'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8',
            'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        },
    )

    best_text = ''
    best_shot = None
    best_url  = cfg['link']

    try:
        page = await context.new_page()

        # Block heavy assets to speed up loading
        await page.route(
            '**/*.{png,jpg,jpeg,gif,webp,ico,woff,woff2,ttf,eot,otf}',
            lambda r: r.abort(),
        )

        # ── Playwright pass ────────────────────────────────────────
        for url in cfg['urls']:
            print(f'    → Playwright: {url}')
            text, shot = await _try_url(page, url, wait_extra)
            if text and len(text.strip()) > len(best_text.strip()):
                best_text = text
                best_shot = shot
                best_url  = url
                print(f'    ✓ {len(text):,} chars')
                if len(text) > 3000:
                    break

        # ── requests fallback ──────────────────────────────────────
        if len(best_text.strip()) < 300:
            print(f'    🔁 Playwright thin ({len(best_text)} chars) → requests fallback...')
            for url in cfg['urls']:
                fb = scrape_with_requests(url)
                if fb and len(fb) > len(best_text):
                    best_text = fb
                    best_url  = url
                    print(f'    ✓ requests: {len(fb):,} chars')
                    if len(fb) > 1000:
                        break

        # ── Screenshot-only fallback ───────────────────────────────
        if best_shot is None and len(best_text.strip()) < 300:
            print('    📸 Still thin → screenshot for Vision fallback...')
            try:
                await page.unroute(
                    '**/*.{png,jpg,jpeg,gif,webp,ico,woff,woff2,ttf,eot,otf}'
                )
                await page.goto(best_url, wait_until='domcontentloaded', timeout=45000)
                await page.wait_for_timeout(wait_extra + 3000)
                best_shot = await page.screenshot(full_page=True, type='png')
                print('    ✓ screenshot taken')
            except Exception as e:
                print(f'    ❌ screenshot failed: {e}')

    finally:
        await context.close()

    return {
        'bank_id':    bank_id,
        'bank_name':  cfg['name'],
        'url':        best_url,
        'text':       best_text,
        'screenshot': best_shot,
        'success':    len(best_text.strip()) > 200,
    }


# ── Run all banks ─────────────────────────────────────────────────────────────

async def _run_all() -> dict:
    results = {}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS)
        for bank_id, cfg in BANK_CONFIGS.items():
            header = f'══ {cfg["name"]} '
            print(f'\n{header}{"═" * max(1, 50 - len(header))}')
            result           = await _scrape_bank(browser, bank_id)
            results[bank_id] = result
            mark = '✅' if result['success'] else '❌'
            print(f'  {mark}  {cfg["name"]}: {len(result["text"]):,} chars')
        await browser.close()
    return results


def run_scraper() -> dict:
    """Synchronous entry point called by main.py."""
    return asyncio.run(_run_all())