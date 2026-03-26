# scripts/scraper.py
import asyncio
import re
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ── Bank configs: list of URLs tried in order ──────────────────────────────
BANK_CONFIGS = {
    'za': {
        'name': 'ZA Bank', 'color': '#25CD9C',
        'urls': [
            'https://bank.za.group/en/promotion',
            'https://bank.za.group/en',
            'https://bank.za.group/',
        ],
        'link': 'https://bank.za.group/en',
    },
    'welab': {
        'name': 'WeLab Bank', 'color': '#7c3aed',
        'urls': [
            'https://www.welab.bank/en/feature/',
            'https://www.welab.bank/en/',
            'https://www.welab.bank/',
        ],
        'link': 'https://www.welab.bank/',
    },
    'pao': {
        'name': 'PAObank', 'color': '#0ea5e9',
        'urls': [
            'https://www.pingandb.com/tc/',
            'https://www.pingandb.com/en/',
            'http://www.pingandb.com.hk/',
        ],
        'link': 'https://www.pingandb.com.hk/',
    },
    'livi': {
        'name': 'livi bank', 'color': '#f97316',
        'urls': [
            'https://www.livibank.com/',   # ✅ correct domain: livi.com.hk
            'https://www.livibank.com.hk/',
            'https://livibank.com.hk/',
        ],
        'link': 'https://www.livi.com.hk/',
    },
    'airstar': {
        'name': 'Airstar Bank', 'color': '#06b6d4',
        'urls': [
            'https://www.airstarbank.com/en-hk/promotion',
            'https://www.airstarbank.com/',
        ],
        'link': 'https://www.airstarbank.com/en-hk/promotion',
    },
    'fusion': {
        'name': 'Fusion Bank', 'color': '#14b8a6',
        'urls': [
            'https://www.fusionbank.com/?lang=en',  # ✅ correct domain: .com.hk
            'https://www.fusionbank.com.hk/zh/promotion/',
            'https://www.fusionbank.com/?lang=zh-HK',
        ],
        'link': 'https://www.fusionbank.com.hk/',
    },
    'mox': {
        'name': 'Mox Bank', 'color': '#ec4899',
        'urls': [
            'https://mox.com/promotions/',
            'https://mox.com/',
        ],
        'link': 'https://mox.com/promotions/',
    },
    'ant': {
        'name': 'Ant Bank', 'color': '#1677ff',
        'urls': [
            'https://www.antbank.hk/em-plus-offer?lang=en_us&',
            'https://www.antbank.hk/',
            'https://www.antbank.hk/em-plus-offer?lang=zh_hk',
        ],
        'link': 'https://www.antbank.hk/',
    },
}

BROWSER_ARGS = [
    '--no-sandbox', '--disable-setuid-sandbox',
    '--disable-dev-shm-usage', '--disable-accelerated-2d-canvas',
    '--no-first-run', '--no-zygote', '--disable-gpu',
    '--disable-web-security',
    '--disable-features=IsolateOrigins,site-per-process',
]

USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/122.0.0.0 Safari/537.36'
)

_JS_GET_TEXT = '''() => {
    const SKIP = new Set(['SCRIPT','STYLE','NAV','HEADER','FOOTER',
                          'NOSCRIPT','SVG','IFRAME','HEAD']);
    function walk(node) {
        if (node.nodeType === 3) return node.textContent || '';
        if (SKIP.has(node.tagName)) return '';
        return Array.from(node.childNodes).map(walk).join(' ');
    }
    const root = document.body || document.documentElement;
    return root ? walk(root) : '';
}'''


# ── Fallback: plain HTTP + BeautifulSoup (works when Playwright gets blocked) ──
def scrape_with_requests(url):
    headers = {
        'User-Agent': USER_AGENT,
        'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
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


# ── Try a single URL with Playwright ──────────────────────────────────────────
async def _try_url(page, url):
    """Load URL, wait for JS, extract text. Returns (text, screenshot)."""
    try:
        await page.goto(url, timeout=45000, wait_until='domcontentloaded')

        # Wait for network to settle (ok if it times out)
        try:
            await page.wait_for_load_state('networkidle', timeout=12000)
        except Exception:
            pass

        # Extra time for SPAs to hydrate
        await page.wait_for_timeout(3000)

        # Scroll to trigger lazy-loaded content
        await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        await page.wait_for_timeout(1500)
        await page.evaluate('window.scrollTo(0, 0)')

        # Extract clean text via JS walker
        text = await page.evaluate(_JS_GET_TEXT)
        text = re.sub(r'\s+', ' ', text or '').strip()

        # Take a viewport screenshot if content looks good
        screenshot = None
        if len(text) > 300:
            try:
                screenshot = await page.screenshot(full_page=False, type='png')
            except Exception:
                pass

        return text, screenshot

    except Exception as e:
        print(f'    ⚠ Playwright error on {url}: {str(e)[:100]}')
        return '', None


# ── Scrape one bank: Playwright first, then requests fallback ─────────────────
async def _scrape_bank(browser, bank_id):
    cfg = BANK_CONFIGS[bank_id]
    context = await browser.new_context(
        viewport={'width': 1366, 'height': 900},
        user_agent=USER_AGENT,
        ignore_https_errors=True,
        extra_http_headers={
            'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        },
    )

    best_text = ''
    best_shot = None
    best_url  = cfg['link']

    try:
        page = await context.new_page()

        # Block images/fonts to speed up loading
        await page.route(
            '**/*.{png,jpg,jpeg,gif,webp,ico,woff,woff2,ttf,eot,otf}',
            lambda r: r.abort()
        )

        # ── STEP 1: Try each URL with Playwright ──────────────────────────
        for url in cfg['urls']:
            print(f'    → Playwright: {url}')
            text, shot = await _try_url(page, url)

            if text and len(text.strip()) > len(best_text.strip()):
                best_text = text
                best_shot = shot
                best_url  = url
                print(f'    ✓ {len(text):,} chars')
                if len(text) > 3000:
                    break  # Good enough, stop trying

        # ── STEP 2: requests fallback if Playwright got too little ────────
        if len(best_text.strip()) < 300:
            print(f'    🔁 Playwright insufficient ({len(best_text)} chars) → trying requests...')
            for url in cfg['urls']:
                fallback_text = scrape_with_requests(url)
                if fallback_text and len(fallback_text) > len(best_text):
                    best_text = fallback_text
                    best_url  = url
                    print(f'    ✓ requests: {len(fallback_text):,} chars')
                    if len(fallback_text) > 1000:
                        break

        # ── STEP 3: Screenshot for Vision fallback if still little text ───
        if best_shot is None and len(best_text.strip()) < 300:
            print(f'    📸 Taking screenshot for Vision fallback...')
            try:
                await page.unroute(
                    '**/*.{png,jpg,jpeg,gif,webp,ico,woff,woff2,ttf,eot,otf}'
                )
                await page.goto(best_url, wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_timeout(4000)
                best_shot = await page.screenshot(full_page=True, type='png')
            except Exception:
                pass

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


async def _run_all():
    results = {}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS)
        for bank_id, cfg in BANK_CONFIGS.items():
            print(f'\n══ {cfg["name"]} {"═" * (40 - len(cfg["name"]))}')
            result = await _scrape_bank(browser, bank_id)
            results[bank_id] = result
            mark = '✅' if result['success'] else '❌'
            print(f'  {mark} {cfg["name"]}: {len(result["text"]):,} chars')
        await browser.close()
    return results


def run_scraper():
    """Synchronous entry point called by main.py."""
    return asyncio.run(_run_all())