# scripts/scraper.py

import asyncio
import re
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ── Bank configs ──────────────────────────────────────────────────────────────
#
#  URL STRATEGY (3 tiers per bank):
#  1. Overview/home pages      → general promotions listing
#  2. Product/BAU pages        → feature pages without "promotion" in URL
#                                 (fund, stock, crypto, etc.)
#  3. MGM / referral pages     → URLs containing "mgm" or referral keyword
#
BANK_CONFIGS = {
    'za': {
        'name':       'ZA Bank',
        'color':      '#25CD9C',
        'urls': [
            # ── Tier 1: Overview / promotion listing ──
            'https://bank.za.group/en/promotion',
            'https://bank.za.group/en',
            'https://bank.za.group/',
            # ── Tier 2: Product / BAU pages (no "promotion" in URL) ──
            'https://bank.za.group/hk/usstock',        # US stocks + StockBack promo
            'https://bank.za.group/hkstock',           # HK stocks 0 commission
            'https://bank.za.group/hk/fund',           # Fund 0% subscription fee
            # ── Tier 3: Referral / MGM ──
            'https://bank.za.group/hk/open-account-mgm',  # Refer-a-friend up to HKD 110,000
        ],
        'link':       'https://bank.za.group/en/promotion',
        'wait_extra': 3000,
    },
    'mox': {
        'name':       'Mox Bank',
        'color':      '#ec4899',
        'urls': [
            # ── Tier 1: Overview ──
            'https://mox.com/promotions/',
            'https://mox.com/',
            # ── Tier 3: Referral / MGM ──
            'https://mox.com/zh/promotions/Mox-Referral-Programme/',  # 多友多賞 HKD 300/referral
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
            # ── Tier 1: Overview ──
            'https://www.welab.bank/en/feature/',
            'https://www.welab.bank/en/',
            'https://www.welab.bank/',
            # ── Tier 2: Product / feature pages (under /feature/ NOT /promotion/) ──
            'https://www.welab.bank/zh/feature/dcp-easter-lucky-draw-2026/',    # Travel lucky draw
            'https://www.welab.bank/zh/feature/2-in-1-welcome-rewards-apr26/', # Welcome bonus HKD 5,000
            'https://www.welab.bank/zh/feature/tesla-mega-combo/',             # Tesla loan 4.55% APR
            'https://www.welab.bank/zh/feature/fund/',                         # Fund 0% fee (BAU)
            # ── Tier 3: Referral / MGM ──
            'https://www.welab.bank/zh/feature/loan_mgm/',                     # Loan referral HKD 800
        ],
        'link':       'https://www.welab.bank/',
        'wait_extra': 3000,
    },
    'pao': {
        'name':       'PAObank',
        'color':      '#0ea5e9',
        'urls': [
            # ── Tier 1: Overview ──
            'https://www.pingandb.com/en/',
            'https://www.pingandb.com/tc/',
            # ── Tier 2: Product / BAU pages ──
            'https://www.pingandb.com/tc/money-market-fund.html',
            'https://www.pingandb.com/tc/investment.html',
            'https://www.pingandb.com/tc/stock.html',
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
            # ── Tier 1: Overview ──
            'https://www.fusionbank.com/?lang=en',
            'https://www.fusionbank.com/?lang=zh-HK',
            # ── Tier 2: Key-based promotion detail pages (no "promotion" in path) ──
            'https://www.fusionbank.com/common/detail.html?key=fxtd2023&lang=tc',
            'https://www.fusionbank.com/common/detail.html?key=fusionflash&lang=tc',
            'https://www.fusionbank.com/common/detail.html?key=savinginterestplus&lang=tc',
            # ── Tier 3: Referral / MGM ──
            'https://www.fusionbank.com/common/detail.html?key=mgm_4&lang=tc',
        ],
        'link':       'https://www.fusionbank.com/?lang=en',
        'wait_extra': 6000,
    },
    'ant': {
        'name':       'Ant Bank',
        'color':      '#1677ff',
        'urls': [
            # ── Tier 1: Overview ──
            'https://www.antbank.hk/em-plus-offer?lang=en_us',
            'https://www.antbank.hk/em-plus-offer?lang=zh_hk',
            'https://www.antbank.hk/',
            # ── Tier 2: Product / BAU pages ──
            'https://www.antbank.hk/fund?lang=zh_hk',  # Fund investment page
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
#
#  KEY CHANGE vs old scraper:
#  OLD → pick the single "best" (longest) URL, break early at 3,000 chars
#  NEW → scrape ALL URLs, combine with === SOURCE === headers so the AI
#        sees every product/MGM/BAU page, not just the overview page
#
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

    sections   = []   # [(url, text), ...] — one entry per successfully scraped URL
    best_shot  = None
    best_url   = cfg['link']

    try:
        page = await context.new_page()

        # Block heavy assets to speed up loading
        await page.route(
            '**/*.{png,jpg,jpeg,gif,webp,ico,woff,woff2,ttf,eot,otf}',
            lambda r: r.abort(),
        )

        # ── Playwright pass: visit EVERY URL, collect ALL sections ─
        for url in cfg['urls']:
            print(f'    → Playwright: {url}')
            text, shot = await _try_url(page, url, wait_extra)

            if text and len(text.strip()) > 200:
                # ✅ Good content — add as a named section
                sections.append((url, text.strip()))
                if best_shot is None and shot:
                    best_shot = shot
                    best_url  = url
                print(f'    ✓ {len(text):,} chars')
            else:
                # ⚠ Thin result — try requests fallback for this URL
                print(f'    🔁 thin ({len(text)} chars) → requests fallback for {url}')
                fb = scrape_with_requests(url)
                if fb and len(fb.strip()) > 200:
                    sections.append((url, fb.strip()))
                    print(f'    ✓ requests: {len(fb):,} chars')
                else:
                    print(f'    ⚠ skipping {url} — insufficient content from both methods')

        # ── Combine every section with a clear URL separator ───────
        #    The AI can now see which bank page each piece came from
        combined_text = '\n\n'.join(
            f'=== SOURCE: {url} ===\n{text}'
            for url, text in sections
        ).strip()

        # ── Screenshot-only fallback if combined text is still empty
        if best_shot is None and len(combined_text) < 300:
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
        'bank_id':        bank_id,
        'bank_name':      cfg['name'],
        'url':            best_url,
        'text':           combined_text,
        'screenshot':     best_shot,
        'success':        len(combined_text.strip()) > 200,
        'sections_count': len(sections),   # how many URLs contributed content
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
            mark    = '✅' if result['success'] else '❌'
            n_urls  = result.get('sections_count', 0)
            total   = len(cfg['urls'])
            print(
                f'  {mark}  {cfg["name"]}: '
                f'{len(result["text"]):,} chars '
                f'from {n_urls}/{total} URLs'
            )
        await browser.close()
    return results


def run_scraper() -> dict:
    """Synchronous entry point called by main.py."""
    return asyncio.run(_run_all())