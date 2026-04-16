# scripts/scraper.py

import asyncio
import hashlib
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_CHARS_PER_SECTION = 10_000  # raised from 8k — gives AI more context per page
MAX_CHARS_TOTAL       = 50_000  # raised from 40k — banks with many promos need more
MIN_CONTENT_CHARS     = 200
MAX_RETRIES           = 3       # raised from 2 — extra resilience for slow banks
CONCURRENCY_LIMIT     = 3

USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/122.0.0.0 Safari/537.36'
)

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

_BLOCKED_EXTENSIONS = re.compile(
    r'\.(png|jpg|jpeg|gif|webp|ico|woff2?|ttf|eot|otf|mp4|mp3|pdf|zip)(\?.*)?$',
    re.IGNORECASE,
)

# ── Bank configs ──────────────────────────────────────────────────────────────
#
#  URL STRATEGY (3 tiers per bank):
#  1. Overview / home pages     → general promotions listing
#  2. Product / BAU pages       → feature pages without "promotion" in URL
#  3. MGM / referral pages      → URLs containing "mgm" or referral keyword
#
#  For banks without a dedicated promotions area, product pages are also
#  included (e.g. ZA Bank loan/statement-instalment, Mox individual campaigns).
#
BANK_CONFIGS: dict[str, dict] = {
    'za': {
        'name':       'ZA Bank',
        'color':      '#25CD9C',
        'urls': [
            # Tier 1 — promotions overview
            'https://bank.za.group/en/promotion',
            'https://bank.za.group/en',
            'https://bank.za.group/',
            # Tier 2 — product pages (these contain embedded promos)
            'https://bank.za.group/hk/usstock',
            'https://bank.za.group/hkstock',
            'https://bank.za.group/hk/fund',
            'https://bank.za.group/hk/loan',                   # ← NEW: loan product page
            'https://bank.za.group/hk/statement-instalment',   # ← NEW: statement instalment
            # Tier 3 — campaign / MGM pages
            'https://bank.za.group/hk/open-account-mgm',
            'https://bank.za.group/6th-anniversary-campaign',  # ← NEW: 6th anniversary
        ],
        'link':       'https://bank.za.group/en/promotion',
        'wait_extra': 4000,   # raised slightly
    },
    'mox': {
        'name':       'Mox Bank',
        'color':      '#ec4899',
        'urls': [
            # Tier 1 — promotions hub (lists ALL active promos)
            'https://mox.com/promotions/',
            'https://mox.com/zh/promotions/',
            'https://mox.com/',
            # Tier 2 — specific campaign pages
            # These must be listed individually because Mox's SPA hub may not
            # render individual card details in the AI-visible text.
            'https://mox.com/zh/promotions/moxsmart/',                           # ← was wrongly expired
            'https://mox.com/zh/promotions/The-Club/',                           # ← was wrongly expired
            'https://mox.com/zh/promotions/1500mox/',                            # ← was wrongly expired
            'https://mox.com/promotions/Personal-Accident-Cushion-Promotion-Jan2026/', # ← was wrongly expired
            'https://mox.com/promotions/CLUBLINK/',                              # ← was wrongly expired
            'https://mox.com/promotions/moxtrip25/',                             # ← was wrongly expired
            'https://mox.com/promotions/MOXHKT25/',                              # ← was wrongly expired
            'https://mox.com/zh/promotions/best-in-town-telco/',                 # ← NEW: missing promo
            'https://mox.com/promotions/mox-zone-at-the-club-hkt/',              # ← NEW: missing promo
            # Tier 3 — referral
            'https://mox.com/zh/promotions/Mox-Referral-Programme/',
        ],
        'link':       'https://mox.com/promotions/',
        'wait_extra': 4000,   # raised — Mox SPA needs time to hydrate
        'max_retries': 3,
    },
    'livi': {
        'name':       'livi bank',
        'color':      '#f97316',
        'urls': [
            'https://www.livibank.com/',
            'https://www.livibank.com/zh_HK/',
        ],
        'link':       'https://www.livibank.com/',
        'wait_extra': 12000,  # raised — livi is the slowest loader
        'max_retries': 3,
    },
    'welab': {
        'name':       'WeLab Bank',
        'color':      '#7c3aed',
        'urls': [
            # Tier 1 — feature / promotions hub
            'https://www.welab.bank/en/feature/',
            'https://www.welab.bank/en/',
            'https://www.welab.bank/',
            # Tier 2 — individual campaign pages (must be listed to avoid missing)
            'https://www.welab.bank/en/feature/2026-wm-april-cash-reward/',     # ← NEW: April investment
            'https://www.welab.bank/zh/feature/dcp-easter-lucky-draw-2026/',
            'https://www.welab.bank/zh/feature/2-in-1-welcome-rewards-apr26/',
            'https://www.welab.bank/zh/feature/tesla-mega-combo/',
            'https://www.welab.bank/zh/feature/fund/',
            # Tier 3 — MGM / referral
            'https://www.welab.bank/en/feature/loan_mgm/',
            'https://www.welab.bank/zh/feature/loan_mgm/',
        ],
        'link':       'https://www.welab.bank/',
        'wait_extra': 4000,
    },
    'pao': {
        'name':       'PAObank',
        'color':      '#0ea5e9',
        'urls': [
            'https://www.pingandb.com/en/',
            'https://www.pingandb.com/tc/',
            'https://www.pingandb.com/tc/money-market-fund.html',
            'https://www.pingandb.com/tc/investment.html',
            'https://www.pingandb.com/tc/stock.html',
        ],
        'link':       'https://www.pingandb.com/en/',
        'wait_extra': 6000,
        'max_retries': 3,
    },
    'airstar': {
        'name':       'Airstar Bank',
        'color':      '#06b6d4',
        'urls': [
            'https://www.airstarbank.com/en-hk/promotion',
            'https://www.airstarbank.com/',
        ],
        'link':       'https://www.airstarbank.com/en-hk/promotion',
        'wait_extra': 4000,
    },
    'fusion': {
        'name':       'Fusion Bank',
        'color':      '#14b8a6',
        'urls': [
            # Tier 1 — home (lists promotions)
            'https://www.fusionbank.com/?lang=en',
            'https://www.fusionbank.com/?lang=zh-HK',
            # Tier 2 — individual campaign detail pages
            'https://www.fusionbank.com/common/detail.html?key=fxtd2023&lang=en',
            'https://www.fusionbank.com/common/detail.html?key=fxtd2023&lang=tc',
            'https://www.fusionbank.com/common/detail.html?key=fusionflash&lang=en',
            'https://www.fusionbank.com/common/detail.html?key=fusionflash&lang=tc',
            'https://www.fusionbank.com/common/detail.html?key=savinginterestplus&lang=en',
            'https://www.fusionbank.com/common/detail.html?key=savinginterestplus&lang=tc',
            # Tier 3 — MGM
            'https://www.fusionbank.com/common/detail.html?key=mgm_4&lang=en',  # ← NEW: en version
            'https://www.fusionbank.com/common/detail.html?key=mgm_4&lang=tc',
        ],
        'link':       'https://www.fusionbank.com/?lang=en',
        'wait_extra': 7000,   # raised — Fusion detail pages are slow
        'max_retries': 3,
    },
    'ant': {
        'name':       'Ant Bank',
        'color':      '#1677ff',
        'urls': [
            'https://www.antbank.hk/em-plus-offer?lang=en_us',
            'https://www.antbank.hk/em-plus-offer?lang=zh_hk',
            'https://www.antbank.hk/',
            'https://www.antbank.hk/fund?lang=zh_hk',
            'https://www.antbank.hk/fund?lang=en_us',
        ],
        'link':       'https://www.antbank.hk/em-plus-offer?lang=en_us',
        'wait_extra': 9000,   # raised — Ant Bank is JS-heavy
        'max_retries': 3,
    },
}

# ── JS text extractor ─────────────────────────────────────────────────────────

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

# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ScrapeResult:
    bank_id:        str
    bank_name:      str
    url:            str
    text:           str
    screenshot:     Optional[bytes] = None
    success:        bool            = False
    sections_count: int             = 0
    elapsed_s:      float           = 0.0
    errors:         list[str]       = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            'bank_id':        self.bank_id,
            'bank_name':      self.bank_name,
            'url':            self.url,
            'text':           self.text,
            'screenshot':     self.screenshot,
            'success':        self.success,
            'sections_count': self.sections_count,
            'elapsed_s':      self.elapsed_s,
            'errors':         self.errors,
        }

# ── Text helpers ──────────────────────────────────────────────────────────────

def _clean_text(raw: str) -> str:
    return re.sub(r'\s+', ' ', raw or '').strip()


def _content_hash(text: str) -> str:
    return hashlib.md5(text[:500].encode('utf-8', errors='replace')).hexdigest()


def _deduplicate_sections(
    sections: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    seen:   set[str]              = set()
    unique: list[tuple[str, str]] = []
    for url, text in sections:
        h = _content_hash(text)
        if h not in seen:
            seen.add(h)
            unique.append((url, text))
        else:
            print(f'    ♻  near-duplicate skipped: {url}')
    return unique


def _truncate_sections(
    sections:    list[tuple[str, str]],
    per_section: int = MAX_CHARS_PER_SECTION,
    total_cap:   int = MAX_CHARS_TOTAL,
) -> list[tuple[str, str]]:
    output:  list[tuple[str, str]] = []
    running: int                   = 0
    for url, text in sections:
        if running >= total_cap:
            print(f'    ✂  total cap ({total_cap:,} chars) reached — dropping remaining sections')
            break
        chunk = text[:per_section]
        output.append((url, chunk))
        running += len(chunk)
    return output

# ── Fallback: requests + BeautifulSoup ───────────────────────────────────────

def scrape_with_requests(url: str) -> str | None:
    headers = {
        'User-Agent':      USER_AGENT,
        'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8',
        'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer':         'https://www.google.com/',
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'noscript']):
            tag.decompose()
        text = _clean_text(soup.get_text(separator=' ', strip=True))
        return text if len(text) > MIN_CONTENT_CHARS else None
    except Exception as e:
        print(f'    ❌ requests failed for {url}: {e}')
        return None

# ── Single URL via Playwright ─────────────────────────────────────────────────

async def _try_url(
    page:       Page,
    url:        str,
    wait_extra: int = 3000,
    retries:    int = MAX_RETRIES,
) -> tuple[str, Optional[bytes]]:
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, timeout=60_000, wait_until='domcontentloaded')
            try:
                await page.wait_for_load_state('networkidle', timeout=15_000)
            except Exception:
                pass

            await page.wait_for_timeout(wait_extra)
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(2_000)
            await page.evaluate('window.scrollTo(0, 0)')

            raw  = await page.evaluate(_JS_GET_TEXT)
            text = _clean_text(raw)

            screenshot: Optional[bytes] = None
            if len(text) > MIN_CONTENT_CHARS:
                try:
                    screenshot = await page.screenshot(full_page=False, type='png')
                except Exception:
                    pass

            return text, screenshot

        except Exception as exc:
            msg = str(exc)[:150]
            if attempt < retries:
                wait_s = 2 ** attempt
                print(f'    ⚠  attempt {attempt}/{retries} failed for {url}: {msg[:80]} — retrying in {wait_s}s…')
                await asyncio.sleep(wait_s)
            else:
                print(f'    ⚠  all {retries} attempts exhausted for {url}: {msg}')

    return '', None

# ── Scrape one bank ───────────────────────────────────────────────────────────

async def _scrape_bank(browser: Browser, bank_id: str) -> ScrapeResult:
    cfg        = BANK_CONFIGS[bank_id]
    wait_extra = cfg.get('wait_extra', 3000)
    retries    = cfg.get('max_retries', MAX_RETRIES)
    t_start    = time.monotonic()

    result = ScrapeResult(
        bank_id   = bank_id,
        bank_name = cfg['name'],
        url       = cfg['link'],
        text      = '',
    )

    context: BrowserContext = await browser.new_context(
        viewport            = {'width': 1366, 'height': 900},
        user_agent          = USER_AGENT,
        ignore_https_errors = True,
        extra_http_headers  = {
            'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8',
            'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        },
    )

    sections:  list[tuple[str, str]] = []
    best_shot: Optional[bytes]       = None

    try:
        page = await context.new_page()

        async def _handle_route(route):
            if _BLOCKED_EXTENSIONS.search(route.request.url):
                await route.abort()
            else:
                await route.continue_()

        await page.route('**/*', _handle_route)

        for url in cfg['urls']:
            print(f'    → {url}')
            text, shot = await _try_url(page, url, wait_extra, retries)

            if text and len(text) > MIN_CONTENT_CHARS:
                sections.append((url, text))
                if best_shot is None and shot:
                    best_shot  = shot
                    result.url = url
                print(f'    ✓  {len(text):,} chars')
            else:
                thin_len = len(text)
                print(f'    🔁 thin ({thin_len} chars) → requests fallback for {url}')
                fb = scrape_with_requests(url)
                if fb and len(fb) > MIN_CONTENT_CHARS:
                    sections.append((url, fb))
                    print(f'    ✓  requests: {len(fb):,} chars')
                else:
                    msg = f'Insufficient content from both methods: {url}'
                    result.errors.append(msg)
                    print(f'    ⚠  {msg}')

        sections = _deduplicate_sections(sections)
        sections = _truncate_sections(sections)

        combined = '\n\n'.join(
            f'=== SOURCE: {url} ===\n{text}'
            for url, text in sections
        ).strip()

        if best_shot is None and len(combined) < MIN_CONTENT_CHARS:
            print('    📸 Still thin → screenshot fallback…')
            try:
                await page.unroute('**/*')
                await page.goto(cfg['link'], wait_until='domcontentloaded', timeout=45_000)
                await page.wait_for_timeout(wait_extra + 3_000)
                best_shot = await page.screenshot(full_page=True, type='png')
                print('    ✓  screenshot taken')
            except Exception as exc:
                msg = f'Screenshot fallback failed: {exc}'
                result.errors.append(msg)
                print(f'    ❌ {msg}')

        result.text           = combined
        result.screenshot     = best_shot
        result.success        = len(combined) > MIN_CONTENT_CHARS
        result.sections_count = len(sections)

    finally:
        await context.close()
        result.elapsed_s = round(time.monotonic() - t_start, 2)

    return result

# ── Run all banks concurrently ────────────────────────────────────────────────

async def _run_all() -> dict[str, dict]:
    results: dict[str, dict] = {}
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async def _bounded(browser: Browser, bank_id: str) -> None:
        async with sem:
            cfg    = BANK_CONFIGS[bank_id]
            header = f'══ {cfg["name"]} '
            print(f'\n{header}{"═" * max(1, 50 - len(header))}')

            result = await _scrape_bank(browser, bank_id)

            mark  = '✅' if result.success else '❌'
            total = len(cfg['urls'])
            error_note = f' | {len(result.errors)} error(s)' if result.errors else ''
            print(
                f'  {mark}  {cfg["name"]}: '
                f'{len(result.text):,} chars '
                f'from {result.sections_count}/{total} URLs '
                f'in {result.elapsed_s}s'
                f'{error_note}'
            )
            results[bank_id] = result.to_dict()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS)
        await asyncio.gather(*[
            _bounded(browser, bank_id) for bank_id in BANK_CONFIGS
        ])
        await browser.close()

    return results


def run_scraper() -> dict:
    """Synchronous entry point called by main.py."""
    return asyncio.run(_run_all())