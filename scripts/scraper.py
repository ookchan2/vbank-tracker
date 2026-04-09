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
#
#  Tune these without touching any logic below.
#
MAX_CHARS_PER_SECTION = 8_000   # cap per URL — prevents one noisy page from
                                 # dominating the AI context window
MAX_CHARS_TOTAL       = 40_000  # cap total text per bank sent to AI
MIN_CONTENT_CHARS     = 200     # below this a page is considered "thin"
MAX_RETRIES           = 2       # Playwright retries per URL before requests fallback
CONCURRENCY_LIMIT     = 3       # banks scraped in parallel (keep ≤ 4 on CI)

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

# Single compiled regex — faster than a glob pattern per request
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
#  Per-bank overrides supported in config:
#    wait_extra  (int, ms)   — extra wait after page load
#    max_retries (int)       — Playwright retries before requests fallback
#
BANK_CONFIGS: dict[str, dict] = {
    'za': {
        'name':       'ZA Bank',
        'color':      '#25CD9C',
        'urls': [
            # Tier 1
            'https://bank.za.group/en/promotion',
            'https://bank.za.group/en',
            'https://bank.za.group/',
            # Tier 2
            'https://bank.za.group/hk/usstock',
            'https://bank.za.group/hkstock',
            'https://bank.za.group/hk/fund',
            # Tier 3
            'https://bank.za.group/hk/open-account-mgm',
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
            'https://mox.com/zh/promotions/Mox-Referral-Programme/',
        ],
        'link':       'https://mox.com/promotions/',
        'wait_extra': 3000,
    },
    'livi': {
        'name':       'livi bank',
        'color':      '#f97316',
        'urls': [
            'https://www.livibank.com/',
            'https://www.livibank.com/zh_HK/',
        ],
        'link':       'https://www.livibank.com/',
        'wait_extra': 10000,  # livi loads very slowly — increased from 6 s
        'max_retries': 3,     # extra retries specific to livi
    },
    'welab': {
        'name':       'WeLab Bank',
        'color':      '#7c3aed',
        'urls': [
            'https://www.welab.bank/en/feature/',
            'https://www.welab.bank/en/',
            'https://www.welab.bank/',
            'https://www.welab.bank/zh/feature/dcp-easter-lucky-draw-2026/',
            'https://www.welab.bank/zh/feature/2-in-1-welcome-rewards-apr26/',
            'https://www.welab.bank/zh/feature/tesla-mega-combo/',
            'https://www.welab.bank/zh/feature/fund/',
            'https://www.welab.bank/zh/feature/loan_mgm/',
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
            'https://www.fusionbank.com/?lang=en',
            'https://www.fusionbank.com/?lang=zh-HK',
            'https://www.fusionbank.com/common/detail.html?key=fxtd2023&lang=tc',
            'https://www.fusionbank.com/common/detail.html?key=fusionflash&lang=tc',
            'https://www.fusionbank.com/common/detail.html?key=savinginterestplus&lang=tc',
            'https://www.fusionbank.com/common/detail.html?key=mgm_4&lang=tc',
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
            'https://www.antbank.hk/fund?lang=zh_hk',
        ],
        'link':       'https://www.antbank.hk/em-plus-offer?lang=en_us',
        'wait_extra': 8000,
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
    """
    Structured result for one bank scrape run.
    Using a dataclass instead of a plain dict gives us:
      - IDE auto-complete and type checking
      - a single source of truth for the shape of scrape output
      - easy serialisation via .to_dict() for callers that expect dicts
    """
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
    """Collapse all whitespace sequences to a single space and strip ends."""
    return re.sub(r'\s+', ' ', raw or '').strip()


def _content_hash(text: str) -> str:
    """
    MD5 of the first 500 chars.
    Used for near-duplicate detection: two pages that open with the same
    500 characters are almost certainly serving identical content (e.g. the
    EN and ZH variants of a page that renders identical text nodes).
    """
    return hashlib.md5(text[:500].encode('utf-8', errors='replace')).hexdigest()


def _deduplicate_sections(
    sections: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    Drop any section whose opening content has already been seen.
    This is a cheap pre-filter that reduces AI token spend and avoids
    inflating the 'within-batch duplicate' count downstream.
    """
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
    """
    Two-level truncation:
      1. Each section is capped at `per_section` chars so a single verbose
         page can't crowd out the others.
      2. The running total is capped at `total_cap` so the AI prompt stays
         within a predictable token budget regardless of bank size.
    """
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
    """
    Lightweight static fallback used when Playwright returns thin content.
    Adding a Referer header mimics an organic Google referral and reduces
    the chance of a 403 from basic bot-detection middleware.
    """
    headers = {
        'User-Agent':      USER_AGENT,
        'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8',
        'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer':         'https://www.google.com/',
    }
    try:
        resp = requests.get(url, headers=headers, timeout=25)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'noscript']):
            tag.decompose()
        text = _clean_text(soup.get_text(separator=' ', strip=True))
        return text if len(text) > MIN_CONTENT_CHARS else None
    except Exception as e:
        print(f'    ❌ requests failed for {url}: {e}')
        return None


# ── Single URL via Playwright (with exponential-backoff retries) ─────────────

async def _try_url(
    page:       Page,
    url:        str,
    wait_extra: int = 3000,
    retries:    int = MAX_RETRIES,
) -> tuple[str, Optional[bytes]]:
    """
    Load `url` up to `retries` times with exponential back-off between
    attempts (2 s, 4 s, …).  Returns (cleaned_text, screenshot_or_None).
    """
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, timeout=60_000, wait_until='domcontentloaded')
            try:
                await page.wait_for_load_state('networkidle', timeout=15_000)
            except Exception:
                pass  # networkidle timeout is acceptable — page may still have content

            await page.wait_for_timeout(wait_extra)
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(1_500)
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

        # ── Block binary/media assets to reduce network overhead ────
        async def _handle_route(route):
            if _BLOCKED_EXTENSIONS.search(route.request.url):
                await route.abort()
            else:
                await route.continue_()

        await page.route('**/*', _handle_route)

        # ── Visit every configured URL, collect all sections ────────
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

        # ── Post-process: dedup → truncate → combine ─────────────────
        sections = _deduplicate_sections(sections)
        sections = _truncate_sections(sections)

        combined = '\n\n'.join(
            f'=== SOURCE: {url} ===\n{text}'
            for url, text in sections
        ).strip()

        # ── Screenshot-only fallback if everything is still thin ─────
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
    """
    Scrape all banks with bounded concurrency (CONCURRENCY_LIMIT).
    Using a semaphore instead of plain asyncio.gather prevents hammering
    the CI runner with too many Chromium contexts at once.
    """
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