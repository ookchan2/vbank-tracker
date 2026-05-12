# scripts/scraper.py

import asyncio
import hashlib
import os
import re
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_CHARS_PER_SECTION = 10_000
MAX_CHARS_TOTAL       = 50_000
MIN_CONTENT_CHARS     = 200
MAX_RETRIES           = 3
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

# ── Blocked content strings ───────────────────────────────────────────────────
BLOCKED_CONTENT_STRINGS: list[str] = [
    'taipofire.gov.hk',
    'taxdeduction.html',
    'hab033',
    'cefs.gov.hk',
    'wang fuk court',
    '宏福苑',
    'support fund for wang fuk',
    'wangfuk',
    'tai po fire',
    '大埔宏福苑',
    '援助基金',
    'donation acknowledgement',
    'tax deduction for donor',
    'tax deduction arrangement',
    'tax deduction for donation',
    'inland revenue department',
    'ird.gov.hk',
    'relief fund',
    'disaster relief',
    'approved charitable donation',
    'letter of appreciation',
    'government sincerely thanks',
    '政府衷心感謝',
    '捐款致謝',
    '稅務扣除安排',
]

_BLOCKED_CONTENT_RE = re.compile(
    '|'.join(re.escape(s) for s in BLOCKED_CONTENT_STRINGS),
    re.IGNORECASE,
)

# ── Bank domain allow-list ────────────────────────────────────────────────────
# ★ CHANGED: 'airstar' domain updated to elebank.com (bank renamed to EleBank)

_CDN_ALLOW: frozenset[str] = frozenset({
    'cdn.tailwindcss.com', 'fonts.googleapis.com',
    'fonts.gstatic.com',   'cdnjs.cloudflare.com',
    'unpkg.com',           'jsdelivr.net',
})

BANK_DOMAINS: dict[str, list[str]] = {
    'za':      ['bank.za.group', 'za.group'],
    'mox':     ['mox.com'],
    'livi':    ['livibank.com'],
    'welab':   ['welab.bank'],
    'pao':     ['pingandb.com'],
    'airstar': ['elebank.com'],          # ★ EleBank (formerly Airstar Bank)
    'fusion':  ['fusionbank.com'],
    'ant':     ['antbank.hk'],
}

BROKER_DOMAINS: dict[str, list[str]] = {
    'ibkr':        ['interactivebrokers.com.hk', 'interactivebrokers.com'],
    'futu':        ['futuhk.com', 'futuhkapp.com', 'invest.futuhk.com', 'openapi.futunn.com'],
    'tiger':       ['itiger.com'],
    'longbridge':  ['longbridge.com'],
    'welllink':    ['wlsec.com'],
    'webull':      ['webull.com'],
    'brightsmart': ['bsgroup.com.hk'],
    'usmart':      ['usmartglobal.com', 'hk.usmartglobal.com'],
}


def _is_valid_url(url: str, entity_id: str, domains: dict[str, list[str]]) -> bool:
    """Return True only if url's hostname belongs to the entity's allowed domains."""
    allowed = domains.get(entity_id, [])
    if not allowed:
        return True
    try:
        hostname = urlparse(url).hostname or ''
    except Exception:
        return False
    return any(hostname == d or hostname.endswith('.' + d) for d in allowed)


def _is_valid_bank_url(url: str, bank_id: str) -> bool:
    return _is_valid_url(url, bank_id, BANK_DOMAINS)


# ── Bank configs ──────────────────────────────────────────────────────────────
# ★ CHANGED:
#   'airstar' -> name='EleBank', new elebank.com URLs
#   'pao'     -> name='PADB' (formerly PAObank); URLs unchanged (still pingandb.com)

BANK_CONFIGS: dict[str, dict] = {
    'za': {
        'name':       'ZA Bank',
        'color':      '#25CD9C',
        'urls': [
            'https://bank.za.group/en/promotion',
            'https://bank.za.group/en',
            'https://bank.za.group/',
            'https://bank.za.group/hk/usstock',
            'https://bank.za.group/hkstock',
            'https://bank.za.group/hk/fund',
            'https://bank.za.group/hk/loan',
            'https://bank.za.group/hk/statement-instalment',
            'https://bank.za.group/hk/open-account-mgm',
            'https://bank.za.group/6th-anniversary-campaign',
        ],
        'link':       'https://bank.za.group/en/promotion',
        'wait_extra': 4000,
    },
    'mox': {
        'name':       'Mox Bank',
        'color':      '#ec4899',
        'urls': [
            'https://mox.com/promotions/',
            'https://mox.com/zh/promotions/',
            'https://mox.com/',
            'https://mox.com/zh/promotions/moxsmart/',
            'https://mox.com/zh/promotions/The-Club/',
            'https://mox.com/zh/promotions/1500mox/',
            'https://mox.com/promotions/Personal-Accident-Cushion-Promotion-Jan2026/',
            'https://mox.com/promotions/CLUBLINK/',
            'https://mox.com/promotions/moxtrip25/',
            'https://mox.com/promotions/MOXHKT25/',
            'https://mox.com/zh/promotions/best-in-town-telco/',
            'https://mox.com/promotions/mox-zone-at-the-club-hkt/',
            'https://mox.com/zh/promotions/Mox-Referral-Programme/',
        ],
        'link':       'https://mox.com/promotions/',
        'wait_extra': 4000,
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
        'wait_extra': 12000,
        'max_retries': 3,
    },
    'welab': {
        'name':       'WeLab Bank',
        'color':      '#7c3aed',
        'urls': [
            'https://www.welab.bank/en/feature/',
            'https://www.welab.bank/en/',
            'https://www.welab.bank/',
            'https://www.welab.bank/en/feature/2026-wm-april-cash-reward/',
            'https://www.welab.bank/zh/feature/dcp-easter-lucky-draw-2026/',
            'https://www.welab.bank/zh/feature/2-in-1-welcome-rewards-apr26/',
            'https://www.welab.bank/zh/feature/tesla-mega-combo/',
            'https://www.welab.bank/zh/feature/fund/',
            'https://www.welab.bank/en/feature/loan_mgm/',
            'https://www.welab.bank/zh/feature/loan_mgm/',
        ],
        'link':       'https://www.welab.bank/',
        'wait_extra': 4000,
    },
    'pao': {
        'name':       'PADB',            # ★ was 'PAObank'
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
        'name':       'EleBank',         # ★ was 'Airstar Bank'
        'color':      '#06b6d4',
        'urls': [
            'https://www.elebank.com/en-hk/promotion',   # ★ new
            'https://www.elebank.com/zh-hk/promotion',   # ★ new
            'https://www.elebank.com/zh-hk',             # ★ new
        ],
        'link':       'https://www.elebank.com/en-hk/promotion',   # ★ new
        'wait_extra': 4000,
    },
    'fusion': {
        'name':       'Fusion Bank',
        'color':      '#14b8a6',
        'urls': [
            'https://www.fusionbank.com/?lang=en',
            'https://www.fusionbank.com/?lang=zh-HK',
            'https://www.fusionbank.com/common/detail.html?key=fxtd2023&lang=en',
            'https://www.fusionbank.com/common/detail.html?key=fxtd2023&lang=tc',
            'https://www.fusionbank.com/common/detail.html?key=fusionflash&lang=en',
            'https://www.fusionbank.com/common/detail.html?key=fusionflash&lang=tc',
            'https://www.fusionbank.com/common/detail.html?key=savinginterestplus&lang=en',
            'https://www.fusionbank.com/common/detail.html?key=savinginterestplus&lang=tc',
            'https://www.fusionbank.com/common/detail.html?key=mgm_4&lang=en',
            'https://www.fusionbank.com/common/detail.html?key=mgm_4&lang=tc',
        ],
        'link':       'https://www.fusionbank.com/?lang=en',
        'wait_extra': 7000,
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
        'wait_extra': 9000,
        'max_retries': 3,
    },
}

# ── Broker configs ────────────────────────────────────────────────────────────

BROKER_CONFIGS: dict[str, dict] = {
    'ibkr': {
        'name': 'IBKR', 'color': '#e74c3c',
        'urls': [
            'https://www.interactivebrokers.com.hk/en/whyib/overview.php',
            'https://www.interactivebrokers.com/en/pricing/commissions-home.php',
        ],
        'link':       'https://www.interactivebrokers.com.hk/en/whyib/overview.php',
        'wait_extra': 5000,
    },
    'futu': {
        'name': 'Futu', 'color': '#e67e22',
        'urls': [
            'https://www.futuhk.com/',
            'https://www.futuhkapp.com/about-us/promotions',
            'https://invest.futuhk.com/vipofficial',
            'https://invest.futuhk.com/invite-centre',
            'https://www.futuhk.com/commissionnew',
        ],
        'link':       'https://www.futuhk.com/',
        'wait_extra': 5000,
    },
    'tiger': {
        'name': 'Tiger', 'color': '#f39c12',
        'urls': [
            'https://www.itiger.com/hk/en/market/promotion',
            'https://www.itiger.com/hk/en/commissions',
        ],
        'link':       'https://www.itiger.com/hk/en/market/promotion',
        'wait_extra': 5000,
    },
    'longbridge': {
        'name': 'Longbridge', 'color': '#27ae60',
        'urls': [
            'https://longbridge.com/hk/investment-products',
            'https://longbridge.com/hk/zh-HK/rate',
        ],
        'link':       'https://longbridge.com/hk/investment-products',
        'wait_extra': 5000,
    },
    'welllink': {
        'name': '立橋', 'color': '#16a085',
        'urls': [
            'https://wlsec.com/notice.jhtml?tab=adInfo',
            'https://wlsec.com/service.jhtml?tab=commissions',
            'https://wlsec.com/service.jhtml?tab=deposit',
        ],
        'link':       'https://wlsec.com/notice.jhtml?tab=adInfo',
        'wait_extra': 6000,
    },
    'webull': {
        'name': 'webull', 'color': '#2980b9',
        'urls': [
            'https://www.webull.com/offers-promotions',
            'https://www.webull.com/pricing',
        ],
        'link':       'https://www.webull.com/offers-promotions',
        'wait_extra': 5000,
    },
    'brightsmart': {
        'name': '耀才', 'color': '#8e44ad',
        'urls': [
            'https://www.bsgroup.com.hk/',
            'https://www.bsgroup.com.hk/commissions/hongkongsecurities/',
            'https://www.bsgroup.com.hk/commissions/shanghaishenzhena/',
            'https://www.bsgroup.com.hk/commissions/globalsecurities/',
            'https://www.bsgroup.com.hk/commissions/hongkongfutureoption/',
            'https://www.bsgroup.com.hk/commissions/usstockoptions/',
        ],
        'link':       'https://www.bsgroup.com.hk/',
        'wait_extra': 5000,
    },
    'usmart': {
        'name': 'uSmart', 'color': '#1abc9c',
        'urls': [
            'https://hk.usmartglobal.com/zh-hk/promotion-and-activities',
            'https://hk.usmartglobal.com/zh-hk/charges',
        ],
        'link':       'https://hk.usmartglobal.com/zh-hk/promotion-and-activities',
        'wait_extra': 5000,
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


def _scrub_blocked_content(text: str, bank_name: str = '') -> str:
    if not text:
        return text
    fragments = re.split(r'(?<=[.。!?！？])\s+|\n+', text)
    clean:   list[str] = []
    removed: int       = 0
    for frag in fragments:
        if _BLOCKED_CONTENT_RE.search(frag):
            removed += 1
            snippet = frag.strip()[:80]
            print(f'    [SCRUB] Scrubbed blocked content: "{snippet}…"')
        else:
            clean.append(frag)
    if removed:
        label = f' [{bank_name}]' if bank_name else ''
        print(
            f'    [SCRUB] Content scrubber{label}: '
            f'{removed} fragment(s) removed containing blocked URLs/phrases'
        )
    return ' '.join(clean)


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
            print(f'    [DUP]  near-duplicate skipped: {url}')
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

# ── Screenshot cache helpers ──────────────────────────────────────────────────

_SCREENSHOT_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', '.screenshot_cache'
)

_SCREENSHOT_CACHE_TTL_DAYS = 7


def _cache_screenshot(url: str, data: bytes) -> Optional[str]:
    """Save screenshot to disk cache. Returns cache path or None on error."""
    try:
        os.makedirs(_SCREENSHOT_CACHE_DIR, exist_ok=True)
        fname = hashlib.md5(url.encode()).hexdigest() + '.png'
        fpath = os.path.join(_SCREENSHOT_CACHE_DIR, fname)
        with open(fpath, 'wb') as f:
            f.write(data)
        return fpath
    except Exception as exc:
        print(f'    ⚠  Screenshot cache write failed: {exc}')
        return None


def _load_cached_screenshot(url: str) -> Optional[bytes]:
    """Load screenshot from disk cache if it exists and is not older than TTL."""
    try:
        fname = hashlib.md5(url.encode()).hexdigest() + '.png'
        fpath = os.path.join(_SCREENSHOT_CACHE_DIR, fname)
        if os.path.exists(fpath):
            age_days = (time.time() - os.path.getmtime(fpath)) / 86400
            if age_days > _SCREENSHOT_CACHE_TTL_DAYS:
                print(f'    ⚠  Screenshot cache expired ({age_days:.1f}d old) for {url}')
                return None
            with open(fpath, 'rb') as f:
                return f.read()
    except Exception:
        pass
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
                # Try cache first
                screenshot = _load_cached_screenshot(url)
                if screenshot is None:
                    try:
                        screenshot = await page.screenshot(full_page=False, type='png')
                        _cache_screenshot(url, screenshot)
                    except Exception:
                        pass

            return text, screenshot

        except Exception as exc:
            msg = str(exc)[:150]
            if attempt < retries:
                wait_s = 2 ** attempt
                print(
                    f'    ⚠  attempt {attempt}/{retries} failed for {url}: '
                    f'{msg[:80]} — retrying in {wait_s}s…'
                )
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

    valid_urls:   list[str] = []
    skipped_urls: list[str] = []
    for url in cfg['urls']:
        if _is_valid_bank_url(url, bank_id):
            valid_urls.append(url)
        else:
            skipped_urls.append(url)
            print(f'    [BLOCKED] Domain guard SKIPPED non-bank URL for {cfg["name"]}: {url}')

    if skipped_urls:
        print(
            f'    [BLOCKED] {len(skipped_urls)} URL(s) outside {cfg["name"]} domain skipped — '
            f'prevents cross-site content contamination'
        )

    if not valid_urls:
        result.errors.append('All URLs failed domain validation')
        result.elapsed_s = round(time.monotonic() - t_start, 2)
        return result

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
            req_url = route.request.url

            if _BLOCKED_EXTENSIONS.search(req_url):
                await route.abort()
                return

            if _BLOCKED_CONTENT_RE.search(req_url):
                await route.abort()
                return

            try:
                req_hostname = urlparse(req_url).hostname or ''
                allowed      = BANK_DOMAINS.get(bank_id, [])
                if allowed and not any(
                    req_hostname == d or req_hostname.endswith('.' + d)
                    for d in allowed
                ):
                    if not any(cdn in req_hostname for cdn in _CDN_ALLOW):
                        if route.request.resource_type in ('document', 'xhr', 'fetch'):
                            await route.abort()
                            return
            except Exception:
                pass

            await route.continue_()

        await page.route('**/*', _handle_route)

        for url in valid_urls:
            print(f'    -> {url}')
            text, shot = await _try_url(page, url, wait_extra, retries)

            if text and len(text) > MIN_CONTENT_CHARS:
                text = _scrub_blocked_content(text, cfg['name'])
                sections.append((url, text))
                if best_shot is None and shot:
                    best_shot  = shot
                    result.url = url
                print(f'    [OK]  {len(text):,} chars')
            else:
                thin_len = len(text)
                print(f'    🔁 thin ({thin_len} chars) -> requests fallback for {url}')
                fb = scrape_with_requests(url)
                if fb and len(fb) > MIN_CONTENT_CHARS:
                    fb = _scrub_blocked_content(fb, cfg['name'])
                    sections.append((url, fb))
                    print(f'    [OK]  requests: {len(fb):,} chars')
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
            print('    [SCREENSHOT] Still thin -> screenshot fallback…')
            try:
                await page.unroute('**/*')
                await page.goto(cfg['link'], wait_until='domcontentloaded', timeout=45_000)
                await page.wait_for_timeout(wait_extra + 3_000)
                best_shot = await page.screenshot(full_page=True, type='png')
                print('    [OK]  screenshot taken')
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
            header = f'== {cfg["name"]} '
            print(f'\n{header}{"=" * max(1, 50 - len(header))}')

            result = await _scrape_bank(browser, bank_id)

            mark       = '✅' if result.success else '❌'
            total      = len(cfg['urls'])
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


# ── Broker scraper ────────────────────────────────────────────────────────────

async def _scrape_broker(browser: Browser, broker_id: str) -> ScrapeResult:
    cfg        = BROKER_CONFIGS[broker_id]
    wait_extra = cfg.get('wait_extra', 3000)
    retries    = cfg.get('max_retries', MAX_RETRIES)
    t_start    = time.monotonic()

    result = ScrapeResult(
        bank_id   = broker_id,
        bank_name = cfg['name'],
        url       = cfg['link'],
        text      = '',
    )

    valid_urls:   list[str] = []
    skipped_urls: list[str] = []
    for url in cfg['urls']:
        if _is_valid_url(url, broker_id, BROKER_DOMAINS):
            valid_urls.append(url)
        else:
            skipped_urls.append(url)
            print(f'    [BLOCKED] Domain guard SKIPPED non-broker URL for {cfg["name"]}: {url}')

    if skipped_urls:
        print(
            f'    [BLOCKED] {len(skipped_urls)} URL(s) outside {cfg["name"]} domain skipped'
        )

    if not valid_urls:
        result.errors.append('All URLs failed domain validation')
        result.elapsed_s = round(time.monotonic() - t_start, 2)
        return result

    allowed_domains = BROKER_DOMAINS.get(broker_id, [])

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
            req_url = route.request.url

            if _BLOCKED_EXTENSIONS.search(req_url):
                await route.abort()
                return

            try:
                req_hostname = urlparse(req_url).hostname or ''
                if allowed_domains and not any(
                    req_hostname == d or req_hostname.endswith('.' + d)
                    for d in allowed_domains
                ):
                    if not any(cdn in req_hostname for cdn in _CDN_ALLOW):
                        if route.request.resource_type in ('document', 'xhr', 'fetch'):
                            await route.abort()
                            return
            except Exception:
                pass

            await route.continue_()

        await page.route('**/*', _handle_route)

        for url in valid_urls:
            print(f'    -> {url}')
            text, shot = await _try_url(page, url, wait_extra, retries)

            if text and len(text) > MIN_CONTENT_CHARS:
                sections.append((url, text))
                if best_shot is None and shot:
                    best_shot  = shot
                    result.url = url
                print(f'    [OK]  {len(text):,} chars')
            else:
                thin_len = len(text)
                print(f'    🔁 thin ({thin_len} chars) -> requests fallback for {url}')
                fb = scrape_with_requests(url)
                if fb and len(fb) > MIN_CONTENT_CHARS:
                    sections.append((url, fb))
                    print(f'    [OK]  requests: {len(fb):,} chars')
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
            print('    [SCREENSHOT] Still thin -> screenshot fallback…')
            try:
                await page.unroute('**/*')
                await page.goto(cfg['link'], wait_until='domcontentloaded', timeout=45_000)
                await page.wait_for_timeout(wait_extra + 3_000)
                best_shot = await page.screenshot(full_page=True, type='png')
                print('    [OK]  screenshot taken')
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


async def _run_all_brokers() -> dict[str, dict]:
    results: dict[str, dict] = {}
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async def _bounded(browser: Browser, broker_id: str) -> None:
        async with sem:
            cfg    = BROKER_CONFIGS[broker_id]
            header = f'== {cfg["name"]} (broker) '
            print(f'\n{header}{"=" * max(1, 50 - len(header))}')

            result = await _scrape_broker(browser, broker_id)

            mark       = '✅' if result.success else '❌'
            total      = len(cfg['urls'])
            error_note = f' | {len(result.errors)} error(s)' if result.errors else ''
            print(
                f'  {mark}  {cfg["name"]}: '
                f'{len(result.text):,} chars '
                f'from {result.sections_count}/{total} URLs '
                f'in {result.elapsed_s}s'
                f'{error_note}'
            )
            results[broker_id] = result.to_dict()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=BROWSER_ARGS)
        await asyncio.gather(*[
            _bounded(browser, broker_id) for broker_id in BROKER_CONFIGS
        ])
        await browser.close()

    return results


def run_broker_scraper() -> dict:
    """Synchronous entry point for broker scraping called by main.py."""
    return asyncio.run(_run_all_brokers())