/**
 * Scraper — two-phase approach:
 *   Phase 1: crawl each URL directly with playwright-cli (TypeScript-controlled),
 *             printing per-URL progress in real time.
 *   Phase 2: send all collected page text to an Agent for AI extraction.
 *
 * This mirrors the old Python scraper.py + ai_helper.py split, giving us
 * per-URL progress visibility while still leveraging AI for extraction.
 * Enhanced with Copy3's content scrubbing and domain validation.
 * @author Alfie
 */

import { execSync } from 'child_process';
import { runAgent, type AgentResult } from './client';
import { BANK_CONFIGS, type BankConfig } from '../config/banks';
import { hktToday } from '../utils/hkt';
import { isNonBankContent } from '../utils/filters';
import { scrubBlockedContent, isValidBankUrl, cacheScreenshot } from '../utils/validation';

// ── Constants ───────────────────────────────────────────────────────────────

const MIN_CONTENT_CHARS  = 200;
const MAX_CHARS_PER_PAGE = 15_000;
const MAX_CHARS_TOTAL    = 60_000;

/** Max playwright-cli processes per bank (URL-level concurrency) */
const URL_CONCURRENCY    = 3;
/** Max banks scraped simultaneously (bank-level concurrency) */
const BANK_CONCURRENCY   = 4;

// ── Concurrency limiter ──────────────────────────────────────────────────────

async function pLimit<T>(
  tasks: (() => Promise<T>)[],
  concurrency: number,
): Promise<PromiseSettledResult<T>[]> {
  const results: PromiseSettledResult<T>[] = new Array(tasks.length);
  let next = 0;

  async function worker() {
    while (next < tasks.length) {
      const i = next++;
      try {
        results[i] = { status: 'fulfilled', value: await tasks[i]() };
      } catch (reason) {
        results[i] = { status: 'rejected', reason };
      }
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, tasks.length) }, worker);
  await Promise.all(workers);
  return results;
}

// ── Types ───────────────────────────────────────────────────────────────────

export interface RawPromotion {
  name: string;
  title?: string;
  types: string[];
  is_bau: boolean;
  start_date: string | null;
  end_date: string | null;
  period: string;
  highlight: string;
  description: string;
  quota: string;
  cost: string;
  tc_link: string;
  analysis_points: string[];
  // stamped fields
  bank?: string;
  bName?: string;
  link?: string;
}

export interface ScrapedBank {
  bankId: string;
  bankName: string;
  promotions: RawPromotion[];
  costUsd?: number;
  durationMs?: number;
  /** Cached page text for reuse by product extraction */
  pages?: PageText[];
}

/** Raw text collected from a single page */
export interface PageText {
  url: string;
  text: string;
  chars: number;
}

// ── BAU overrides ────────────────────────────────────────────────────────────

const BAU_OVERRIDES: Record<string, string[]> = {
  za: ['new crypto customer fee waiver'],
};

const BAU_GLOBAL_OVERRIDES: string[] = [
  'account opening in 3 minutes',
  'account opening in 5 minutes',
  'quick account opening',
  'mobile account opening',
  'open account in minutes',
  'open an account in minutes',
  'sign up in the time it takes',
  'open account in the time',
  '24/7 mobile banking',
  '24/7 digital banking',
  '24×7 banking',
  '24x7 customer service',
  '24/7 customer service',
  '24×7 customer service',
  '24/7 banking',
  '24/7 service',
];

// ── Allowed categories ───────────────────────────────────────────────────────

const ALLOWED_CATEGORIES = [
  '迎新', '消費', '投資', '旅遊', '保險',
  '貸款', '存款', '外匯', '推薦', '新資金', 'Others',
];

// ── AI extraction prompt ─────────────────────────────────────────────────────

const EXTRACTION_SYSTEM_PROMPT = `You are a specialist at extracting bank promotion data from website text.

CRITICAL RULES:
1. Extract EVERY SINGLE promotion you can find from each page.
2. Do NOT merge multiple promotions into one entry.
3. name and highlight must be in English.
4. For start_date / end_date: look for any date mentioned near the promotion. Always use YYYY-MM-DD format. Use null only if truly absent.
5. is_bau: set true ONLY for permanent product features with NO end date and NO special eligibility condition.
   ✅ BAU: "Free Instant FPS Transfers", "Multi-Currency Savings Account", "Account Opening in 3 Minutes"
   ❌ NOT BAU: "New Customer Bonus", "Limited-Time Fee Waiver", anything with a promo code
6. CATEGORY TAGGING: Any referral/invite → 推薦, fund/基金/stock/crypto → 投資, travel/flight → 旅遊
7. FOOTNOTES are real promotions — scan every footnote for fee waivers, discounts, rewards.
8. DO NOT extract: navigation items, section headings without concrete benefit, pure disclaimers, footer links.
9. ONLY extract promotions OFFERED BY the specified bank. Skip government/charity content (gov.hk, tax, donation).
10. If start_date < today, the promotion is NOT new today — but still extract it.
11. If end_date > today, the promotion is STILL ACTIVE — do NOT treat as expired.

ALLOWED CATEGORY TAGS (pick 1-3 per promotion):
  迎新 / 消費 / 投資 / 旅遊 / 保險 / 貸款 / 存款 / 外匯 / 推薦 / 新資金 / Others

Return a JSON array of promotion objects with these fields:
- name: Full descriptive English name
- types: Array of category tags
- is_bau: boolean
- start_date: "YYYY-MM-DD" or null
- end_date: "YYYY-MM-DD" or null
- period: Human-readable period string
- highlight: One-line key benefit starting with an emoji
- description: 2-3 sentences describing the promotion
- quota: Eligibility or quota info
- cost: Minimum spend or required cost, or Free
- tc_link: Source URL
- analysis_points: Array of 3-5 concise analysis bullet points covering:
    * Value assessment (is the reward/fee waiver worth it?)
    * Eligibility requirements (who qualifies?)
    * Competitive positioning (how does it compare to similar offers from other HK virtual banks?)
    * Time sensitivity (when should users act?)
    * Hidden conditions (minimum spend, quota limits, etc.)`;

const ELEBANK_FEE_CONTEXT = `
[EleBank Stock Trading Fee Reference — factual, use for exact descriptions]
Non-fractional stock trading standard fee schedule (BAU — always available):
  HK-listed stocks: $0 commission per order + HKD 15 platform fee per order
  US-listed stocks: Commission USD 0.0049 per share (min USD 0.99 per order)
                   + Platform fee USD 0.005 per share (min USD 1.00 per order)
                   = Total USD 0.0099 per share, minimum USD 1.99 per order
These are the standard BAU rates — NOT a limited-time promotional discount.
Extract any TIME-LIMITED fee waivers or discounts on top of these rates as
separate non-BAU promotions with the relevant end_date.`;

// ── Output schema ────────────────────────────────────────────────────────────

const PROMOTION_LIST_SCHEMA = {
  type: 'object',
  properties: {
    promotions: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          name:        { type: 'string' },
          types:       { type: 'array', items: { type: 'string' } },
          is_bau:      { type: 'boolean' },
          start_date:  { type: ['string', 'null'] },
          end_date:    { type: ['string', 'null'] },
          period:      { type: 'string' },
          highlight:   { type: 'string' },
          description: { type: 'string' },
          quota:       { type: 'string' },
          cost:        { type: 'string' },
          tc_link:     { type: 'string' },
          analysis_points: { type: 'array', items: { type: 'string' } },
        },
        required: ['name', 'types', 'is_bau', 'highlight'],
      },
    },
  },
  required: ['promotions'],
};

// ── Phase 1: playwright-cli crawl ────────────────────────────────────────────

function fetchPageText(
  url: string,
  waitMs: number,
  sessionId: string,
  maxRetries: number = 2,
): string | null {
  const JS_EXTRACT =
    `(() => { ` +
    `const SKIP=new Set(['SCRIPT','STYLE','NAV','HEADER','FOOTER','NOSCRIPT','SVG','IFRAME','HEAD']); ` +
    `function w(n){if(n.nodeType===3)return n.textContent||''; ` +
    `if(SKIP.has(n.tagName))return''; ` +
    `return Array.from(n.childNodes).map(w).join(' ');} ` +
    `return (document.body?w(document.body):'').replace(/\\s+/g,' ').trim(); ` +
    `})()`;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      if (attempt === 1) {
        execSync(`playwright-cli -s=${sessionId} open "${url}"`,
          { stdio: 'pipe', timeout: 90_000 });
      } else {
        execSync(`playwright-cli -s=${sessionId} goto "${url}"`,
          { stdio: 'pipe', timeout: 90_000 });
      }

      if (waitMs > 0) {
        execSync(
          `playwright-cli -s=${sessionId} run-code "async page => { await page.waitForTimeout(${waitMs}); }"`,
          { stdio: 'pipe', timeout: waitMs + 15_000 },
        );
      }

      // Scroll to trigger lazy-loaded content, then wait a bit more
      execSync(
        `playwright-cli -s=${sessionId} run-code "async page => { ` +
        `await page.evaluate(()=>window.scrollTo(0,document.body.scrollHeight)); ` +
        `await page.waitForTimeout(1500); }"`,
        { stdio: 'pipe', timeout: 15_000 },
      );

      // Extract visible text
      const raw = execSync(
        `playwright-cli -s=${sessionId} --raw eval "${JS_EXTRACT}"`,
        { stdio: 'pipe', timeout: 15_000 },
      ).toString().trim();

      if (raw.length >= MIN_CONTENT_CHARS) return raw;

      if (attempt < maxRetries) {
        waitMs = Math.max(waitMs, 5000) * 1.5;
      }
    } catch {
      // silently retry
    }
  }

  return null;
}

/**
 * Phase 1: crawl all URLs for a bank in parallel, printing per-URL progress.
 * Returns collected page texts (deduplicated, truncated, scrubbed).
 */
async function crawlBankUrls(config: BankConfig): Promise<PageText[]> {
  const { id: bankId, urls, waitExtra = 0 } = config;
  const waitMs = Math.max(2000, waitExtra);
  const total = urls.length;

  const settled = await pLimit(
    urls.map((url, i) => async () => {
      const sessionId = `scrape-${bankId}-${i}-${Date.now()}`;
      const t0 = Date.now();
      const label = `    [${i + 1}/${total}] ${url}`;

      // Validate URL belongs to bank's domain (Copy3 enhancement)
      if (!isValidBankUrl(url, bankId)) {
        console.log(`${label} ⚠️  URL outside bank domain — skipping`);
        return null;
      }

      let text: string | null = null;
      try {
        text = fetchPageText(url, waitMs, sessionId, config.maxRetries ?? 2);
      } finally {
        try {
          execSync(`playwright-cli -s=${sessionId} close`, { stdio: 'pipe', timeout: 10_000 });
        } catch { /* ignore */ }
      }

      const elapsed = ((Date.now() - t0) / 1000).toFixed(1);

      if (!text) {
        console.log(`${label} ⚠️  thin/failed (${elapsed}s)`);
        return null;
      }

      // Scrub blocked content from scraped text (Copy3 enhancement)
      const scrubbed = scrubBlockedContent(text, bankId);

      const truncated = scrubbed.slice(0, MAX_CHARS_PER_PAGE);
      console.log(`${label} ✓ ${truncated.length.toLocaleString()} chars (${elapsed}s)`);
      return { url, text: truncated, chars: truncated.length };
    }),
    URL_CONCURRENCY,
  );

  // Collect results, deduplicate by first-500-char hash
  const pages: PageText[] = [];
  const seenHashes = new Set<string>();

  for (const outcome of settled) {
    if (outcome.status !== 'fulfilled' || !outcome.value) continue;
    const page = outcome.value;
    const hash = page.text.slice(0, 500);
    if (seenHashes.has(hash)) {
      console.log(`    ♻️  duplicate content skipped: ${page.url}`);
      continue;
    }
    seenHashes.add(hash);
    pages.push(page);
  }

  return pages;
}

// ── Phase 2: AI extraction ───────────────────────────────────────────────────

async function extractPromotions(
  pages: PageText[],
  bankName: string,
  bankId: string,
  today: string,
): Promise<{ promotions: RawPromotion[]; costUsd?: number; durationMs?: number }> {
  let totalChars = 0;
  const sections: string[] = [];
  for (const p of pages) {
    if (totalChars >= MAX_CHARS_TOTAL) break;
    const chunk = p.text.slice(0, MAX_CHARS_TOTAL - totalChars);
    sections.push(`=== SOURCE: ${p.url} ===\n${chunk}`);
    totalChars += chunk.length;
  }
  const combinedText = sections.join('\n\n');

  let systemAppend = EXTRACTION_SYSTEM_PROMPT;
  if (/elebank|ele bank|airstar/i.test(bankName)) {
    systemAppend += '\n' + ELEBANK_FEE_CONTEXT;
  }

  const prompt = `Extract ALL promotions from the following ${bankName} website content.

Bank: ${bankName}
Today's date: ${today}

--- PAGE CONTENT START ---
${combinedText}
--- PAGE CONTENT END ---

Extract every promotion you can find and return as JSON per the schema.`;

  const result: AgentResult = await runAgent({
    prompt,
    systemPromptAppend: systemAppend,
    allowedTools: [],
    maxTurns: 5,
    outputSchema: PROMOTION_LIST_SCHEMA,
    maxBudgetUsd: 1.0,
  });

  if (!result.success) {
    const err = result.errors?.join(', ') || '(no detail)';
    console.log(`  ⚠️  [${bankId.toUpperCase()}] AI extraction failed: ${err}`);
    return { costUsd: result.costUsd, durationMs: result.durationMs, promotions: [] };
  }

  let promotions: RawPromotion[] = [];
  if (result.structuredOutput?.promotions) {
    promotions = result.structuredOutput.promotions as RawPromotion[];
  } else if (result.result) {
    try {
      const m = result.result.match(/\[[\s\S]*\]/);
      if (m) promotions = JSON.parse(m[0]);
    } catch {
      console.log(`  ⚠️  [${bankId.toUpperCase()}] Could not parse AI result text`);
    }
  }

  return { promotions, costUsd: result.costUsd, durationMs: result.durationMs };
}

// ── Post-processing helpers ──────────────────────────────────────────────────

function stampPromotions(
  promos: RawPromotion[],
  bankId: string,
  bankName: string,
  defaultUrl: string,
): RawPromotion[] {
  for (const p of promos) {
    p.bank = bankId;
    p.bName = bankName;
    p.link = p.link || defaultUrl;
    p.tc_link = p.tc_link || defaultUrl;
    p.types = p.types?.length ? p.types : ['Others'];
    p.is_bau = p.is_bau ?? false;
    p.start_date = p.start_date ?? null;
    p.end_date = p.end_date ?? null;
    p.period = p.period || 'Ongoing';
    p.highlight = p.highlight || '';
    p.description = p.description || '';
    p.quota = p.quota || 'Check official website';
    p.cost = p.cost || 'Check official website';
    p.analysis_points = p.analysis_points?.length ? p.analysis_points : [];
    if (!p.title && p.name) p.title = p.name;
  }
  return promos;
}

function applyBauOverrides(promos: RawPromotion[], bankId: string): RawPromotion[] {
  const bankSpecific = (BAU_OVERRIDES[bankId.toLowerCase()] || []).map(s => s.toLowerCase());
  const globalList = BAU_GLOBAL_OVERRIDES.map(s => s.toLowerCase());
  const allOverrides = [...bankSpecific, ...globalList];
  if (!allOverrides.length) return promos;

  for (const p of promos) {
    const title = (p.name || p.title || '').toLowerCase();
    if (allOverrides.some(ov => title.includes(ov)) && !p.is_bau) {
      p.is_bau = true;
      console.log(`    🔒 BAU override: ${p.name || p.title}`);
    }
  }
  return promos;
}

function filterNonBankPromotions(promos: RawPromotion[], bankName: string): RawPromotion[] {
  const filtered: RawPromotion[] = [];
  let removed = 0;
  for (const p of promos) {
    // Copy3 enhancement: also check description and tc_link
    if (isNonBankContent(p.name || p.title || '', p.highlight || '', p.description || '', p.tc_link || '')) {
      removed++;
    } else {
      p.types = p.types
        .map(t => ALLOWED_CATEGORIES.includes(t) ? t : 'Others')
        .filter((t, i, arr) => arr.indexOf(t) === i);
      if (!p.types.length) p.types = ['Others'];
      filtered.push(p);
    }
  }
  if (removed) {
    console.log(`  🚫 Non-bank filter: ${removed} removed for ${bankName}`);
  }
  return filtered;
}

// ── Public API ───────────────────────────────────────────────────────────────

/**
 * Scrape a single bank: crawl URLs (Phase 1) then AI-extract (Phase 2).
 */
export async function scrapeBank(config: BankConfig): Promise<ScrapedBank> {
  const { id: bankId, name: bankName, link: defaultUrl } = config;
  const today = hktToday();
  const t0 = Date.now();

  console.log(`\n  🏦 [${bankId.toUpperCase()}] ${bankName} (${config.urls.length} URLs)`);

  // Phase 1: crawl all URLs in parallel
  const pages = await crawlBankUrls(config);

  if (!pages.length) {
    console.log(`  ⚠️  [${bankId.toUpperCase()}] No content collected — skipping AI extraction`);
    return { bankId, bankName, promotions: [], pages: [] };
  }

  const totalChars = pages.reduce((s, p) => s + p.chars, 0);
  console.log(`  🤖 [${bankId.toUpperCase()}] Extracting from ${pages.length} pages (${totalChars.toLocaleString()} chars)...`);

  // Phase 2: AI extract
  const { promotions: raw, costUsd, durationMs } = await extractPromotions(pages, bankName, bankId, today);

  // Post-processing
  let promotions = stampPromotions(raw, bankId, bankName, defaultUrl);
  promotions = applyBauOverrides(promotions, bankId);
  promotions = filterNonBankPromotions(promotions, bankName);

  const bauCount = promotions.filter(p => p.is_bau).length;
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(
    `  ✅ [${bankId.toUpperCase()}] ${bankName}: ${promotions.length} promotions` +
    (bauCount ? ` (${bauCount} BAU)` : '') +
    ` in ${elapsed}s` +
    (costUsd ? ` [$${costUsd.toFixed(4)}]` : ''),
  );

  return { bankId, bankName, promotions, costUsd, durationMs, pages };
}

/**
 * Scrape all configured banks in parallel.
 */
export async function scrapeAllBanks(bankIds?: string[]): Promise<ScrapedBank[]> {
  const configs = bankIds
    ? bankIds.map(id => BANK_CONFIGS[id]).filter(Boolean)
    : Object.values(BANK_CONFIGS);

  const settled = await pLimit(
    configs.map(cfg => () => scrapeBank(cfg)),
    BANK_CONCURRENCY,
  );

  const results: ScrapedBank[] = settled.map((outcome, i) => {
    if (outcome.status === 'fulfilled') return outcome.value;
    console.log(`  ❌ Error scraping ${configs[i].name}: ${outcome.reason?.message || outcome.reason}`);
    return { bankId: configs[i].id, bankName: configs[i].name, promotions: [] };
  });

  const total = results.reduce((s, r) => s + r.promotions.length, 0);
  console.log(`\n📊 Scraping complete: ${total} promotions from ${results.length} banks`);
  return results;
}
