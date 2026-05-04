/**
 * Product extraction agent — extracts permanent product offerings from bank pages.
 * Uses the same Phase 1 crawled text as the promotion scraper,
 * but with a different AI prompt focused on products (not time-limited promos).
 * @author Alfie
 */

import { runAgent, type AgentResult } from './client';
import { type PageText } from './scraper';
import { BANK_CONFIGS, type BankConfig } from '../config/banks';
import { hktToday } from '../utils/hkt';

// ── Constants ───────────────────────────────────────────────────────────────

const MAX_CHARS_PER_PAGE = 15_000;
const MAX_CHARS_TOTAL    = 60_000;

// ── Types ───────────────────────────────────────────────────────────────────

export interface RawProduct {
  product_name: string;
  category: string;
  subcategory?: string;
  description: string;
  features: string[];
  interest_rate: string;
  fees: string;
  min_deposit: string;
  min_balance: string;
  url: string;
  is_bau: boolean;
  // stamped fields
  bank?: string;
  bName?: string;
}

export interface ExtractedProducts {
  bankId: string;
  bankName: string;
  products: RawProduct[];
  costUsd?: number;
  durationMs?: number;
}

// ── 10 Product Categories ───────────────────────────────────────────────────

export const PRODUCT_CATEGORIES = [
  'US Stock',
  'HK Stock',
  'Crypto',
  'Fund',
  'Credit Card',
  'Saving/Current Deposit',
  'Time Deposit',
  'Currency Exchange',
  'Personal Loan',
  'Others',
] as const;

export type ProductCategory = typeof PRODUCT_CATEGORIES[number];

// ── AI extraction prompt ────────────────────────────────────────────────────

const PRODUCT_EXTRACTION_SYSTEM_PROMPT = `You are a specialist at extracting permanent product offerings from Hong Kong virtual bank websites.

CRITICAL RULES:
1. Extract EVERY permanent product offering you can find — savings accounts, investment products, loan products, credit cards, etc.
2. Do NOT extract time-limited promotions — those are handled separately. Focus on always-available products.
3. product_name must be in English, descriptive, and specific (e.g., "ZA Savings Account" not just "Savings").
4. For each product, assign exactly ONE primary category from this list:
   - US Stock: US stock/equity trading products
   - HK Stock: HK stock/equity trading products
   - Crypto: Cryptocurrency trading or holding products
   - Fund: Fund investment products (mutual funds, ETFs, etc.)
   - Credit Card: Credit card products
   - Saving/Current Deposit: Savings or current/checking account products
   - Time Deposit: Time/fixed deposit products
   - Currency Exchange: Foreign exchange/remittance products
   - Personal Loan: Personal loan products (instalment, revolving, etc.)
   - Others: Any product not fitting above categories
5. features: List key features as an array (e.g., ["No minimum balance", "Instant transfers", "USD support"])
6. interest_rate: State the interest rate if applicable (e.g., "0.2% p.a.", "Up to 3.6% p.a."). Use "N/A" if not applicable.
7. fees: State any fees (e.g., "No annual fee", "0.15% commission"). Use "Free" or "N/A" if no fees.
8. min_deposit: Minimum deposit or balance requirement. Use "None" if no minimum.
9. min_balance: Minimum balance to maintain. Use "None" if no minimum.
10. is_bau: For products, this should almost always be true since products are permanent offerings.
11. Extract ONLY products offered by the specified bank — skip third-party or partner products unless deeply integrated.
12. If a bank has multiple variants of the same product (e.g., different tiers), extract each as a separate entry.
13. For stock trading products, clearly distinguish US Stock vs HK Stock categories.
14. For deposit products, clearly distinguish Saving/Current Deposit vs Time Deposit.

Return a JSON array of product objects with these fields:
- product_name: Full descriptive English name
- category: One of the 10 categories above
- subcategory: Optional subcategory (e.g., "Fractional Shares" under "US Stock")
- description: 2-3 sentences describing the product
- features: Array of key features
- interest_rate: Interest rate info or "N/A"
- fees: Fee info or "Free" or "N/A"
- min_deposit: Minimum deposit requirement or "None"
- min_balance: Minimum balance requirement or "None"
- url: Source URL
- is_bau: boolean (should be true for permanent products)`;

// ── Output schema ────────────────────────────────────────────────────────────

const PRODUCT_LIST_SCHEMA = {
  type: 'object',
  properties: {
    products: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          product_name:  { type: 'string' },
          category:      { type: 'string' },
          subcategory:   { type: 'string' },
          description:   { type: 'string' },
          features:      { type: 'array', items: { type: 'string' } },
          interest_rate: { type: 'string' },
          fees:          { type: 'string' },
          min_deposit:   { type: 'string' },
          min_balance:   { type: 'string' },
          url:           { type: 'string' },
          is_bau:        { type: 'boolean' },
        },
        required: ['product_name', 'category', 'is_bau'],
      },
    },
  },
  required: ['products'],
};

// ── Phase 2: AI extraction ──────────────────────────────────────────────────

async function extractProducts(
  pages: PageText[],
  bankName: string,
  bankId: string,
  today: string,
): Promise<{ products: RawProduct[]; costUsd?: number; durationMs?: number }> {
  let totalChars = 0;
  const sections: string[] = [];
  for (const p of pages) {
    if (totalChars >= MAX_CHARS_TOTAL) break;
    const chunk = p.text.slice(0, MAX_CHARS_TOTAL - totalChars);
    sections.push(`=== SOURCE: ${p.url} ===\n${chunk}`);
    totalChars += chunk.length;
  }
  const combinedText = sections.join('\n\n');

  const prompt = `Extract ALL permanent product offerings from the following ${bankName} website content.

Bank: ${bankName}
Today's date: ${today}

--- PAGE CONTENT START ---
${combinedText}
--- PAGE CONTENT END ---

Extract every product you can find and return as JSON per the schema.`;

  const result: AgentResult = await runAgent({
    prompt,
    systemPromptAppend: PRODUCT_EXTRACTION_SYSTEM_PROMPT,
    allowedTools: [],
    maxTurns: 5,
    outputSchema: PRODUCT_LIST_SCHEMA,
    maxBudgetUsd: 1.0,
  });

  if (!result.success) {
    const err = result.errors?.join(', ') || '(no detail)';
    console.log(`  ⚠️  [${bankId.toUpperCase()}] Product extraction failed: ${err}`);
    return { costUsd: result.costUsd, durationMs: result.durationMs, products: [] };
  }

  let products: RawProduct[] = [];
  if (result.structuredOutput?.products) {
    products = result.structuredOutput.products as RawProduct[];
  } else if (result.result) {
    try {
      const m = result.result.match(/\[[\s\S]*\]/);
      if (m) products = JSON.parse(m[0]);
    } catch {
      console.log(`  ⚠️  [${bankId.toUpperCase()}] Could not parse product result text`);
    }
  }

  return { products, costUsd: result.costUsd, durationMs: result.durationMs };
}

// ── Post-processing ─────────────────────────────────────────────────────────

const VALID_CATEGORIES = new Set<string>(PRODUCT_CATEGORIES);

function stampProducts(
  prods: RawProduct[],
  bankId: string,
  bankName: string,
  defaultUrl: string,
): RawProduct[] {
  for (const p of prods) {
    p.bank = bankId;
    p.bName = bankName;
    p.url = p.url || defaultUrl;
    p.category = VALID_CATEGORIES.has(p.category) ? p.category : 'Others';
    p.is_bau = p.is_bau ?? true;
    p.description = p.description || '';
    p.features = p.features?.length ? p.features : [];
    p.interest_rate = p.interest_rate || 'N/A';
    p.fees = p.fees || 'N/A';
    p.min_deposit = p.min_deposit || 'None';
    p.min_balance = p.min_balance || 'None';
    p.subcategory = p.subcategory || '';
  }
  return prods;
}

// ── Public API ───────────────────────────────────────────────────────────────

/**
 * Extract products from pre-crawled page text.
 * Reuses Phase 1 results from the promotion scraper — no additional crawling needed.
 */
export async function extractBankProducts(
  pages: PageText[],
  config: BankConfig,
): Promise<ExtractedProducts> {
  const { id: bankId, name: bankName, link: defaultUrl } = config;
  const today = hktToday();
  const t0 = Date.now();

  if (!pages.length) {
    console.log(`  ⚠️  [${bankId.toUpperCase()}] No pages for product extraction — skipping`);
    return { bankId, bankName, products: [] };
  }

  const totalChars = pages.reduce((s, p) => s + p.chars, 0);
  console.log(`  🏦 [${bankId.toUpperCase()}] Extracting products from ${pages.length} pages (${totalChars.toLocaleString()} chars)...`);

  const { products: raw, costUsd, durationMs } = await extractProducts(pages, bankName, bankId, today);

  const products = stampProducts(raw, bankId, bankName, defaultUrl);

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(
    `  ✅ [${bankId.toUpperCase()}] ${bankName}: ${products.length} products in ${elapsed}s` +
    (costUsd ? ` [$${costUsd.toFixed(4)}]` : ''),
  );

  return { bankId, bankName, products, costUsd, durationMs };
}
