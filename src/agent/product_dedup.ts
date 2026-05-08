/**
 * Product Dedup & Matching — within-batch dedup and DB matching for products.
 * Follows the same pattern as dedup.ts and matcher.ts, adapted for products.
 * @author Alfie
 */

import { runAgent, type AgentResult } from './client.js';
import type { RawProduct } from './products.js';
import { PRODUCT_CATEGORIES } from './products.js';

// ── Types ───────────────────────────────────────────────────────────────────

export interface ProductDedupGroup {
  keep_index: number;
  duplicate_indices: number[];
  reason: string;
}

export interface DbProduct {
  id: number;
  product_name: string;
  category: string;
  bank_name?: string;
  [key: string]: any;
}

// ── Within-batch dedup schema ───────────────────────────────────────────────

const DEDUP_SCHEMA = {
  type: 'object',
  properties: {
    groups: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          keep_index: { type: 'number' },
          duplicate_indices: { type: 'array', items: { type: 'number' } },
          reason: { type: 'string' },
        },
        required: ['keep_index', 'duplicate_indices', 'reason'],
      },
    },
  },
  required: ['groups'],
};

// ── Product synonym rules ───────────────────────────────────────────────────

const PRODUCT_SYNONYM_RULES = `
PRODUCT SYNONYM RULES — these ALWAYS refer to the same product:

Savings products:
  "GoSave" = "GoSave Time Deposit" = "GoSave 2.0" = "GoSave High Interest Savings"
  "liviSave" = "liviSave Preferential Interest Rate" = "liviSave Savings"
  "USD Savings" = "US Dollar Savings Account" = "USD Savings Account"
  "HK Dollar Savings" = "HKD Savings Account"

Stock trading:
  "US Stock Trading" = "US Equity Trading" = "US Stock Investment Platform"
  "HK Stock Trading" = "HK Equity Trading" = "HK Stock Investment Platform"
  "Fractional Shares" = "Fractional Share Trading" = "US Fractional Shares"

Crypto:
  "Crypto Trading" = "Digital Asset Trading" = "Cryptocurrency Exchange"

Fund:
  "Fund Investment" = "Fund Platform" = "Investment Fund" = "Mutual Fund Platform"

Time Deposit:
  Any two time deposit products from the same bank with the same currency → SAME

Currency Exchange:
  "Foreign Exchange" = "FX Exchange" = "Currency Exchange" = "Global Remittance" = "Global Wallet"

Personal Loan:
  "Personal Instalment Loan" = "Personal Loan" = "Instalment Loan"
  "Revolving Loan" = "Personal Revolving Loan" = "Flexi Loan"

Credit Card:
  Cards with the same card name (ignoring "Virtual" prefix) → SAME

General: If two products have the same category AND nearly identical names → SAME`;

// ── DB matching schema ──────────────────────────────────────────────────────

const MATCH_SCHEMA = {
  type: 'object',
  properties: {
    matches: {
      type: 'object',
      additionalProperties: { type: 'number' },
    },
  },
  required: ['matches'],
};

const PRODUCT_MATCHING_RULES = `
PRODUCT MATCHING RULES — mark as MATCH in all these cases:

1. Same category + same core product name (ignoring version numbers, adjectives):
   "GoSave 2.0" ↔ "GoSave" → MATCH (both Saving/Current Deposit)

2. Same category + synonymous names:
   "US Stock Trading" ↔ "US Equity Trading Platform" → MATCH (both US Stock)

3. Same category + same function:
   "Crypto Trading Fee Free" ↔ "Digital Asset Trading" → MATCH (both Crypto)

4. Same category + localized names:
   "外幣兌換" ↔ "Currency Exchange" → MATCH (both Currency Exchange)

5. Time deposit products in the same currency from the same bank → MATCH

6. When uncertain → declare MATCH`;

// ── Within-batch dedup ──────────────────────────────────────────────────────

/**
 * AI-powered within-batch deduplication of product names.
 * Returns a map of duplicate_index → keep_index.
 */
export async function aiDedupProducts(
  products: RawProduct[],
  bankName: string,
): Promise<Map<number, number>> {
  const dupMap = new Map<number, number>();

  if (products.length < 2) return dupMap;

  const lines = products.map(
    (p, i) => `${i}. [${p.category}] ${p.product_name}`,
  ).join('\n');

  const prompt = `You are a strict deduplication assistant for a Hong Kong virtual bank products database.
Bank: ${bankName}

Your task: Find products that are THE SAME underlying product, just described differently.
When genuinely uncertain → mark as DUPLICATE. It is better to merge than to leave duplicates.

${PRODUCT_SYNONYM_RULES}

Return ONLY valid compact JSON — no markdown, no code fences, no explanation:
{"groups":[{"keep_index":0,"duplicate_indices":[1,2],"reason":"one sentence"}]}
If there are NO duplicates, return exactly: {"groups":[]}

Products to evaluate (0-indexed, category shown in brackets):
${lines}`;

  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const result: AgentResult = await runAgent({
        prompt,
        outputSchema: DEDUP_SCHEMA,
        maxTurns: 5,
      });

      if (!result.success || !result.structuredOutput) {
        if (attempt === 0) {
          console.log(`  🔄 Retry aiDedupProducts for ${bankName}...`);
          continue;
        }
        return dupMap;
      }

      const groups: ProductDedupGroup[] = result.structuredOutput.groups || [];
      for (const g of groups) {
        for (const dup of g.duplicate_indices) {
          dupMap.set(dup, g.keep_index);
        }
      }

      if (dupMap.size) {
        console.log(`  🤖 aiDedupProducts [${bankName}]: ${dupMap.size} duplicate(s)`);
      }
      return dupMap;
    } catch (err: any) {
      if (attempt === 0) {
        console.log(`  ⚠️  aiDedupProducts [${bankName}] attempt 1 failed: ${err.message} — retrying`);
      } else {
        console.log(`  ⚠️  aiDedupProducts [${bankName}]: ${err.message} — skipping`);
        return dupMap;
      }
    }
  }
  return dupMap;
}

/**
 * Apply dedup results: remove duplicate products.
 */
export function applyProductDedup(
  products: RawProduct[],
  dupMap: Map<number, number>,
): RawProduct[] {
  if (!dupMap.size) return products;

  const removeIndices = new Set(dupMap.keys());
  return products.filter((_, i) => !removeIndices.has(i));
}

// ── DB matching ──────────────────────────────────────────────────────────────

/**
 * AI-powered matching of new products against existing DB records.
 * Returns a map of new_index → db_id.
 */
export async function aiMatchProductsAgainstExisting(
  newProducts: RawProduct[],
  existingProducts: DbProduct[],
  bankName: string,
): Promise<Map<number, number>> {
  const matchMap = new Map<number, number>();

  if (!newProducts.length || !existingProducts.length) return matchMap;

  const newLines = newProducts.map(
    (p, i) => `[NEW-${i}] [${p.category}] ${(p.product_name || '').trim()}`,
  ).join('\n');

  const exLines = existingProducts.map(
    p => `[DB-${p.id}] [${p.category || ''}] ${(p.product_name || '').trim()}`,
  ).join('\n');

  const prompt = `You are a strict deduplication assistant for a Hong Kong virtual bank products database.
Bank: ${bankName}

${PRODUCT_MATCHING_RULES}

NEWLY EXTRACTED (this run):
${newLines}

ALREADY IN DATABASE:
${exLines}

For each [NEW-N] that matches a [DB-ID], output that pair.
Respond ONLY with compact JSON. Key = new index (string). Value = DB id (string).
Example: {"0": "47", "3": "112"}
If no matches: {"matches":{}}
No explanation. No markdown. No code fences.`;

  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const result: AgentResult = await runAgent({
        prompt,
        outputSchema: MATCH_SCHEMA,
        maxTurns: 5,
      });

      if (!result.success || !result.structuredOutput) {
        if (result.result) {
          const parsed = parseProductMatchFromText(result.result);
          if (parsed.size) return parsed;
        }
        if (attempt === 0) {
          console.log(`  🔄 Retry aiMatchProductsAgainstExisting for ${bankName}...`);
          continue;
        }
        return matchMap;
      }

      const matches = result.structuredOutput.matches || {};
      for (const [k, v] of Object.entries(matches)) {
        const newIdx = parseInt(k, 10);
        const dbId = typeof v === 'number' ? v : parseInt(String(v), 10);
        if (!isNaN(newIdx) && !isNaN(dbId)) {
          matchMap.set(newIdx, dbId);
        }
      }

      const msg = matchMap.size
        ? `${matchMap.size} match(es)`
        : '0 matches — all appear genuinely new';
      console.log(`  🤖 aiMatchProductsAgainstExisting [${bankName}]: ${msg}`);
      return matchMap;
    } catch (err: any) {
      if (attempt === 0) {
        console.log(`  ⚠️  aiMatchProductsAgainstExisting [${bankName}] attempt 1: ${err.message} — retrying`);
      } else {
        console.log(`  ⚠️  aiMatchProductsAgainstExisting [${bankName}]: ${err.message} — skipping`);
        return matchMap;
      }
    }
  }
  return matchMap;
}

function parseProductMatchFromText(text: string): Map<number, number> {
  const matchMap = new Map<number, number>();
  try {
    let cleaned = text.replace(/^```[a-z]*\n?/, '').replace(/\n?```$/, '').trim();
    const m = cleaned.match(/\{[\s\S]*\}/);
    if (m) {
      const data = JSON.parse(m[0]);
      for (const [k, v] of Object.entries(data)) {
        const newIdx = parseInt(k, 10);
        const dbId = parseInt(String(v), 10);
        if (!isNaN(newIdx) && !isNaN(dbId)) {
          matchMap.set(newIdx, dbId);
        }
      }
    }
  } catch {
    // ignore parse errors
  }
  return matchMap;
}
