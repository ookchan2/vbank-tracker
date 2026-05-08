/**
 * Product Comparison Agent — generates ZA Bank baseline comparisons for products.
 * For each non-ZA bank product, finds the equivalent ZA Bank product in the
 * same category and generates a pros/cons comparison.
 * @author Alfie
 */

import { runAgent, type AgentResult } from './client.js';
import { PRODUCT_CATEGORIES, type ProductCategory } from './products.js';

// ── Types ───────────────────────────────────────────────────────────────────

export interface ProductComparison {
  bank: string;
  category: string;
  product_name: string;
  za_product: string;
  pros_vs_za: string[];
  cons_vs_za: string[];
  verdict: string;
}

export interface ProductsByBank {
  [bankId: string]: {
    bankName: string;
    products: Record<string, any>[];
  };
}

// ── Output schema ────────────────────────────────────────────────────────────

const COMPARISON_SCHEMA = {
  type: 'object',
  properties: {
    comparisons: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          bank:         { type: 'string' },
          category:     { type: 'string' },
          product_name: { type: 'string' },
          za_product:   { type: 'string' },
          pros_vs_za:   { type: 'array', items: { type: 'string' } },
          cons_vs_za:   { type: 'array', items: { type: 'string' } },
          verdict:      { type: 'string' },
        },
        required: ['bank', 'category', 'product_name', 'za_product', 'pros_vs_za', 'cons_vs_za', 'verdict'],
      },
    },
  },
  required: ['comparisons'],
};

// ── Comparison prompt ────────────────────────────────────────────────────────

const COMPARISON_SYSTEM_PROMPT = `You are a Hong Kong virtual bank product comparison expert.
You compare products from various HK virtual banks against ZA Bank (眾安銀行) as the baseline.

COMPARISON RULES:
1. For each non-ZA product, find the closest equivalent ZA Bank product in the same category.
2. If ZA Bank has no product in that category, set za_product to "ZA Bank does not offer this product" and note what ZA Bank offers instead (if anything).
3. pros_vs_za: 2-3 advantages the non-ZA product has over ZA Bank's equivalent. Be specific with numbers/rates when available.
4. cons_vs_za: 2-3 disadvantages or areas where ZA Bank's product is stronger. Be specific.
5. verdict: One of "Better", "Worse", "Comparable", or "Different niche" — with a one-sentence explanation.
6. Be factual and fair — do not favor any particular bank. Base comparisons on the actual product details provided.
7. If interest rates or fees are mentioned, use them in the comparison.
8. Categories: ${PRODUCT_CATEGORIES.join(', ')}`;

// ── Public API ───────────────────────────────────────────────────────────────

/**
 * Generate ZA Bank comparisons for all non-ZA products.
 * Processes one bank at a time to stay within token limits.
 */
export async function generateProductComparisons(
  productsByBank: ProductsByBank,
): Promise<Map<string, ProductComparison>> {
  const comparisonMap = new Map<string, ProductComparison>();

  const zaProducts = productsByBank['za']?.products || [];
  if (!zaProducts.length) {
    console.log('  ⚠️  No ZA Bank products found — skipping product comparisons');
    return comparisonMap;
  }

  // Build ZA product summary for reference
  const zaSummary = zaProducts.map(
    p => `[${p.category || 'Others'}] ${p.product_name || ''}: rate=${p.interest_rate || 'N/A'}, fees=${p.fees || 'N/A'}, min_deposit=${p.min_deposit || 'None'}`,
  ).join('\n');

  for (const [bankId, { bankName, products }] of Object.entries(productsByBank)) {
    if (bankId === 'za' || !products.length) continue;

    // Process in batches of up to 10 products per API call
    const batchSize = 10;
    for (let offset = 0; offset < products.length; offset += batchSize) {
      const batch = products.slice(offset, offset + batchSize);
      const batchProducts = batch.map(
        p => `[${p.category || 'Others'}] ${p.product_name || ''}: rate=${p.interest_rate || 'N/A'}, fees=${p.fees || 'N/A'}, min_deposit=${p.min_deposit || 'None'}`,
      ).join('\n');

      const prompt = `Compare these ${bankName} products against ZA Bank equivalents.

ZA BANK PRODUCTS:
${zaSummary}

${bankName.toUpperCase()} PRODUCTS:
${batchProducts}

For each ${bankName} product, generate a comparison against the most similar ZA Bank product in the same category.
Return as JSON per the schema.`;

      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const result: AgentResult = await runAgent({
            prompt,
            systemPromptAppend: COMPARISON_SYSTEM_PROMPT,
            outputSchema: COMPARISON_SCHEMA,
            allowedTools: [],
            maxTurns: 5,
            maxBudgetUsd: 0.5,
          });

          if (!result.success || !result.structuredOutput) {
            if (attempt === 0) {
              console.log(`  🔄 Retry product comparison for ${bankName}...`);
              continue;
            }
            break;
          }

          const comparisons: ProductComparison[] = result.structuredOutput.comparisons || [];
          for (let i = 0; i < batch.length && i < comparisons.length; i++) {
            const product = batch[i];
            const comp = comparisons[i];
            // Key by product id or name for later lookup
            const key = product.id
              ? `${bankId}:${product.id}`
              : `${bankId}:${product.product_name}`;
            comparisonMap.set(key, comp);
          }

          console.log(`  🤖 Product comparison [${bankName}]: ${comparisons.length} comparison(s) generated`);
          break;
        } catch (err: any) {
          if (attempt === 0) {
            console.log(`  ⚠️  Product comparison [${bankName}] attempt 1: ${err.message} — retrying`);
          } else {
            console.log(`  ⚠️  Product comparison [${bankName}]: ${err.message} — skipping`);
          }
        }
      }
    }
  }

  console.log(`  ✅ Product comparisons: ${comparisonMap.size} total`);
  return comparisonMap;
}
