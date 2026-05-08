/**
 * Matcher Agent — matches newly scraped promotions against existing DB records.
 * Uses Agent SDK to identify which new promotions correspond to existing DB entries.
 * Ported from Python vbank-tracker ai_helper.py ai_match_against_existing.
 * @author Alfie
 */

import { runAgent, type AgentResult } from './client.js';
import type { RawPromotion } from './scraper.js';

// ── Types ──────────────────────────────────────────────────────────────────

export interface DbPromotion {
  id: number;
  title: string;
  bank_name?: string;
  [key: string]: any;
}

// ── Output schema ──────────────────────────────────────────────────────────

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

// ── Matching rules (embedded in prompt) ────────────────────────────────────

const MATCHING_RULES = `
MATCHING RULES — mark as MATCH in all these cases:

1. Product name synonyms:
   "余額+" = "Deposit Plus" = "Balance+" → always the same product

2. Fee synonyms:
   "Zero Fee" = "0% Fee" = "Fee Waiver" = "No Fee" = "Commission-Free"

3. Promo codes:
   Same code stem (ignoring trailing 2-digit year) → MATCH

4. Crypto fee promotions:
   Any title about crypto + fee waiver/removal → MATCH each other

5. Best-in-Town:
   Any title containing "Best-in-Town" for the same bank → MATCH

6. Trip.com:
   "Trip.com Annual Discount" ↔ "Trip.com x Mox Credit Card Year-Round Promotion" → MATCH

7. SWIFT / Payment Connect:
   "SWIFT Transfers" ↔ "Payment Connect" → MATCH

8. WeLab Global Wallet FX:
   Any WeLab Global Wallet + FX/exchange/remittance title → MATCH

9. Payroll switch:
   Any payroll + switch/deposit/benefit → MATCH

10. Fund zero-fee:
    "$0 Fund Trading Fee" ↔ "Zero Fund Subscription Fee" ↔ "0% Fund Subscription Fee" → MATCH

11. Referral programs:
    "多友多賞" ↔ "Mox Referral Programme" ↔ "Refer a Friend HKD300" → MATCH

12. Account opening BAU:
    "Account Opening in 3 Minutes" ↔ "Quick Account Opening" → MATCH

13. EleBank HK stock BAU (formerly Airstar Bank):
    "Lifetime $0 Commissions on HK Stocks" ↔ "HK Stock Trading with $0 Commission"
    ↔ "HK Stock $0 Commission per Order" → MATCH

14. When uncertain → declare MATCH`;

// ── Main matcher function ──────────────────────────────────────────────────

/**
 * AI-powered matching of new promotions against existing DB records.
 * Returns a map of new_index → db_id.
 */
export async function aiMatchAgainstExisting(
  newPromos: RawPromotion[],
  existingPromos: DbPromotion[],
  bankName: string,
): Promise<Map<number, number>> {
  const matchMap = new Map<number, number>();

  if (!newPromos.length || !existingPromos.length) return matchMap;

  const newLines = newPromos.map(
    (p, i) => `[NEW-${i}] ${(p.name || p.title || '').trim()}`,
  ).join('\n');

  const exLines = existingPromos.map(
    p => `[DB-${p.id}] ${(p.title || '').trim()}`,
  ).join('\n');

  const prompt = `You are a strict deduplication assistant for a Hong Kong virtual bank promotions database.
Bank: ${bankName}

${MATCHING_RULES}

NEWLY SCRAPED (this run):
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
          const parsed = parseMatchFromText(result.result);
          if (parsed.size) return parsed;
        }
        if (attempt === 0) {
          console.log(`  🔄 Retry ai_match_against_existing for ${bankName}...`);
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
      console.log(`  🤖 ai_match_against_existing [${bankName}]: ${msg}`);
      return matchMap;
    } catch (err: any) {
      if (attempt === 0) {
        console.log(`  ⚠️  ai_match_against_existing [${bankName}] attempt 1: ${err.message} — retrying`);
      } else {
        console.log(`  ⚠️  ai_match_against_existing [${bankName}]: ${err.message} — skipping`);
        return matchMap;
      }
    }
  }
  return matchMap;
}

/**
 * Fallback: parse match result from raw text (e.g. {"0": "47", "3": "112"}).
 */
function parseMatchFromText(text: string): Map<number, number> {
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
