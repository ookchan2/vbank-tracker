/**
 * Dedup Agent — within-batch deduplication of promotion titles.
 * Uses Agent SDK to identify duplicate promotions based on synonym rules.
 * Ported from Python vbank-tracker ai_helper.py ai_dedup_titles.
 * @author Alfie
 */

import { runAgent, type AgentResult } from './client';
import type { RawPromotion } from './scraper';

// ── Types ──────────────────────────────────────────────────────────────────

export interface DedupGroup {
  keep_index: number;
  duplicate_indices: number[];
  reason: string;
}

// ── Output schema ──────────────────────────────────────────────────────────

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

// ── Synonym rules (embedded in prompt) ─────────────────────────────────────

const SYNONYM_RULES = `
SYNONYM RULES — these are ALWAYS the same promotion:

Product synonyms:
  "余額+"  =  "Deposit Plus"  =  "Balance+"
  Any title with "余額+", "Deposit Plus", or "Balance+" → same product

Fee synonyms:
  "Zero Fee" = "0% Fee" = "Fee Waiver" = "No Fee" = "Commission-Free"

Speed synonyms:
  "Quick" = "Fast" = "Instant" = "Express" = "Immediate"

Crypto synonyms:
  "Crypto Trading Fee Waiver" = "0% Crypto Platform Fee" = "Zero Fee Cryptocurrency Trading"
  = "Digital Asset Trading Fee Exemption"

Fund subscription synonyms:
  "Zero Subscription Fee on All Funds" = "Zero-Fee Fund Subscription & Switching"
  = "Featured Funds with Zero Subscription Fees" = "Zero Fee Investment Funds"
  = "$0 Fund Trading Fee Mode" = "$0基金買賣收費" = "Zero Fund Subscription Fee"
  = "0% Fund Subscription Fee"

Mox × csl:
  "Best-in-Town Plan Offer" = "Best-in-Town Device Plans with Instalments" = anything + "Best-in-Town"

Trip.com:
  "Trip.com Annual Discount" = "Trip.com x Mox Credit Card Year-Round Promotion"
  = "Trip.com Year-Round Exclusive Discount"

Payroll:
  "Payroll Switching Benefits" = "Payroll Switch Benefits" = "Payroll Deposit Benefit"

SWIFT / Payment Connect:
  "Zero Fee SWIFT Transfers" = "Zero-Fee Payment Connect" = "Payment Connect Zero Fee Transfers"

WeLab Global Wallet FX:
  "WeLab Global Wallet Exchange Rate Promotion" = "WeLab Global Wallet - Best Exchange Rates"
  = "Global Remittance Service" = "WeLab Global Wallet Best FX Rates"

Referral programs:
  "Referral Bonus" = "Invite a Friend" = "多友多賞" = "推薦計劃" = "Friend Referral Program"
  = "Refer a Friend" = any title with "推薦碼" or "referral code" + HKD amount

Promo codes: if two titles share the same promo code (ignoring trailing year digits),
  e.g. MOXBILL25 and MOXBILL26 → SAME campaign. MOXHKT25 in both titles → SAME.

Account opening (all BAU — treat as same feature if they appear twice):
  "Quick Account Opening" = "Account Opening in 3 Minutes" = "Mobile Account Opening in 5 Minutes"
  = "Sign Up in Minutes" = "Open Account Instantly"

24/7 banking:
  "24/7 Mobile Banking Services" = "24/7 Digital Banking Services" = "24×7 Banking Services"

Insurance with rate:
  "3.6% Annualized Rate Promotion" = "Insurance Products with Annual Rate up to 3.6%"
  = "Insurance Products with 3.6% Annual Rate" = "Insurance Products with Premium Rebate"

EleBank HK stock trading (all refer to same BAU fee schedule):
  "HK Stock Trading with $0 Commission" = "Lifetime $0 Commissions on HK Stocks"
  = "HK Stock $0 Commission per Order" = "HK Stock $0 Commission + HKD 15 Platform Fee"

GoSave:
  "GoSave 2.0 High Interest Savings" = "GoSave 2.0 Enhanced Savings"

liviSave:
  "liviSave Preferential Interest Rate" = "liviSave Preferential Savings Rate"`;

// ── Main dedup function ────────────────────────────────────────────────────

/**
 * AI-powered within-batch deduplication of promotion titles.
 * Returns a map of duplicate_index → keep_index.
 */
export async function aiDedupTitles(
  promotions: RawPromotion[],
  bankName: string,
): Promise<Map<number, number>> {
  const dupMap = new Map<number, number>();

  if (promotions.length < 2) return dupMap;

  const titles = promotions.map(p => p.name || p.title || '');
  const numbered = titles.map((t, i) => `${i}. ${t}`).join('\n');

  const prompt = `You are a strict deduplication assistant for a Hong Kong virtual bank promotions database.
Bank: ${bankName}

Your task: Find titles that describe THE SAME underlying product or promotion.
When genuinely uncertain → mark as DUPLICATE. It is always better to merge than to leave duplicates.

${SYNONYM_RULES}

Return ONLY valid compact JSON — no markdown, no code fences, no explanation:
{"groups":[{"keep_index":0,"duplicate_indices":[1,2],"reason":"one sentence"}]}
If there are NO duplicates, return exactly: {"groups":[]}

Titles to evaluate (0-indexed):
${numbered}`;

  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const result: AgentResult = await runAgent({
        prompt,
        outputSchema: DEDUP_SCHEMA,
        maxTurns: 5,
      });

      if (!result.success || !result.structuredOutput) {
        if (attempt === 0) {
          console.log(`  🔄 Retry ai_dedup_titles for ${bankName}...`);
          continue;
        }
        return dupMap;
      }

      const groups: DedupGroup[] = result.structuredOutput.groups || [];
      for (const g of groups) {
        for (const dup of g.duplicate_indices) {
          dupMap.set(dup, g.keep_index);
        }
      }

      if (dupMap.size) {
        console.log(`  🤖 ai_dedup_titles [${bankName}]: ${dupMap.size} duplicate(s)`);
      }
      return dupMap;
    } catch (err: any) {
      if (attempt === 0) {
        console.log(`  ⚠️  ai_dedup_titles [${bankName}] attempt 1 failed: ${err.message} — retrying`);
      } else {
        console.log(`  ⚠️  ai_dedup_titles [${bankName}]: ${err.message} — skipping`);
        return dupMap;
      }
    }
  }
  return dupMap;
}

/**
 * Apply dedup results: merge duplicate promotions, keeping the one at keepIndex.
 * Returns deduplicated promotions array.
 */
export function applyDedup(
  promotions: RawPromotion[],
  dupMap: Map<number, number>,
): RawPromotion[] {
  if (!dupMap.size) return promotions;

  const removeIndices = new Set(dupMap.keys());
  return promotions.filter((_, i) => !removeIndices.has(i));
}
