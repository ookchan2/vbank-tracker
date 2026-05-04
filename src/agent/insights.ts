/**
 * Insights Agent — cross-bank strategic analysis of promotions.
 * Generates best_for comparisons, bank_analysis, and competitive insights.
 * Ported from Python vbank-tracker ai_helper.py generate_strategic_insights.
 * Enhanced with Copy3's _usBreakevenShares, expanded CATEGORY_KEYWORDS,
 * diagnostic output, and optional dbFetchFn parameter.
 * @author Alfie
 */

import { runAgent, type AgentResult } from './client';
import { hktToday } from '../utils/hkt';
import { canonicalBankName } from '../utils/filters';

// ── Types ──────────────────────────────────────────────────────────────────

export interface BestForEntry {
  category: string;
  bank: string;
  detail: string;
  is_bau: boolean;
  similar_banks: string[];
  why_others_lose: string;
}

export interface BankAnalysisEntry {
  focus: string;
  strengths: string[];
  expiring_alert: string;
  vs_za_pros: string | null;
  vs_za_cons: string | null;
  count?: number;
  bau_count?: number;
}

export interface StrategicInsights {
  best_for: BestForEntry[];
  bank_analysis: Record<string, BankAnalysisEntry>;
}

// ── Constants ──────────────────────────────────────────────────────────────

const SPARSE_THRESHOLD = 3;

// ── Evidence validation ────────────────────────────────────────────────────

const CONCRETE_EVIDENCE_RE = /\b(?:HKD\s*[\d,]+|\$\s*0\b|\d+\.?\d*\s*%|\d{1,2}\s+[A-Za-z]+\s+20\d\d|20\d\d-\d\d-\d\d|trip\.com|asia\s*miles|\bapr\b|subscription\s*fee|platform\s*fee|trading\s*fee|fee\s*waiver|zero[\s-]fee|free\s+stock|payment\s+connect|global\s+wallet|commission|cashback|cash\s*back|\bflight\b|\bhotel\b|\blounge\b|travel\s*insur|\bagoda\b|booking\.com|旅遊)\b/i;

const VAGUE_DETAIL_PATTERNS = [
  /special\s+\w+[-\s]related\s+promotions?/i,
  /year[- ]round\s+\w+\s+offers?\s+with\s+special/i,
  /^\s*various\b/i,
  /competitive\s+features/i,
  /\bservices?\s+available\s*$/i,
  /no\s+\w+\s+promotions?\s+available/i,
];

// ── Stock trading validation ───────────────────────────────────────────────

const STOCK_CATS = new Set(['HK Stock Trading', 'US Stock Trading', 'Stock Trading']);
const ZA_NAMES = new Set(['za bank', 'za', 'za invest']);
const ELEBANK_NAMES = new Set(['elebank', 'ele bank', 'elebank bank', 'airstar bank', 'airstar', 'airstar invest']);
const PADB_NAMES = new Set(['padb', 'paobank', 'pao bank', 'pao']);

const ZA_HK_PLATFORM_FEE = 18.0;
const ZA_US_PLATFORM_FEE = 1.99;
const ELEBANK_HK_TOTAL = 15.0;
const ELEBANK_HK_PLATFORM = 15.0;
const ELEBANK_US_TOTAL_PER_SHARE = 0.0099;
const ELEBANK_US_MIN_TOTAL = 1.99;
const ELEBANK_US_COMM_PER_SHARE = 0.0049;
const ELEBANK_US_COMM_MIN = 0.99;
const ELEBANK_US_PLAT_PER_SHARE = 0.005;
const ELEBANK_US_PLAT_MIN = 1.00;

/** Copy3: breakeven shares where competitor per-share cost equals ZA Bank flat fee */
function usBreakevenShares(perShareUsd: number): number {
  if (perShareUsd <= 0) return Infinity;
  return ZA_US_PLATFORM_FEE / perShareUsd;
}

// ── Output schema ──────────────────────────────────────────────────────────

const INSIGHTS_SCHEMA = {
  type: 'object',
  properties: {
    best_for: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          category: { type: 'string' },
          bank: { type: 'string' },
          detail: { type: 'string' },
          is_bau: { type: 'boolean' },
          similar_banks: { type: 'array', items: { type: 'string' } },
          why_others_lose: { type: 'string' },
        },
        required: ['category', 'bank', 'detail', 'is_bau', 'similar_banks', 'why_others_lose'],
      },
    },
    bank_analysis: {
      type: 'object',
      additionalProperties: {
        type: 'object',
        properties: {
          focus: { type: 'string' },
          strengths: { type: 'array', items: { type: 'string' } },
          expiring_alert: { type: 'string' },
          vs_za_pros: { type: ['string', 'null'] },
          vs_za_cons: { type: ['string', 'null'] },
        },
        required: ['focus', 'strengths', 'expiring_alert'],
      },
    },
  },
  required: ['best_for', 'bank_analysis'],
};

// ── Build bank summary lines ───────────────────────────────────────────────

function buildBankSummaryLines(promos: any[]): string[] {
  return promos.map(p => {
    const title = (p.name || p.title || 'N/A').slice(0, 80);
    const highlight = (p.highlight || p.description || '').slice(0, 120);
    const period = (p.period || 'Ongoing').slice(0, 60);
    const rawTypes = p.types || ['General'];
    const ptype = (Array.isArray(rawTypes) ? rawTypes.join(', ') : String(rawTypes)).slice(0, 40);
    const bauTag = p.is_bau ? ' [BAU - Permanent Feature]' : '';
    return `  [${ptype}]${bauTag} ${title}: ${highlight} | ${period}`;
  });
}

// ── Supplement sparse banks from DB ────────────────────────────────────────

type DbFetchFn = (bank: string) => any[];

function supplementFromDb(
  promotionsByBank: Record<string, any[]>,
  dbFetchFn: DbFetchFn,
  minPromosPerBank: number = SPARSE_THRESHOLD,
): Record<string, any[]> {
  let supplementedTotal = 0;
  for (const [bank, promos] of Object.entries(promotionsByBank)) {
    if (promos.length >= minPromosPerBank) continue;

    try {
      const dbPromos = dbFetchFn(bank);
      if (!dbPromos?.length) continue;

      const existingTitles = new Set(
        promos.map(p => (p.name || p.title || '').trim().toLowerCase()),
      );
      let added = 0;
      for (const dp of dbPromos) {
        const dt = (dp.title || dp.name || '').trim().toLowerCase();
        if (dt && !existingTitles.has(dt)) {
          promos.push(dp);
          existingTitles.add(dt);
          added++;
        }
      }
      supplementedTotal += added;
      if (added) {
        console.log(`  🔄 supplement_from_db: "${bank}" added ${added} from DB → now ${promos.length} total`);
      }
    } catch (err: any) {
      console.log(`  ⚠️  supplement_from_db: DB fetch failed for "${bank}": ${err.message}`);
    }
  }
  if (supplementedTotal) {
    console.log(`  🔄 supplement_from_db: ${supplementedTotal} DB row(s) merged total`);
  }
  return promotionsByBank;
}

// ── Copy3: diagnostic output ───────────────────────────────────────────────

const DIAGNOSTIC_CATEGORIES: [string, string[]][] = [
  ['HK Stock Trading', ['hk stock', 'hkex', 'commission', 'trading fee', 'platform fee', 'hk stock $0']],
  ['US Stock Trading', ['us stock', 'nyse', 'nasdaq', 'commission', 'trading fee', 'us stock $0']],
  ['Stock Trading', ['stock', 'securities', 'brokerage', 'commission', 'trading fee']],
  ['Crypto Trading', ['crypto', 'bitcoin', 'virtual asset', 'digital asset', 'cryptocurrency', '0% crypto']],
  ['Fund Investment', ['fund', '基金', '認購費', '轉換費', '$0認購費', 'subscription fee']],
  ['Referral Bonus', ['referral', '推薦', 'invite', '多友多賞', '推薦碼', 'refer a friend']],
  ['FX/Multi-Currency', ['fx', 'forex', 'multi-currency', 'global wallet', 'payment connect', 'remittance', 'welab global', 'fps transfer']],
  ['Travel', ['trip.com', 'asia miles', 'flight', 'hotel', 'travel insurance', '旅遊', 'booking.com', 'agoda']],
  ['Spending/CashBack', ['cashback', 'cash back', 'spending reward', 'rebate', 'merchant offer']],
  ['Welcome Bonus', ['welcome', 'new customer', 'account opening', 'sign up', 'hkd8,888', 'hkd888']],
  ['Loan APR', ['loan', 'apr', 'personal loan', 'interest rate', '1.18%', 'tax season']],
  ['Insurance', ['insurance', '保險', 'premium rebate', 'annualized rate']],
  ['Savings/Deposit', ['savings', 'deposit', '存款', 'interest', 'goSave', 'livisave', 'time deposit']],
];

function diagnoseInputData(promotionsByBank: Record<string, any[]>): Record<string, string[]> {
  console.log();
  console.log('='.repeat(70));
  console.log('📊  INSIGHTS INPUT DIAGNOSTIC');
  console.log('='.repeat(70));

  const bankTagMap: Record<string, string[]> = {};

  for (const [bank, promos] of Object.entries(promotionsByBank).sort(([a], [b]) => a.localeCompare(b))) {
    const bauPromos = promos.filter(p => p.is_bau);
    const nonBauPromos = promos.filter(p => !p.is_bau);

    const allTags = new Set<string>();
    for (const p of promos) {
      const raw = p.types || [];
      const tags = Array.isArray(raw) ? raw : [String(raw)];
      for (const t of tags) allTags.add(t);
      for (const field of ['name', 'title', 'highlight', 'description'] as const) {
        const val = (p[field] || '').toLowerCase();
        if (val) allTags.add(val.slice(0, 40));
      }
    }

    const tagDisplay = [...allTags]
      .filter(t => t.length > 1 && t.length <= 12 && t !== 'others' && t !== 'general')
      .sort()
      .join(', ') || '⚠️  NONE';

    const sparseFlag = promos.length < SPARSE_THRESHOLD
      ? '  ⚠️  SPARSE — may cause None slots'
      : '  ✅';

    console.log(
      `  📊 ${bank.padEnd(20)}: ${String(nonBauPromos.length).padStart(2)} active` +
      ` + ${String(bauPromos.length).padStart(2)} BAU` +
      ` = ${String(promos.length).padStart(2)} total` +
      `  | tags: ${tagDisplay.slice(0, 55)}` +
      sparseFlag,
    );
    bankTagMap[bank] = [...allTags];
  }

  console.log();
  console.log('  CATEGORY COVERAGE CHECK:');
  for (const [catName, kwList] of DIAGNOSTIC_CATEGORIES) {
    const coveredBy: string[] = [];
    for (const [bank, promos] of Object.entries(promotionsByBank)) {
      for (const p of promos) {
        const typesStr = Array.isArray(p.types)
          ? p.types.join(' ')
          : String(p.types || '');
        const text = [typesStr, p.name, p.title, p.highlight, p.description]
          .join(' ').toLowerCase();
        if (kwList.some(kw => text.includes(kw.toLowerCase()))) {
          coveredBy.push(bank);
          break;
        }
      }
    }
    if (coveredBy.length) {
      console.log(`    ✅ ${catName.padEnd(42)} → ${coveredBy.join(', ')}`);
    } else {
      console.log(`    ❌ ${catName.padEnd(42)} → NO DATA — will output None`);
    }
  }

  console.log('='.repeat(70));
  console.log();
  return bankTagMap;
}

// ── Post-processing: evidence validation ───────────────────────────────────

function validateBestForEvidence(bestFor: BestForEntry[]): BestForEntry[] {
  const validated: BestForEntry[] = [];
  let rejectCount = 0;

  for (const entry of bestFor) {
    const detail = (entry.detail || '').trim();
    const bank = (entry.bank || '').trim();
    const cat = (entry.category || '').trim();

    if (bank.toLowerCase() === 'none' || bank === '' || bank.toLowerCase() === 'n/a') {
      validated.push(entry);
      continue;
    }

    const isVague = VAGUE_DETAIL_PATTERNS.some(pat => pat.test(detail));
    const hasEvidence = CONCRETE_EVIDENCE_RE.test(detail);

    if (isVague) {
      console.log(`  ⚠️  Vague-pattern flag [${cat}] "${bank}" — evidence=${hasEvidence}: "${detail.slice(0, 70)}"`);
    }

    if (!hasEvidence) {
      console.log(`  🚫 Evidence gate REJECTED [${cat}] winner "${bank}" → detail: "${detail}"`);
      validated.push({
        ...entry,
        bank: 'None',
        detail: `No verified ${cat} promotion with concrete details found`,
        is_bau: false,
      });
      rejectCount++;
    } else {
      validated.push(entry);
    }
  }

  if (rejectCount) {
    console.log(`  🚫 Evidence gate total: ${rejectCount} vague winner(s) nullified`);
  }
  return validated;
}

// ── Post-processing: stock trading winner validation ───────────────────────

function validateStockTradingWinners(bestFor: BestForEntry[]): BestForEntry[] {
  let overrides = 0;

  for (let i = 0; i < bestFor.length; i++) {
    const entry = bestFor[i];
    const cat = (entry.category || '').trim();
    const bank = (entry.bank || '').trim();

    if (!STOCK_CATS.has(cat)) continue;

    // ZA Bank path: ensure platform fee mentioned + EleBank in similar_banks
    if (ZA_NAMES.has(bank.toLowerCase())) {
      let detail = entry.detail || '';
      if (!detail.toLowerCase().includes('platform fee')) {
        bestFor[i] = {
          ...bestFor[i],
          detail: detail.replace(/[. ]+$/, '') +
            `; platform fee applies (HK: HKD ${ZA_HK_PLATFORM_FEE}/order, US: USD ${ZA_US_PLATFORM_FEE}/order minimum)`,
        };
      }
      // Ensure EleBank in similar_banks
      const similarLower = (bestFor[i].similar_banks || []).map(s => s.toLowerCase());
      if (!similarLower.some(s => ELEBANK_NAMES.has(s))) {
        bestFor[i] = {
          ...bestFor[i],
          similar_banks: ['EleBank', ...(bestFor[i].similar_banks || [])],
        };
      }
      continue;
    }

    // EleBank path
    if (ELEBANK_NAMES.has(bank.toLowerCase())) {
      if (cat === 'HK Stock Trading') {
        console.log(`  ✅ HK stock: EleBank total cost HKD ${ELEBANK_HK_TOTAL}/order — lower than ZA Bank HKD ${ZA_HK_PLATFORM_FEE}. Accepted.`);
        const similarLower = (bestFor[i].similar_banks || []).map(s => s.toLowerCase());
        let added = [...(bestFor[i].similar_banks || [])];
        if (!similarLower.some(s => ZA_NAMES.has(s))) added = ['ZA Bank', ...added];
        if (!similarLower.some(s => PADB_NAMES.has(s))) added = [...added, 'PADB'];
        bestFor[i] = { ...bestFor[i], similar_banks: added };
      } else if (cat === 'US Stock Trading' || cat === 'Stock Trading') {
        console.log(`  ✅ US stock: EleBank selected — total cost matches ZA Bank (both min USD ${ELEBANK_US_MIN_TOTAL}/order). Accepted.`);
        const similarLower = (bestFor[i].similar_banks || []).map(s => s.toLowerCase());
        let added = [...(bestFor[i].similar_banks || [])];
        if (!similarLower.some(s => ZA_NAMES.has(s))) added = ['ZA Bank', ...added];
        if (!similarLower.some(s => PADB_NAMES.has(s))) added = [...added, 'PADB'];
        bestFor[i] = { ...bestFor[i], similar_banks: added };
      }
      continue;
    }

    // Other bank that charges commission — override if ZA Bank is cheaper
    const detail = entry.detail || '';
    const chargesCommission = /\busd\s*[\d]+\.[\d]+\s*\/\s*share\b|\busd\s*[\d]+\.[\d]+\s*per\s*share\b/i.test(detail) ||
      /commission\s+(of|at|is|:)\s*[\d]/i.test(detail);

    if (!chargesCommission) continue;

    // Check if US per-share rate can be extracted
    const usdPerShareMatch = detail.match(/usd\s*([\d]+\.[\d]+)\s*(?:\/|per)\s*share/i);
    if (usdPerShareMatch && (cat === 'US Stock Trading' || cat === 'Stock Trading')) {
      const perShare = parseFloat(usdPerShareMatch[1]);
      const competitorCost200 = 200 * perShare;
      const breakeven = usBreakevenShares(perShare);

      if (competitorCost200 > ZA_US_PLATFORM_FEE) {
        console.log(
          `  🔄 US stock total-cost OVERRIDE [${cat}]: "${bank}" @ USD ${perShare}/share × 200 = USD ${competitorCost200.toFixed(2)} vs ZA Bank/EleBank USD ${ZA_US_PLATFORM_FEE.toFixed(2)}. Overriding.`,
        );
        bestFor[i] = {
          ...entry,
          bank: 'ZA Bank',
          detail: `$0 brokerage commission for US stocks via ZA Invest; platform fee USD 0.0099/share (min USD ${ZA_US_PLATFORM_FEE.toFixed(2)}/order). ${bank} charges USD ${perShare}/share commission — ZA Bank cheaper for ${Math.ceil(breakeven)}+ shares. EleBank also ties at USD ${ELEBANK_US_TOTAL_PER_SHARE}/share, min USD ${ELEBANK_US_MIN_TOTAL.toFixed(2)}/order.`,
          is_bau: true,
          similar_banks: [bank, 'EleBank'],
          why_others_lose: `${bank} charges USD ${perShare}/share commission. ZA Bank $0 commission + USD ${ZA_US_PLATFORM_FEE.toFixed(2)} min (cheaper for ${Math.ceil(breakeven)}+ shares). EleBank ties ZA Bank at USD ${ELEBANK_US_MIN_TOTAL.toFixed(2)} min.`,
        };
        overrides++;
      }
    }
  }

  if (overrides) {
    console.log(`  🔄 Stock trading total-cost override: ${overrides} winner(s) updated`);
  }
  return bestFor;
}

// ── Cross-check: fill None best_for slots from bank_analysis strengths ─────

const CATEGORY_KEYWORDS: Record<string, string[]> = {
  'HK Stock Trading': ['hk stock', 'hkex', 'commission', 'trading fee', 'platform fee', 'elebank', 'airstar', 'hk stock $0'],
  'US Stock Trading': ['us stock', 'nyse', 'nasdaq', 'commission', 'trading fee', 'platform fee', 'elebank', 'airstar', 'us stock $0'],
  'Stock Trading': ['stock', 'securities', 'brokerage', 'commission', 'trading fee', 'platform fee', 'elebank', 'airstar'],
  'Crypto Trading': ['crypto', 'bitcoin', 'virtual asset', 'digital asset', 'cryptocurrency', '0% crypto', 'fee waiver'],
  'Fund Investment': ['fund', '基金', '認購費', '轉換費', '$0認購費', 'subscription fee', 'zero subscription'],
  'Referral Bonus': ['referral', '推薦', 'invite', '多友多賞', '推薦碼', 'refer a friend', 'referral code'],
  'FX/Multi-Currency': ['fx', 'forex', 'multi-currency', 'global wallet', 'payment connect', 'remittance', 'welab global', 'fps transfer'],
  'Travel': ['trip.com', 'asia miles', 'flight', 'hotel', 'travel insurance', '旅遊', 'booking.com', 'agoda'],
  'Spending/CashBack': ['cashback', 'cash back', 'spending reward', 'rebate', 'merchant offer', 'card reward'],
  'Welcome Bonus': ['welcome', 'new customer', 'account opening', 'sign up', 'hkd8,888', 'hkd888', '迎新'],
  'Loan APR': ['loan', 'apr', 'personal loan', 'interest rate', '1.18%', 'tax season', 'tax loan'],
  'Insurance': ['insurance', '保險', 'premium rebate', 'annualized rate', '3.6%'],
  'Savings/Deposit': ['savings', 'deposit', '存款', 'interest', 'gosave', 'livisave', 'time deposit', '新資金'],
};

function crossCheckFromStrengths(
  result: StrategicInsights,
  promotionsByBank: Record<string, any[]>,
): StrategicInsights {
  const { best_for: bestFor, bank_analysis: bankAnalysis } = result;
  if (!bankAnalysis) return result;

  let filled = 0;
  for (let i = 0; i < bestFor.length; i++) {
    const entry = bestFor[i];
    const bank = (entry.bank || '').trim();
    if (bank.toLowerCase() !== 'none' && bank !== '' && bank.toLowerCase() !== 'n/a') continue;

    const cat = (entry.category || '').trim();
    let keywords = CATEGORY_KEYWORDS[cat] || [];
    if (!keywords && (cat === 'HK Stock Trading' || cat === 'US Stock Trading')) {
      keywords = CATEGORY_KEYWORDS['Stock Trading'] || [];
    }
    if (!keywords.length) continue;

    const candidates: [string, string][] = [];
    for (const [bname, bdata] of Object.entries(bankAnalysis)) {
      for (const s of (bdata.strengths || [])) {
        if (keywords.some(kw => s.toLowerCase().includes(kw.toLowerCase()))) {
          candidates.push([bname, s]);
        }
      }
    }
    if (!candidates.length) continue;

    const best = candidates.find(c => CONCRETE_EVIDENCE_RE.test(c[1])) || candidates[0];
    const [bestBank, bestDetail] = best;

    const bankPromos = promotionsByBank[bestBank] || [];
    const isBauGuess = bankPromos.some(
      p => p.is_bau && keywords.some(kw => (p.name || p.title || '').toLowerCase().includes(kw.toLowerCase())),
    );

    console.log(`  🔁 Strength cross-check FILLED [${cat}] → ${bestBank}: "${bestDetail.slice(0, 80)}"`);
    bestFor[i] = { ...entry, bank: bestBank, detail: bestDetail, is_bau: isBauGuess };
    filled++;
  }

  if (filled) {
    console.log(`  🔁 Cross-check: ${filled} slot(s) filled from bank_analysis.strengths`);
  }
  result.best_for = bestFor;
  return result;
}

// ── Main insights function ─────────────────────────────────────────────────

/**
 * Generate strategic insights across all banks.
 * Uses Agent SDK with structured output.
 * @param dbFetchFn Optional DB fetch function (Copy3 enhancement for dependency injection)
 */
export async function generateStrategicInsights(
  promotionsByBank: Record<string, any[]>,
  dbFetchFn?: DbFetchFn,
): Promise<StrategicInsights | null> {
  // Copy3: diagnostic output before processing
  diagnoseInputData(promotionsByBank);

  // Check for sparse banks and supplement from DB
  const sparseBanks = Object.entries(promotionsByBank)
    .filter(([_, promos]) => promos.length < SPARSE_THRESHOLD)
    .map(([bank]) => bank);

  if (sparseBanks.length && dbFetchFn) {
    console.log(`  ⚠️  SPARSE BANKS: ${sparseBanks} (each has < ${SPARSE_THRESHOLD} promos)`);
    promotionsByBank = supplementFromDb(promotionsByBank, dbFetchFn);
  }

  // Build bank summaries for the prompt
  const bankSummaries: string[] = [];
  for (const [bankName, promos] of Object.entries(promotionsByBank).sort(([a], [b]) => a.localeCompare(b))) {
    if (!promos.length) continue;
    const nonBauCount = promos.filter(p => !p.is_bau).length;
    const bauCount = promos.length - nonBauCount;
    const lines = buildBankSummaryLines(promos);
    bankSummaries.push(
      `## ${bankName} (${nonBauCount} time-limited promos + ${bauCount} BAU permanent features)\n` +
      lines.join('\n'),
    );
  }

  if (!bankSummaries.length) {
    console.log('⚠️  No promotions data — skipping strategic insights');
    return null;
  }

  const promotionsText = bankSummaries.join('\n\n');
  const today = hktToday();

  const prompt = `You are a Hong Kong virtual bank analyst.
Analyze these active promotions and return strategic insights as JSON.
Today's date: ${today}

${promotionsText}

SECTION 1 — BAU ITEMS
Items tagged [BAU - Permanent Feature] are ALWAYS-AVAILABLE with no expiry.
You MUST include BAU items when evaluating "best_for" category winners.
A permanent zero-fee or zero-commission feature is often the strongest competitive advantage.

SECTION 2 — DATE VALIDATION
Today is ${today}. If end_date > ${today}, the promotion is STILL ACTIVE — never treat it as expired.

SECTION 3 — CHINESE TYPE TAG → ENGLISH CATEGORY MAPPING
  [推薦] → "Referral Bonus"
  [投資] + fund/基金 → "Fund Investment"
  [投資] + hk stock/hkex → "HK Stock Trading"
  [投資] + us stock/nyse → "US Stock Trading"
  [投資] + crypto → "Crypto Trading"
  [消費] → "Spending/CashBack"
  [迎新] → "Welcome Bonus"
  [旅遊] → "Travel"
  [貸款] → "Loan APR"
  [外匯] → "FX/Multi-Currency"
  [保險] → "Insurance"
  [存款] → "Savings/Deposit"

SECTION 4 — STRICT CATEGORY DEFINITIONS
• HK Stock Trading → HKEX-listed stocks: commission + platform fee comparison
• US Stock Trading → NYSE/NASDAQ stocks: commission + platform fee comparison
• Crypto Trading → crypto/virtual asset trading fees
• Fund Investment → fund subscription or switching fee promotions
• Spending/CashBack → card cashback or merchant spending rewards
• Welcome Bonus → new customer account opening cash/gift rewards
• Travel → travel insurance, flight/hotel, Asia Miles, Trip.com
• Loan APR → personal loan with lowest specific APR
• FX/Multi-Currency → FX rate promotions, global wallet, remittance
• Referral Bonus → referral programs with HKD reward
• Insurance → insurance products with premium rebate or annualized rate
• Savings/Deposit → high-interest savings, time deposit, GoSave, liviSave

SECTION 5 — MANDATORY WINNER SELECTION
Output "None" ONLY when there is absolutely zero evidence across ALL banks for that category.

SECTION 6 — EVIDENCE GATE
The "detail" field MUST contain at least ONE concrete verifiable fact: specific HKD/USD amount, percentage, $0/zero-fee, date, product name, or fee keyword.

SECTION 7 — SIMILAR BANKS
For EVERY best_for entry populate similar_banks with ALL other banks offering similar promotions.
⚠️ For HK/US Stock Trading you MUST include BOTH "ZA Bank" and "EleBank" in similar_banks.

SECTION 8 — STOCK TRADING TOTAL COST ANALYSIS (CRITICAL)
EleBank: HK stocks $0 commission + HKD 15 platform fee = HKD 15/order (CHEAPEST)
          US stocks USD 0.0099/share total, min USD 1.99/order (ties ZA Bank)
ZA Bank:  HK stocks $0 commission + HKD 18 platform fee = HKD 18/order
          US stocks USD 0.0099/share, min USD 1.99/order
PADB:     US stocks USD 0.012/share commission + $0 platform fee

HK Stock Trading winner priority: 1st EleBank (HKD 15) → 2nd ZA Bank (HKD 18)
US Stock Trading: ZA Bank default winner when tied (simpler $0-commission structure).
❌ NEVER say "EleBank charges no platform fee" — HK stocks cost HKD 15/order.

SECTION 9 — FORBIDDEN COMPARISONS
❌ Account opening speed comparisons
❌ Generic app speed / UI claims without specific financial benefit
✅ VALID: specific fee savings in HKD/%, named products with concrete amounts

Return JSON with "best_for" array and "bank_analysis" object.`;

  console.log('  🧠 Generating strategic insights...');

  const result: AgentResult = await runAgent({
    prompt,
    outputSchema: INSIGHTS_SCHEMA,
    maxTurns: 10,
    maxBudgetUsd: 1.5,
  });

  if (!result.success) {
    console.log(`  ❌ Strategic insights agent failed: ${result.errors?.join(', ')}`);
    return null;
  }

  let insights: StrategicInsights | null = null;

  if (result.structuredOutput) {
    insights = result.structuredOutput as StrategicInsights;
  } else if (result.result) {
    try {
      const text = result.result;
      const m = text.match(/\{[\s\S]*\}/);
      if (m) insights = JSON.parse(m[0]);
    } catch {
      console.log('  ❌ Strategic insights: JSON parse failed');
      return null;
    }
  }

  if (!insights) {
    console.log('  ❌ Strategic insights: no output');
    return null;
  }

  // Post-processing pipeline
  insights.best_for = validateBestForEvidence(insights.best_for || []);
  insights.best_for = validateStockTradingWinners(insights.best_for);
  insights = crossCheckFromStrengths(insights, promotionsByBank);

  // Add counts to bank_analysis
  const nameLookup: Record<string, string> = {};
  for (const k of Object.keys(promotionsByBank)) {
    nameLookup[k.toLowerCase()] = k;
  }
  for (const bname of Object.keys(insights.bank_analysis || {})) {
    const matchedKey = nameLookup[bname.toLowerCase()];
    if (matchedKey) {
      const allP = promotionsByBank[matchedKey];
      const nonBau = allP.filter(p => !p.is_bau);
      insights.bank_analysis[bname].count = nonBau.length;
      insights.bank_analysis[bname].bau_count = allP.length - nonBau.length;
    } else {
      insights.bank_analysis[bname].count = 0;
      insights.bank_analysis[bname].bau_count = 0;
    }
  }

  const bauWins = (insights.best_for || []).filter(b => b.is_bau).length;
  const noneWins = (insights.best_for || []).filter(
    b => (b.bank || '').toLowerCase() === 'none' || b.bank === '' || (b.bank || '').toLowerCase() === 'n/a',
  ).length;

  if (noneWins) {
    const noneCats = (insights.best_for || [])
      .filter(b => (b.bank || '').toLowerCase() === 'none' || b.bank === '' || (b.bank || '').toLowerCase() === 'n/a')
      .map(b => b.category);
    console.log(`  ⚠️  ${noneWins} best_for slot(s) still None: ${noneCats}`);
  }

  console.log(`✅ Strategic insights generated (${bauWins} BAU winner(s), ${noneWins} None slot(s))`);
  return insights;
}
