/**
 * Main pipeline — 9-step orchestration for VBank Tracker Agent.
 * Ported from Python vbank-tracker scripts/main.py.
 * Enhanced with Copy3's AI-unavailability handling, pre-run DB recovery,
 * and legacy alias-aware db_fetch_fn.
 * @author Alfie
 */

import fs from 'fs';
import path from 'path';
import { config as dotenvConfig } from 'dotenv';

dotenvConfig({ path: path.join(process.cwd(), '.env') });

import { BANK_CONFIGS } from './config/banks';
import {
  initDb,
  startNewRun,
  savePromotions,
  saveProducts,
  markStaleAsInactive,
  markStaleProductsInactive,
  markInactiveOld,
  reactivatePromotionsSeenOn,
  reactivateMostRecentlySeen,
  repairReinsertedPromotions,
  getActivePromosForBank,
  getActivePromotions,
  getActiveProducts,
  getProductsByBank,
  getNewPromotionsToday,
  getNewPromotionsLastNDays,
  getNewProductsToday,
  getNewProductsLastNDays,
  hasBaselineScan,
  recordScan,
  getDbStats,
  generateDailyReport,
  exportToJson,
  getPromotionsByBankName,
  closeDb,
} from './db/database';
import { warmup, isAgentUnavailable } from './agent/client';
import { scrapeBank, type ScrapedBank, type RawPromotion } from './agent/scraper';
import { extractBankProducts, type ExtractedProducts } from './agent/products';
import { aiDedupTitles, applyDedup } from './agent/dedup';
import { aiDedupProducts, applyProductDedup, aiMatchProductsAgainstExisting, type DbProduct } from './agent/product_dedup';
import { aiMatchAgainstExisting, type DbPromotion } from './agent/matcher';
import { generateStrategicInsights } from './agent/insights';
import { generateProductComparisons, type ProductsByBank } from './agent/product_comparison';
import { buildHtmlEmail, sendEmail, saveHtmlFallback } from './email/emailer';
import { hktToday, hktNow } from './utils/hkt';
import { canonicalBankName, BANK_NAME_LEGACY_ALIASES } from './utils/filters';

// ── Paths ────────────────────────────────────────────────────────────────────

const DATA_JSON_PATH = path.join(process.cwd(), 'docs', 'data.json');
const HTML_PREVIEW_PATH = path.join(process.cwd(), 'output', 'email_preview.html');

// ── CLI flags ────────────────────────────────────────────────────────────────

const NO_EMAIL = process.argv.includes('--no-email') || process.argv.includes('--dry-run');
const SKIP_SCRAPE = process.argv.includes('--skip-scrape');

// ── DB fetch with legacy alias fallback ──────────────────────────────────────

function makeDbFetchFn(): (bankName: string) => Record<string, any>[] {
  return (bankName: string): Record<string, any>[] => {
    let rows = getPromotionsByBankName(bankName);
    if (rows.length) return rows;
    for (const alias of BANK_NAME_LEGACY_ALIASES[bankName] || []) {
      rows = getPromotionsByBankName(alias);
      if (rows.length) {
        console.log(
          `  ℹ️  db_fetch_fn: "${bankName}" returned 0 rows; ` +
          `fell back to legacy alias "${alias}" (${rows.length} rows)`,
        );
        return rows;
      }
    }
    return [];
  };
}

// ── Env helpers ──────────────────────────────────────────────────────────────

function readEnv(): { addr: string; pwd: string; to: string[] } {
  const addr = (process.env.GMAIL_ADDRESS || '').trim();
  const pwd = (process.env.GMAIL_APP_PASSWORD || '').trim();
  const raw = (
    process.env.RECIPIENT_EMAIL ||
    process.env.EMAIL_RECIPIENT ||
    process.env.EMAIL_TO || ''
  ).trim();
  const to = raw.split(',').map(e => e.trim()).filter(Boolean);
  return { addr, pwd, to };
}

function printEnvCheck(addr: string, pwd: string, to: string[]): void {
  const toDisplay = to.join(', ') || '';
  console.log('  Env check:');
  console.log(`    GMAIL_ADDRESS     : ${addr ? '✅ set' : '❌ MISSING'}`);
  console.log(`    GMAIL_APP_PASSWORD: ${pwd ? '✅ set (hidden)' : '❌ MISSING'}`);
  console.log(`    RECIPIENT_EMAIL   : ${to.length ? '✅ ' + toDisplay : '❌ MISSING'}`);
  if (NO_EMAIL) {
    console.log('    📴 --no-email flag — SMTP step will be skipped');
  }
}

// ── data.json helpers ────────────────────────────────────────────────────────

function patchDataJson(filePath: string, extra: Record<string, any>): void {
  const keys = Object.keys(extra).join(', ');
  try {
    const jdata = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    Object.assign(jdata, extra);
    fs.writeFileSync(filePath, JSON.stringify(jdata, null, 2), 'utf-8');
    console.log(`  ✅ data.json patched with key(s): ${keys}`);
  } catch (exc: any) {
    console.log(`  ⚠️  data.json patch failed (${keys}): ${exc.message}`);
  }
}

function loadDataJson(filePath: string): Record<string, any> | null {
  try {
    const content = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    const n = (content.promotions || []).length;
    console.log(`  ✅ data.json loaded for email stats (${n} promotions in file)`);
    return content;
  } catch (exc: any) {
    if (exc.code === 'ENOENT') {
      console.log(`  ⚠️  data.json not found at ${filePath} — email will use DB rows for stats`);
    } else {
      console.log(`  ⚠️  data.json load failed: ${exc.message} — email will use DB rows for stats`);
    }
    return null;
  }
}

// ── Main pipeline ────────────────────────────────────────────────────────────

async function main(): Promise<number> {
  const tStart = Date.now();
  const today = hktToday();

  console.log(`\n${'═'.repeat(60)}`);
  console.log(`  HK Virtual Bank Promotions Tracker  |  ${today}`);
  if (NO_EMAIL) console.log('  MODE: --no-email  (pipeline runs; SMTP skipped)');
  if (SKIP_SCRAPE) console.log('  MODE: --skip-scrape  (re-processing DB data only)');
  console.log(`${'═'.repeat(60)}\n`);

  const { addr, pwd, to } = readEnv();
  printEnvCheck(addr, pwd, to);

  // ── Step 1: Database ──────────────────────────────────────────
  console.log('\nStep 1 ── Init database');
  let currentRunId: number;
  try {
    initDb();
    currentRunId = startNewRun(Object.keys(BANK_CONFIGS));
  } catch (exc: any) {
    console.log(`  ❌ Database init failed — cannot continue: ${exc.message}`);
    return 1;
  }

  // ── Step 2: Warm up Agent SDK + check availability ────────────
  console.log('\nStep 2 ── Warm up Agent SDK');
  let aiOk = true;
  try {
    await warmup();
    if (isAgentUnavailable()) {
      aiOk = false;
      console.log('  ⚠️  Agent SDK unavailable — AI features will be skipped');
    }
  } catch (exc: any) {
    aiOk = false;
    console.log(`  ⚠️  Agent warmup failed: ${exc.message} — AI features will be skipped`);
  }

  // ── Step 2b: Pre-run DB recovery (only when AI unavailable) ───
  if (!aiOk) {
    console.log('\nStep 2b ── Pre-run DB recovery (AI unavailable)');
    const preStats = getDbStats();
    const preTotal = preStats.total_promotions || 0;
    const preActive = preStats.active_promotions || 0;

    if (preTotal > 0 && preActive === 0) {
      console.log(
        `  🚨 AI unavailable + 0 active promotions (DB has ${preTotal} total) → attempting DB recovery`,
      );
      const recovered = reactivateMostRecentlySeen(7);
      if (recovered) {
        const post = getDbStats();
        console.log(
          `  ✅ Recovery succeeded: ${recovered} promotions restored ` +
          `(${post.active_promotions || 0} active, ${post.bau_promotions || 0} BAU)`,
        );
      } else {
        console.log('  ⚠️  Recovery found nothing to restore — DB may be truly empty');
      }
    } else if (preTotal > 0 && preActive > 0) {
      console.log(
        `\n  ℹ️  AI unavailable — using existing ${preActive} active promotions from DB for email/website`,
      );
    } else {
      console.log('\n  ⚠️  AI unavailable and DB is completely empty');
    }
  }

  // ── Step 3: Scrape + Extract ──────────────────────────────────
  console.log(`\nStep 3 ── Scrape all ${Object.keys(BANK_CONFIGS).length} banks`);
  const t3 = Date.now();

  let scrapedBanks: ScrapedBank[];

  if (SKIP_SCRAPE || !aiOk) {
    if (!aiOk) {
      console.log('  ⚠️  AI unavailable — skipping scrape entirely');
    }
    if (SKIP_SCRAPE) {
      console.log('  ⏭  --skip-scrape: using existing DB data only');
    }
    scrapedBanks = Object.values(BANK_CONFIGS).map(cfg => ({
      bankId: cfg.id,
      bankName: cfg.name,
      promotions: [],
    }));
  } else {
    console.log('  🚀 Scraping all banks in parallel...');
    const configs = Object.values(BANK_CONFIGS);
    const settled = await Promise.allSettled(configs.map(cfg => scrapeBank(cfg)));
    scrapedBanks = settled.map((outcome, i) => {
      if (outcome.status === 'fulfilled') {
        return outcome.value;
      }
      console.log(`  ❌ Error scraping ${configs[i].name}: ${outcome.reason?.message || outcome.reason}`);
      return { bankId: configs[i].id, bankName: configs[i].name, promotions: [] };
    });
  }

  console.log(`  ⏱  Scrape completed in ${((Date.now() - t3) / 1000).toFixed(1)}s`);

  if (!scrapedBanks.length) {
    console.log('  ❌ No data scraped — abort');
    closeDb();
    return 1;
  }

  const bankIdsOk = scrapedBanks.filter(r => r.promotions.length > 0).map(r => r.bankId);

  // ── Step 3b: Extract products from cached pages ──────────────
  console.log('\nStep 3b ── Extract products from scraped pages');

  let extractedProductsList: ExtractedProducts[] = [];

  if (!aiOk) {
    console.log('  ⚠️  AI unavailable — skipping product extraction');
  } else {
    const configs = Object.values(BANK_CONFIGS);
    for (const bank of scrapedBanks) {
      const cfg = configs.find(c => c.id === bank.bankId);
      if (!cfg || !bank.pages?.length) {
        console.log(`  ⚠️  [${bank.bankId.toUpperCase()}] No cached pages — skipping product extraction`);
        extractedProductsList.push({ bankId: bank.bankId, bankName: bank.bankName, products: [] });
        continue;
      }

      try {
        const result = await extractBankProducts(bank.pages, cfg);
        extractedProductsList.push(result);
      } catch (exc: any) {
        console.log(`  ❌ Product extraction error for ${bank.bankName}: ${exc.message}`);
        extractedProductsList.push({ bankId: bank.bankId, bankName: bank.bankName, products: [] });
      }
    }
  }

  // ── Step 4: Dedup + Match + Save ──────────────────────────────
  console.log('\nStep 4 ── Within-batch dedup + Match against DB + Save to DB');

  let totalExtracted = 0;
  let totalNew = 0;
  let totalUpdated = 0;
  let totalDeduped = 0;
  let totalDbMatched = 0;
  const banksAiSaved: string[] = [];

  for (const bank of scrapedBanks) {
    const { bankId, bankName } = bank;
    let promos = bank.promotions;
    const defaultUrl = BANK_CONFIGS[bankId]?.link || '';
    const mark = promos.length ? '✅' : '⚠️';
    console.log(`\n  [${bankId.toUpperCase()}] ${bankName}  ${mark}  (${promos.length} promotions)`);

    if (!promos.length) {
      if (!aiOk) {
        console.log(`    ⚠️  AI unavailable — skip`);
      } else {
        console.log(`    ⚠️  No promotions extracted for ${bankName} — skip`);
      }
      continue;
    }

    if (!aiOk) {
      console.log(`    ⚠️  AI unavailable — skip dedup/match/save for ${bankName}`);
      continue;
    }

    // 4a: Within-batch dedup
    try {
      const dupMap = await aiDedupTitles(promos, bankName);
      if (dupMap.size) {
        const before = promos.length;
        promos = applyDedup(promos, dupMap);
        const removed = before - promos.length;
        totalDeduped += removed;
        console.log(
          `    🤖 Within-batch dedup: ${removed} removed ` +
          `(${before} → ${promos.length}) for ${bankName}`,
        );
      }
    } catch (exc: any) {
      console.log(`    ⚠️  Within-batch dedup error for ${bankName}: ${exc.message}`);
    }

    if (!promos.length) {
      console.log(`    ⚠️  0 promotions after within-batch dedup for ${bankName}`);
      continue;
    }

    // 4b: Match against existing DB records
    try {
      const existingDb = getActivePromosForBank(bankId);
      if (existingDb.length) {
        const matchMap = await aiMatchAgainstExisting(promos, existingDb as DbPromotion[], bankName);
        for (const [idx, dbId] of matchMap.entries()) {
          if (idx >= 0 && idx < promos.length) {
            (promos[idx] as any)._matched_id = dbId;
          }
        }
        totalDbMatched += matchMap.size;
      } else {
        console.log(`    ℹ️  No existing DB records for ${bankName} — all will be new`);
      }
    } catch (exc: any) {
      console.log(`    ⚠️  DB-match error for ${bankName}: ${exc.message} — formula pass only`);
    }

    // 4c: Save to DB
    totalExtracted += promos.length;
    try {
      const dbResult = savePromotions(
        bankId, bankName, promos,
        currentRunId, today,
      );
      banksAiSaved.push(bankId);
      totalNew += dbResult.new;
      totalUpdated += dbResult.updated;
      console.log(
        `    ✅ ${dbResult.new} new, ${dbResult.updated} updated, ` +
        `${dbResult.skipped} skipped — ${bankName}`,
      );
    } catch (exc: any) {
      console.log(`    ❌ save_promotions error for ${bankName}: ${exc.message}`);
    }
  }

  console.log(
    `\n📊 Extracted:${totalExtracted}  New:${totalNew}  Updated:${totalUpdated}  ` +
    `Deduped:${totalDeduped}  DB-matched:${totalDbMatched}`,
  );

  // ── Step 4b: Product dedup + match + save ─────────────────────
  console.log('\nStep 4b ── Product dedup + match + save to DB');

  let totalProductsExtracted = 0;
  let totalProductsNew = 0;
  let totalProductsUpdated = 0;
  const banksProductSaved: string[] = [];

  for (const ep of extractedProductsList) {
    const { bankId, bankName, products: rawProducts } = ep;
    let prods = rawProducts;
    const mark = prods.length ? '✅' : '⚠️';
    console.log(`\n  [${bankId.toUpperCase()}] ${bankName}  ${mark}  (${prods.length} products)`);

    if (!prods.length) {
      if (!aiOk) {
        console.log(`    ⚠️  AI unavailable — skip`);
      }
      continue;
    }

    // 4b-a: Within-batch dedup
    try {
      const dupMap = await aiDedupProducts(prods, bankName);
      if (dupMap.size) {
        const before = prods.length;
        prods = applyProductDedup(prods, dupMap);
        const removed = before - prods.length;
        console.log(`    🤖 Within-batch product dedup: ${removed} removed (${before} → ${prods.length})`);
      }
    } catch (exc: any) {
      console.log(`    ⚠️  Product dedup error for ${bankName}: ${exc.message}`);
    }

    if (!prods.length) continue;

    // 4b-b: Match against existing DB records
    try {
      const existingDb = getProductsByBank(bankId);
      if (existingDb.length) {
        const matchMap = await aiMatchProductsAgainstExisting(prods, existingDb as DbProduct[], bankName);
        for (const [idx, dbId] of matchMap.entries()) {
          if (idx >= 0 && idx < prods.length) {
            (prods[idx] as any)._matched_id = dbId;
          }
        }
      }
    } catch (exc: any) {
      console.log(`    ⚠️  Product DB-match error for ${bankName}: ${exc.message}`);
    }

    // 4b-c: Save to DB
    totalProductsExtracted += prods.length;
    try {
      const dbResult = saveProducts(bankId, bankName, prods as any[], today);
      banksProductSaved.push(bankId);
      totalProductsNew += dbResult.new;
      totalProductsUpdated += dbResult.updated;
      console.log(`    ✅ Products: ${dbResult.new} new, ${dbResult.updated} updated — ${bankName}`);
    } catch (exc: any) {
      console.log(`    ❌ saveProducts error for ${bankName}: ${exc.message}`);
    }

    // Record baseline or daily scan
    try {
      const isBaseline = !hasBaselineScan(bankId);
      recordScan(bankId, isBaseline ? 'baseline' : 'daily', prods.length);
    } catch (exc: any) {
      console.log(`    ⚠️  recordScan error for ${bankName}: ${exc.message}`);
    }
  }

  console.log(
    `\n📊 Products: Extracted:${totalProductsExtracted}  New:${totalProductsNew}  Updated:${totalProductsUpdated}`,
  );

  // ── Step 5: Mark stale / old inactive ────────────────────────
  console.log('\nStep 5 ── Mark stale / old promos inactive');

  if (!aiOk) {
    console.log(
      '  ⚠️  AI unavailable — skipping mark_stale_as_inactive and ' +
      'mark_inactive_old to preserve existing data',
    );
  } else if (!banksAiSaved.length) {
    console.log(
      '  ⚠️  No banks were successfully saved this run — ' +
      'skipping mark_stale_as_inactive to avoid false-expiry',
    );
  } else {
    markStaleAsInactive(banksAiSaved, today);
    markInactiveOld(90);
  }

  // ── Step 5b: Post-staleness sanity check ─────────────────────
  const activeAfterStale = getActivePromotions(true);
  if (!activeAfterStale.length && banksAiSaved.length) {
    console.log(
      `  🚨 CRITICAL: 0 active promotions after mark_stale_as_inactive! ` +
      `Triggering date-skew recovery for RUN_DATE=${today}`,
    );
    const recovered = reactivatePromotionsSeenOn(today);
    if (!recovered) {
      console.log('  ❌ Recovery found nothing — attempting broad recovery');
      reactivateMostRecentlySeen(7);
    }
  } else if (!activeAfterStale.length && !banksAiSaved.length) {
    console.log('  ⚠️  Still 0 active promotions after recovery attempt');
  }

  // ── Step 5c: Repair re-inserted promotions ────────────────────
  console.log('\nStep 5c ── Repair re-inserted promotions');
  if (aiOk && banksAiSaved.length) {
    try {
      repairReinsertedPromotions(false);
    } catch (exc: any) {
      console.log(`  ⚠️  repair_reinserted_promotions error: ${exc.message} — continuing`);
    }
  } else if (!aiOk) {
    console.log('  ⏭  Skipping repair — AI unavailable this run (no new insertions possible)');
  } else {
    console.log('  ⏭  Skipping repair — no new insertions possible this run');
  }

  // ── Step 5d: Mark stale products inactive ──────────────────────
  console.log('\nStep 5d ── Mark stale products inactive');
  if (!aiOk) {
    console.log('  ⚠️  AI unavailable — skipping product staleness check');
  } else if (!banksProductSaved.length) {
    console.log('  ⚠️  No products saved this run — skipping staleness check');
  } else {
    markStaleProductsInactive(banksProductSaved, today);
  }

  // ── Step 6: Export data.json for website ─────────────────────
  console.log('\nStep 6 ── Export data.json for website');

  const activeForExport = getActivePromotions(true);
  if (!activeForExport.length) {
    console.log(
      '  ⚠️  Skipping data.json export — 0 active promotions in DB ' +
      '(preserving existing file)',
    );
  } else {
    exportToJson(DATA_JSON_PATH);

    const runTs = hktNow();
    const extraPatch: Record<string, any> = {
      updated: runTs,
      last_updated: runTs,
      ai_unavailable: !aiOk,
    };
    patchDataJson(DATA_JSON_PATH, extraPatch);
  }

  // ── Step 6b: Load data.json as canonical count source ────────
  console.log('\nStep 6b ── Load data.json for email count source');
  let dataJsonContent = loadDataJson(DATA_JSON_PATH);

  // ── Step 7: Strategic insights ────────────────────────────────
  console.log('\nStep 7 ── Generate AI strategic insights');
  const allActiveWithBau = getActivePromotions(true);
  const bauCountInsights = allActiveWithBau.filter(p => p.is_bau).length;

  console.log(
    `  📊 Insights input: ${allActiveWithBau.length} promos ` +
    `(${bauCountInsights} BAU + ${allActiveWithBau.length - bauCountInsights} time-limited)`,
  );

  // Build promos_by_name with canonical name normalisation
  const promosByName: Record<string, any[]> = {};
  for (const p of allActiveWithBau) {
    const bname = canonicalBankName(p.bank_name || p.bName || p.bank || 'Unknown');
    (promosByName[bname] ??= []).push(p);
  }

  // Diagnostic: warn if any legacy names slipped through
  const legacyNames = new Set(['Airstar Bank', 'PAObank', 'PAO Bank', 'PAOB']);
  const foundLegacy = new Set([...legacyNames].filter(n => promosByName[n]));
  if (foundLegacy.size) {
    console.log(
      `  ⚠️  Legacy bank name(s) still in promos_by_name after normalisation: ` +
      `${[...foundLegacy].join(', ')} — run migration to fix DB rows`,
    );
  }

  const allPromosEmail = allActiveWithBau.filter(p => !p.is_bau);
  const newPromosEmail = getNewPromotionsToday(false);
  const newPromosWeekRaw = getNewPromotionsLastNDays(6, false);
  const newPromosWeekEmail = newPromosWeekRaw.filter(p => !p.is_bau);

  console.log(`  [INFO] Non-BAU new (today):        ${newPromosEmail.length}`);
  console.log(`  [INFO] Non-BAU new (past 6 days):  ${newPromosWeekEmail.length}`);
  console.log(`  [INFO] Non-BAU active (all):       ${allPromosEmail.length}`);
  console.log(`  [INFO] BAU (insights input):       ${bauCountInsights}`);
  console.log(`  [INFO] Banks in promos_by_name:    ${Object.keys(promosByName).sort().join(', ')}`);

  let strategicInsights: Record<string, any> | null = null;
  if (aiOk && Object.keys(promosByName).length) {
    try {
      strategicInsights = await generateStrategicInsights(
        promosByName,
        makeDbFetchFn(),
      );
    } catch (exc: any) {
      console.log(`  ⚠️  Insights error: ${exc.message}`);
    }
  } else if (!aiOk) {
    console.log('  ⚠️  AI unavailable — skipping strategic insights');
  }

  if (strategicInsights) {
    patchDataJson(DATA_JSON_PATH, { strategic_insights: strategicInsights });
  } else {
    patchDataJson(DATA_JSON_PATH, { strategic_insights: null });
    console.log('  ⚠️  Insights unavailable — continuing without it');
  }

  // Step 7b: Reload data.json after insights patch
  if (strategicInsights) {
    console.log('\nStep 7b ── Reload data.json after insights patch');
    const reloaded = loadDataJson(DATA_JSON_PATH);
    if (reloaded) dataJsonContent = reloaded;
  }

  // ── Step 7c: Generate product comparisons (ZA baseline) ────────
  console.log('\nStep 7c ── Generate ZA Bank product comparisons');

  const allActiveProducts = getActiveProducts(true);
  const productsByBank: ProductsByBank = {};

  for (const prod of allActiveProducts) {
    const bid = prod.bank_id || 'unknown';
    if (!productsByBank[bid]) {
      productsByBank[bid] = {
        bankName: prod.bank_name || 'Unknown',
        products: [],
      };
    }
    productsByBank[bid].products.push(prod);
  }

  let productComparisons: Map<string, any> | null = null;
  if (!aiOk) {
    console.log('  ⚠️  AI unavailable — skipping product comparisons');
  } else if (!productsByBank['za']?.products.length) {
    console.log('  ⚠️  No ZA Bank products found — skipping product comparisons');
  } else {
    try {
      productComparisons = await generateProductComparisons(productsByBank);
      if (productComparisons && productComparisons.size) {
        // Update products in DB with comparison data
        const comparisonObj = Object.fromEntries(productComparisons);
        patchDataJson(DATA_JSON_PATH, { product_comparisons: comparisonObj });
        console.log(`  ✅ Generated ${productComparisons.size} product comparisons`);
      }
    } catch (exc: any) {
      console.log(`  ⚠️  Product comparison error: ${exc.message}`);
    }
  }

  // ── Step 8: Build & send email ────────────────────────────────
  console.log('\nStep 8 ── Build & send email');

  // Product stats for email
  const newProductsEmail = getNewProductsToday(false);
  const newProductsWeekEmail = getNewProductsLastNDays(6, false);
  const allProductsEmail = getActiveProducts(false);

  const html = buildHtmlEmail(
    allPromosEmail,
    dataJsonContent,
    strategicInsights,
    newPromosEmail,
    newPromosWeekEmail,
    !aiOk,
    // Product data for email restructuring (Step 2.9)
    newProductsEmail,
    newProductsWeekEmail,
    allProductsEmail,
  );
  console.log('  ✅ HTML email built');

  saveHtmlFallback(html, HTML_PREVIEW_PATH);

  const smtpReady = addr && pwd && to.length;

  const emailSubject = !aiOk
    ? `🏦 VBank Daily Report — ${hktNow().split(' ')[0]} [Cached Data — AI Unavailable]`
    : `🏦 VBank Daily Report — ${hktNow().split(' ')[0]}`;

  if (NO_EMAIL) {
    console.log('  📴 Email skipped (--no-email)');
    console.log(`  📄 HTML preview → ${HTML_PREVIEW_PATH}`);
  } else if (!smtpReady) {
    const missing = [
      !addr ? 'GMAIL_ADDRESS' : '',
      !pwd ? 'GMAIL_APP_PASSWORD' : '',
      !to.length ? 'RECIPIENT_EMAIL' : '',
    ].filter(Boolean);
    console.log(`  ❌ Missing ${missing.join(' / ')} — email skipped`);
    console.log(`  📄 HTML preview → ${HTML_PREVIEW_PATH}`);
  } else {
    try {
      const success = await sendEmail(
        html,
        emailSubject,
        to,
        newPromosEmail,
        newPromosWeekEmail,
        allPromosEmail,
        !aiOk,
        dataJsonContent,
        // Product data for email restructuring (Step 2.9)
        newProductsEmail,
        newProductsWeekEmail,
        allProductsEmail,
      );
      if (success) {
        console.log(`  ✅ Email sent → ${to.join(', ')}`);
      } else {
        console.log('  ❌ send_email() returned False');
        console.log(`  📄 HTML preview → ${HTML_PREVIEW_PATH}`);
      }
    } catch (exc: any) {
      console.log(`  ❌ Email failed: ${exc.message}`);
      console.log(`  📄 HTML preview → ${HTML_PREVIEW_PATH}`);
    }
  }

  // ── Done ──────────────────────────────────────────────────────
  const elapsed = ((Date.now() - tStart) / 1000).toFixed(1);
  const dbStats = getDbStats();

  const summaryPromos = dataJsonContent?.promotions || allPromosEmail;
  const summaryNonBau = (summaryPromos as any[]).filter((p: any) => !p.is_bau);
  const summaryTotal = summaryNonBau.filter((p: any) =>
    p.active !== false && (!p.end_date || String(p.end_date).slice(0, 10) >= today),
  ).length;

  const report = generateDailyReport(currentRunId);
  const expiredCount = report.summary?.expired_count || 0;

  console.log(`\n${'═'.repeat(60)}`);
  console.log(
    `  Done in ${elapsed}s  |  ` +
    `🆕 ${newPromosEmail.length} new promos today  |  ` +
    `📅 ${newPromosWeekEmail.length} new promos this week  |  ` +
    `✅ ${allPromosEmail.length} active promos (DB)  |  ` +
    `📦 ${allProductsEmail.length} products (DB)  |  ` +
    `📄 ${summaryTotal} active (data.json)  |  ` +
    `❌ ${expiredCount} expired  |  ` +
    `🤖 deduped:${totalDeduped} matched:${totalDbMatched}  |  ` +
    `⚙️  ${bauCountInsights} BAU  |  ` +
    `📦 DB:${dbStats.total_promotions || '?'} promos total` +
    (!aiOk ? '  |  ⚠️ AI UNAVAILABLE' : ''),
  );
  console.log(`${'═'.repeat(60)}\n`);

  closeDb();
  return 0;
}

// ── Entry point ──────────────────────────────────────────────────────────────

main().then(code => {
  process.exit(code);
}).catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
