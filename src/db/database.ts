/**
 * SQLite database layer using better-sqlite3 (synchronous).
 * Ported from Python vbank-tracker database.py with Copy3 enhancements:
 *   - _rebuildPromotionsTable() for legacy schema migration
 *   - isNonBankContent() checks description and tc_link
 *   - Better first_seen_at handling
 * @author Alfie
 */

import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { hktToday, hktNDaysAgo } from '../utils/hkt.js';
import { isNonBankContent } from '../utils/filters.js';

const DB_PATH = path.join(process.cwd(), 'data', 'promotions.db');

let _db: Database.Database | null = null;

function getDb(): Database.Database {
  if (!_db) {
    throw new Error('Database not initialized. Call initDb() first.');
  }
  return _db;
}

// ── Dedup constants ────────────────────────────────────────────────────────

const JACCARD_THRESHOLD = 0.50;
const JACCARD_THRESHOLD_INACTIVE = 0.42;
const LCP_THRESHOLD = 0.72;
const LCP_THRESHOLD_INACTIVE = 0.65;
const MIN_NORM_LEN = 10;
const MIN_TOKENS = 2;

const RE_INSTALMENT = /installment/gi;
const RE_AMOUNT = /(?:hkd|usd|rmb|sgd|cny)\s*[\d,]+(?:\.\d+)?/gi;
const RE_PCT = /\d+(?:\.\d+)?\s*%/g;
const RE_NONALNUM = /[\s\W]+/g;

const PROMO_CODE_SKIP = new Set([
  'SWIFT', 'VISA', 'FPS', 'ATM', 'HKD', 'USD', 'APR', 'ETF', 'IPO',
  'HKT', 'CSL', 'USA', 'UFO', 'VIP', 'APP', 'SMS', 'PIN', 'QR',
  'MOX', 'ZA', 'ANT', 'PAO', 'PADB', 'LIVI', 'AIRSTAR', 'ELEBANK',
  'FUSION', 'WELAB',
]);

interface SynonymPattern {
  regex: RegExp;
  replacement: string;
}

const SYNONYM_PATTERNS: SynonymPattern[] = [
  { regex: /余[額额]\+/g, replacement: 'depositplus' },
  { regex: /\bdeposit[- ]?plus\b/gi, replacement: 'depositplus' },
  { regex: /balance\+/gi, replacement: 'depositplus' },
  { regex: /\b(?:daily|high|boosted?|tiered?)[- ]?interest[- ]?(?:saving\w*|earn\w*|account\w*)?\b/gi, replacement: 'hisavings' },
  { regex: /(?:(?:zero|0)[- ]?(?:brokerage|commission)[- ]?fees?|commission[- ]?free[- ]?(?:stock|trading)?|no[- ]?brokerage|lifetime[- ]?\$0[- ]?commission|\$0[- ]?commission)/gi, replacement: 'zerobrokfee' },
  { regex: /\bswift\b/gi, replacement: 'swifttransfer' },
  { regex: /\bpayment[- ]?connect\b/gi, replacement: 'swifttransfer' },
  { regex: /(?:zero[- ]?(?:subscription[- ]?)?fees?[- ]?(?:on[- ]?(?:all[- ]?)?)?funds?|featured?[- ]?funds?[- ]?with[- ]?zero[- ]?(?:subscription[- ]?)?fees?|zero[- ]?fees?[- ]?fund[- ]?(?:subscription|switching)?|zero[- ]?(?:fee|cost)[- ]?(?:investment[- ]?)?fund)/gi, replacement: 'zerosubfee' },
  { regex: /\bcrypto(?:currency|currencies|currenc\w*)?\b/gi, replacement: 'crypto' },
  { regex: /\bdigital[- ]?assets?\b/gi, replacement: 'crypto' },
  { regex: /\bvirtual[- ]?assets?\b/gi, replacement: 'crypto' },
  { regex: /\b(?:fx|foreign[- ]?exchange|currency[- ]?exchange|forex)\b/gi, replacement: 'fxexchange' },
  { regex: /\bglobal[- ]?(?:remittance|wallet)\b/gi, replacement: 'fxexchange' },
  { regex: /\bwelab[- ]?global[- ]?wallet\b/gi, replacement: 'welabwallet' },
  { regex: /\btime[- ]?deposit\b/gi, replacement: 'timedeposit' },
  { regex: /\bpowerdraw\b/gi, replacement: 'powerdraw' },
  { regex: /insurance[- ]?(?:product\w*)?[- ]?(?:with[- ]?)?(?:annual[- ]?rate|premium[- ]?rebate|\d+(?:\.\d+)?%)/gi, replacement: 'insurance_rate' },
  { regex: /\d+(?:\.\d+)?%[- ]?(?:annuali[sz]ed|annual)[- ]?rate/gi, replacement: 'insurance_rate' },
  { regex: /\bpayroll[- ]?(?:switch(?:ing)?|deposit|benefit\w*)?\b/gi, replacement: 'payroll' },
  { regex: /\b(?:(?:quick|fast|instant|mobile|online|digital)[- ]?)?account[- ]?open(?:ing)?\b/gi, replacement: 'accountopen' },
  { regex: /\b24[/×x]7\b/g, replacement: 'banking247' },
  { regex: /\basia[- ]?miles?\b/gi, replacement: 'asiamiles' },
  { regex: /\bmiles?[- ]?(?:reward|earn|redeem)\w*\b/gi, replacement: 'milesreward' },
  { regex: /\bgosave\b/gi, replacement: 'gosave' },
  { regex: /\blivisave\b/gi, replacement: 'livisave' },
  { regex: /\btrip\.com\b/gi, replacement: 'tripcom' },
  { regex: /\bxiaomi\b/gi, replacement: 'xiaomi' },
  { regex: /\bsamsung[- ]?s\d+\b/gi, replacement: 'samsung_phone' },
  { regex: /\bbest[- ]?in[- ]?town\b/gi, replacement: 'bestintown' },
  { regex: /\bdevice[- ]?plans?\b/gi, replacement: 'deviceplan' },
  { regex: /\bintegrated[- ]?investment\b/gi, replacement: 'intinvest' },
  { regex: /\bone[- ]?stop[- ]?(?:trading|investment)[- ]?platform\b/gi, replacement: 'onestoplatform' },
  { regex: /\bant[- ]?bank[- ]?investment[- ]?fund[- ]?platform\b/gi, replacement: 'antfundplatform' },
  { regex: /\bpersonal[- ]?(?:revolving[- ]?)?loan\b/gi, replacement: 'personalloan' },
  { regex: /(?:zero[- ]?(?:\w+[- ]?)?fees?|0\s*%[- ]?(?:\w+[- ]?)?fees?|fee[- ]?(?:free|waiver|waived)|no[- ]?(?:\w+[- ]?)?fees?)/gi, replacement: 'zfee' },
  { regex: /\b(?:quick|fast|instant|rapid|express|immediate)\b/gi, replacement: 'fastspd' },
  { regex: /\b(?:flexible|custom(?:iz\w*)?|select(?:able|ion)?|personali[sz]\w*)\b/gi, replacement: 'flexcust' },
  { regex: /\b(?:welcome|sign[- ]?up|new[- ]?customer|new[- ]?user)[- ]?(?:bonus|reward|offer|gift)?\b/gi, replacement: 'welcome' },
  { regex: /\b(?:refer(?:ral)?[- ]?(?:bonus|reward|program)?|invite[- ]?friend\w*|friend[- ]?refer\w*)\b/gi, replacement: 'referral' },
  { regex: /\b(?:cash[- ]?back|cash[- ]?rebate)\b/gi, replacement: 'cashback' },
];

const NOISE_WORDS = [
  'rebateprogram', 'rewardprogram', 'program',
  'promotion', 'campaign', 'offer', 'bonus',
  'reward', 'scheme', 'deal', 'activity',
  'rebate', 'exclusive', 'special',
  'with', 'from', 'for', 'and', 'the', 'your',
];

const NOISE_PATTERNS: RegExp[] = NOISE_WORDS.map(w =>
  new RegExp(`(?<![a-z0-9])${w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?![a-z0-9])`, 'gi')
);

const JACCARD_STOPWORDS = new Set([
  'for', 'with', 'and', 'or', 'the', 'a', 'an', 'of', 'on', 'in',
  'at', 'to', 'by', 'from', 'all', 'via',
  'new', 'customers', 'customer', 'users', 'user',
  'service', 'services', 'promotion', 'promotions',
  'offer', 'offering', 'program', 'feature', 'platform',
  'enhanced', 'advanced', 'exclusive', 'special',
  'get', 'your', 'our', 'rebate', 'bonus',
  'earn', 'enjoy', 'limited', 'only', 'up', 'upto',
  'valid', 'terms', 'apply', 'conditions',
  'hong', 'kong', 'hk',
  'mox', 'za', 'ant', 'airstar', 'elebank', 'fusion',
  'pao', 'padb', 'livi', 'welab',
]);

// ── Dedup helper functions ─────────────────────────────────────────────────

const _normCache = new Map<string, string>();
const _tokCache = new Map<string, Set<string>>();

function normalizeTitle(title: string): string {
  if (!title) return '';
  if (_normCache.has(title)) return _normCache.get(title)!;

  let t = title.toLowerCase().replace(RE_INSTALMENT, 'instalment');
  for (const { regex, replacement } of SYNONYM_PATTERNS) {
    t = t.replace(regex, replacement);
  }
  t = t.replace(RE_AMOUNT, '');
  t = t.replace(RE_PCT, '');
  for (const pat of NOISE_PATTERNS) {
    t = t.replace(pat, '');
  }
  t = t.replace(RE_NONALNUM, '');

  if (_normCache.size > 4096) _normCache.clear();
  _normCache.set(title, t);
  return t;
}

function stem(tok: string): string {
  if (tok.length > 4 && tok.endsWith('s') && !tok.endsWith('ss')) {
    return tok.slice(0, -1);
  }
  return tok;
}

function tokenizeForJaccard(title: string): Set<string> {
  if (!title) return new Set();
  if (_tokCache.has(title)) return _tokCache.get(title)!;

  let t = title.toLowerCase().replace(RE_INSTALMENT, 'instalment');
  for (const { regex, replacement } of SYNONYM_PATTERNS) {
    t = t.replace(regex, replacement);
  }
  t = t.replace(RE_AMOUNT, '');
  t = t.replace(RE_PCT, '');
  t = t.replace(/[^\w\s]/g, ' ');

  const toks = new Set<string>();
  for (const tok of t.split(/\s+/)) {
    if (tok && !JACCARD_STOPWORDS.has(tok) && tok.length > 1) {
      toks.add(stem(tok));
    }
  }

  if (_tokCache.size > 4096) _tokCache.clear();
  _tokCache.set(title, toks);
  return toks;
}

function jaccardSimilarity(title1: string, title2: string): number {
  const a = tokenizeForJaccard(title1);
  const b = tokenizeForJaccard(title2);
  if (!a.size || !b.size) return 0;
  let intersection = 0;
  for (const t of a) { if (b.has(t)) intersection++; }
  return intersection / (a.size + b.size - intersection);
}

function commonPrefixRatio(a: string, b: string): number {
  if (!a || !b) return 0;
  const minLen = Math.min(a.length, b.length);
  let i = 0;
  while (i < minLen && a[i] === b[i]) i++;
  return i / minLen;
}

function extractPromoCodeStem(title: string): string | null {
  if (!title) return null;

  for (const m of title.matchAll(/\b([A-Z][A-Z0-9]{3,14})\b/g)) {
    const code = m[1];
    if (PROMO_CODE_SKIP.has(code)) continue;
    if ((code.match(/[A-Z]/g) || []).length < 2) continue;
    const s = code.replace(/\d{2}$/, '');
    return s.length >= 3 ? s : code;
  }

  for (const m of title.matchAll(/\b(\d+[A-Z]{2,}(?:\d+)?)\b/g)) {
    return m[1];
  }

  return null;
}

function findDuplicateId(
  db: Database.Database,
  bankId: string,
  title: string,
  highlight: string,
): number | null {
  const rows = db.prepare(
    'SELECT id, title, highlight, active FROM promotions WHERE bank_id = ?'
  ).all(bankId) as any[];

  const normNew = normalizeTitle(title);
  const hiSnippet = (highlight || '').trim().slice(0, 150);
  const newCodeStem = extractPromoCodeStem(title);
  const toksNew = tokenizeForJaccard(title);

  for (const row of rows) {
    const isInactive = !row.active;
    const jThr = isInactive ? JACCARD_THRESHOLD_INACTIVE : JACCARD_THRESHOLD;
    const lThr = isInactive ? LCP_THRESHOLD_INACTIVE : LCP_THRESHOLD;

    if (newCodeStem) {
      const oldCodeStem = extractPromoCodeStem(row.title);
      if (oldCodeStem && newCodeStem === oldCodeStem) return row.id;
    }

    const normOld = normalizeTitle(row.title);
    const oldSnip = (row.highlight || '').trim().slice(0, 150);

    if (normNew && normOld) {
      if (normNew === normOld) return row.id;

      const lenNew = normNew.length;
      const lenOld = normOld.length;
      const minLen = Math.min(lenNew, lenOld);
      const maxLen = Math.max(lenNew, lenOld);

      if (
        minLen >= MIN_NORM_LEN &&
        minLen >= maxLen * 0.35 &&
        (normNew.includes(normOld) || normOld.includes(normNew))
      ) {
        return row.id;
      }

      const toksOld = tokenizeForJaccard(row.title);
      const shared = [...toksNew].filter(t => toksOld.has(t)).length;
      if (
        toksNew.size >= MIN_TOKENS &&
        toksOld.size >= MIN_TOKENS &&
        shared >= MIN_TOKENS &&
        jaccardSimilarity(title, row.title) >= jThr
      ) {
        return row.id;
      }

      if (
        lenNew >= MIN_NORM_LEN &&
        lenOld >= MIN_NORM_LEN &&
        commonPrefixRatio(normNew, normOld) >= lThr
      ) {
        return row.id;
      }
    }

    if (hiSnippet && oldSnip && hiSnippet === oldSnip) return row.id;
  }

  return null;
}

// ── 1. Init ────────────────────────────────────────────────────────────────

function rebuildPromotionsTable(db: Database.Database): void {
  console.log('  🔧 Rebuilding promotions table (legacy schema migration)...');
  db.exec(`
    CREATE TABLE IF NOT EXISTS promotions_new (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      bank_id       TEXT    NOT NULL DEFAULT '',
      bank_name     TEXT    NOT NULL DEFAULT '',
      title         TEXT    NOT NULL DEFAULT '',
      description   TEXT    DEFAULT '',
      highlight     TEXT    DEFAULT '',
      category      TEXT    DEFAULT '',
      url           TEXT    DEFAULT '',
      tc_link       TEXT    DEFAULT '',
      start_date    TEXT    DEFAULT NULL,
      period        TEXT    DEFAULT '',
      end_date      TEXT    DEFAULT NULL,
      quota         TEXT    DEFAULT '',
      cost          TEXT    DEFAULT '',
      interest_rate TEXT    DEFAULT '',
      min_deposit   TEXT    DEFAULT '',
      promo_type    TEXT    DEFAULT '',
      is_bau        INTEGER DEFAULT 0,
      first_run_id  INTEGER DEFAULT NULL,
      created_at    TEXT    NOT NULL DEFAULT '',
      first_seen_at TEXT    DEFAULT NULL,
      last_seen     TEXT    NOT NULL DEFAULT '',
      active        INTEGER NOT NULL DEFAULT 1
    )
  `);

  // Try to migrate existing data
  const cols = (db.pragma('table_info(promotions)') as any[]).map(r => r.name);
  const commonCols = [
    'id', 'bank_id', 'bank_name', 'title', 'description', 'highlight',
    'category', 'url', 'tc_link', 'start_date', 'period', 'end_date',
    'quota', 'cost', 'interest_rate', 'min_deposit', 'promo_type',
    'is_bau', 'first_run_id', 'created_at', 'first_seen_at', 'last_seen', 'active',
  ].filter(c => cols.includes(c));

  if (commonCols.length > 0) {
    const colList = commonCols.join(', ');
    db.exec(`INSERT INTO promotions_new (${colList}) SELECT ${colList} FROM promotions`);
    const count = (db.prepare('SELECT COUNT(*) as cnt FROM promotions_new').get() as any).cnt;
    console.log(`  🔧 Migrated ${count} rows from old promotions table`);
  }

  db.exec('DROP TABLE promotions');
  db.exec('ALTER TABLE promotions_new RENAME TO promotions');
  console.log('  🔧 Promotions table rebuilt successfully');
}

export function initDb(): void {
  fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  // Check if promotions table exists and needs rebuild
  const tableExists = (db.prepare(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='promotions'"
  ).get() as any);

  if (tableExists) {
    const existingCols = new Set(
      (db.pragma('table_info(promotions)') as any[]).map(r => r.name)
    );
    // If critical columns are missing, rebuild the table
    const criticalCols = ['bank_id', 'first_seen_at', 'is_bau'];
    const needsRebuild = criticalCols.some(c => !existingCols.has(c));

    if (needsRebuild) {
      rebuildPromotionsTable(db);
    }
  }

  db.exec(`
    CREATE TABLE IF NOT EXISTS promotions (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      bank_id       TEXT    NOT NULL DEFAULT '',
      bank_name     TEXT    NOT NULL DEFAULT '',
      title         TEXT    NOT NULL DEFAULT '',
      description   TEXT    DEFAULT '',
      highlight     TEXT    DEFAULT '',
      category      TEXT    DEFAULT '',
      url           TEXT    DEFAULT '',
      tc_link       TEXT    DEFAULT '',
      start_date    TEXT    DEFAULT NULL,
      period        TEXT    DEFAULT '',
      end_date      TEXT    DEFAULT NULL,
      quota         TEXT    DEFAULT '',
      cost          TEXT    DEFAULT '',
      interest_rate TEXT    DEFAULT '',
      min_deposit   TEXT    DEFAULT '',
      promo_type    TEXT    DEFAULT '',
      is_bau        INTEGER DEFAULT 0,
      first_run_id  INTEGER DEFAULT NULL,
      created_at    TEXT    NOT NULL DEFAULT '',
      first_seen_at TEXT    DEFAULT NULL,
      last_seen     TEXT    NOT NULL DEFAULT '',
      active        INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS scrape_runs (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      run_at        TEXT    NOT NULL,
      banks_scraped TEXT    DEFAULT ''
    );

    CREATE TABLE IF NOT EXISTS products (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      bank_id       TEXT    NOT NULL DEFAULT '',
      bank_name     TEXT    NOT NULL DEFAULT '',
      product_name  TEXT    NOT NULL DEFAULT '',
      category      TEXT    NOT NULL DEFAULT '',
      subcategory   TEXT    DEFAULT '',
      description   TEXT    DEFAULT '',
      features      TEXT    DEFAULT '',
      interest_rate TEXT    DEFAULT '',
      fees          TEXT    DEFAULT '',
      min_deposit   TEXT    DEFAULT '',
      min_balance   TEXT    DEFAULT '',
      url           TEXT    DEFAULT '',
      is_bau        INTEGER DEFAULT 1,
      first_seen_at TEXT    DEFAULT NULL,
      last_seen     TEXT    NOT NULL DEFAULT '',
      active        INTEGER NOT NULL DEFAULT 1,
      za_comparison TEXT    DEFAULT ''
    );

    CREATE TABLE IF NOT EXISTS product_scan_meta (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      bank_id       TEXT    NOT NULL,
      scan_type     TEXT    NOT NULL DEFAULT 'baseline',
      scanned_at    TEXT    NOT NULL,
      product_count INTEGER DEFAULT 0
    );
  `);

  // Migrations: add missing columns
  const existingCols = new Set(
    (db.pragma('table_info(promotions)') as any[]).map(r => r.name)
  );

  const migrations: [string, string][] = [
    ['bank_id',       "ALTER TABLE promotions ADD COLUMN bank_id       TEXT NOT NULL DEFAULT ''"],
    ['bank_name',     "ALTER TABLE promotions ADD COLUMN bank_name     TEXT NOT NULL DEFAULT ''"],
    ['url',           "ALTER TABLE promotions ADD COLUMN url           TEXT DEFAULT ''"],
    ['highlight',     "ALTER TABLE promotions ADD COLUMN highlight     TEXT DEFAULT ''"],
    ['tc_link',       "ALTER TABLE promotions ADD COLUMN tc_link       TEXT DEFAULT ''"],
    ['start_date',    "ALTER TABLE promotions ADD COLUMN start_date    TEXT DEFAULT NULL"],
    ['period',        "ALTER TABLE promotions ADD COLUMN period        TEXT DEFAULT ''"],
    ['end_date',      "ALTER TABLE promotions ADD COLUMN end_date      TEXT DEFAULT NULL"],
    ['quota',         "ALTER TABLE promotions ADD COLUMN quota         TEXT DEFAULT ''"],
    ['cost',          "ALTER TABLE promotions ADD COLUMN cost          TEXT DEFAULT ''"],
    ['category',      "ALTER TABLE promotions ADD COLUMN category      TEXT DEFAULT ''"],
    ['interest_rate', "ALTER TABLE promotions ADD COLUMN interest_rate TEXT DEFAULT ''"],
    ['min_deposit',   "ALTER TABLE promotions ADD COLUMN min_deposit   TEXT DEFAULT ''"],
    ['promo_type',    "ALTER TABLE promotions ADD COLUMN promo_type    TEXT DEFAULT ''"],
    ['is_bau',        "ALTER TABLE promotions ADD COLUMN is_bau        INTEGER DEFAULT 0"],
    ['first_run_id',  "ALTER TABLE promotions ADD COLUMN first_run_id  INTEGER DEFAULT NULL"],
    ['first_seen_at',  "ALTER TABLE promotions ADD COLUMN first_seen_at  TEXT DEFAULT NULL"],
    ['analysis_points', "ALTER TABLE promotions ADD COLUMN analysis_points TEXT DEFAULT '[]'"],
  ];

  for (const [col, sql] of migrations) {
    if (!existingCols.has(col)) {
      db.exec(sql);
      console.log(`  🔧 DB migration: added column "${col}"`);
    }
  }

  // Products table migrations
  const existingProdCols = new Set(
    (db.pragma('table_info(products)') as any[]).map(r => r.name)
  );

  const productMigrations: [string, string][] = [
    ['min_deposit',   "ALTER TABLE products ADD COLUMN min_deposit   TEXT DEFAULT ''"],
    ['min_balance',   "ALTER TABLE products ADD COLUMN min_balance   TEXT DEFAULT ''"],
    ['is_bau',        "ALTER TABLE products ADD COLUMN is_bau        INTEGER DEFAULT 1"],
    ['za_comparison', "ALTER TABLE products ADD COLUMN za_comparison TEXT DEFAULT ''"],
    ['first_seen_at', "ALTER TABLE products ADD COLUMN first_seen_at TEXT DEFAULT NULL"],
  ];

  for (const [col, sql] of productMigrations) {
    if (!existingProdCols.has(col)) {
      db.exec(sql);
      console.log(`  🔧 DB migration (products): added column "${col}"`);
    }
  }

  // Back-fill first_seen_at
  const nullCount = (db.prepare(
    'SELECT COUNT(*) as cnt FROM promotions WHERE first_seen_at IS NULL'
  ).get() as any).cnt;

  if (nullCount > 0) {
    db.exec('UPDATE promotions SET first_seen_at = created_at WHERE first_seen_at IS NULL');
    console.log(`  🔧 DB migration: first_seen_at back-filled for ${nullCount} NULL row(s)`);
  }

  // Indexes
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_bank_id       ON promotions(bank_id);
    CREATE INDEX IF NOT EXISTS idx_active        ON promotions(active);
    CREATE INDEX IF NOT EXISTS idx_last_seen     ON promotions(last_seen);
    CREATE INDEX IF NOT EXISTS idx_created_at    ON promotions(created_at);
    CREATE INDEX IF NOT EXISTS idx_first_seen_at ON promotions(first_seen_at);
    CREATE INDEX IF NOT EXISTS idx_first_run     ON promotions(first_run_id);
    CREATE INDEX IF NOT EXISTS idx_is_bau        ON promotions(is_bau);
    CREATE INDEX IF NOT EXISTS idx_bank_name     ON promotions(bank_name);
    CREATE INDEX IF NOT EXISTS idx_end_date      ON promotions(end_date);

    CREATE INDEX IF NOT EXISTS idx_prod_bank_id      ON products(bank_id);
    CREATE INDEX IF NOT EXISTS idx_prod_active       ON products(active);
    CREATE INDEX IF NOT EXISTS idx_prod_category     ON products(category);
    CREATE INDEX IF NOT EXISTS idx_prod_last_seen    ON products(last_seen);
    CREATE INDEX IF NOT EXISTS idx_prod_first_seen   ON products(first_seen_at);
    CREATE INDEX IF NOT EXISTS idx_prod_bank_name    ON products(bank_name);

    CREATE INDEX IF NOT EXISTS idx_psm_bank_id       ON product_scan_meta(bank_id);
    CREATE INDEX IF NOT EXISTS idx_psm_scan_type     ON product_scan_meta(scan_type);
  `);

  _db = db;
  console.log('  ✅ Database ready');
}

// ── 2. Run tracking ────────────────────────────────────────────────────────

export function startNewRun(banks: string[] = []): number {
  const db = getDb();
  const result = db.prepare(
    'INSERT INTO scrape_runs (run_at, banks_scraped) VALUES (?, ?)'
  ).run(
    new Date().toISOString().slice(0, 19).replace('T', ' '),
    banks.join(','),
  );
  const runId = result.lastInsertRowid as number;
  console.log(`  🏃 Scrape run #${runId} started`);
  return runId;
}

export function getPreviousRunId(currentRunId: number): number | null {
  const db = getDb();
  const row = db.prepare(
    'SELECT id FROM scrape_runs WHERE id < ? ORDER BY id DESC LIMIT 1'
  ).get(currentRunId) as any;
  return row ? row.id : null;
}

// ── 4. Save (upsert) ──────────────────────────────────────────────────────

export interface SaveStats {
  new: number;
  updated: number;
  skipped: number;
  blocked: number;
}

export function savePromotions(
  bankId: string,
  bankName: string,
  promotions: Record<string, any>[],
  currentRunId: number = 0,
  todayStr?: string,
): SaveStats {
  const today = todayStr || hktToday();
  const db = getDb();
  const stats: SaveStats = { new: 0, updated: 0, skipped: 0, blocked: 0 };

  const updateStmt = db.prepare(`
    UPDATE promotions SET
      bank_name   = ?,
      title       = ?,
      highlight   = COALESCE(NULLIF(?, ''), highlight),
      description = COALESCE(NULLIF(?, ''), description),
      category    = COALESCE(NULLIF(?, ''), category),
      start_date  = COALESCE(NULLIF(?, ''), start_date),
      end_date    = COALESCE(NULLIF(?, ''), end_date),
      period      = COALESCE(NULLIF(?, ''), period),
      quota       = COALESCE(NULLIF(?, ''), quota),
      cost        = COALESCE(NULLIF(?, ''), cost),
      promo_type  = COALESCE(NULLIF(?, ''), promo_type),
      url         = COALESCE(NULLIF(?, ''), url),
      tc_link     = COALESCE(NULLIF(?, ''), tc_link),
      is_bau      = ?,
      analysis_points = COALESCE(NULLIF(?, '[]'), analysis_points),
      active      = 1,
      last_seen   = ?
    WHERE id = ?
  `);

  const insertStmt = db.prepare(`
    INSERT INTO promotions
      (bank_id, bank_name, title, highlight, description,
       category, start_date, period, end_date, quota, cost,
       promo_type, url, tc_link, is_bau, analysis_points,
       first_run_id, active, created_at, first_seen_at, last_seen)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
  `);

  const transaction = db.transaction(() => {
    for (const p of promotions) {
      const title = (
        p.title || p.name || p.promotion_name || p.promo_name || ''
      ).trim();
      const highlight = (p.highlight || '').trim();

      if (!title) {
        stats.skipped++;
        console.log(`  ⚠️  [${bankId}] skipped promo with empty title — keys: ${Object.keys(p).join(',')}`);
        continue;
      }

      // Copy3 enhancement: also check description and tc_link
      if (isNonBankContent(title, highlight, p.description || '', p.tc_link || '')) {
        stats.blocked++;
        console.log(`  🚫 [${bankId}] blocked non-bank content: "${title.slice(0, 70)}"`);
        continue;
      }

      const typesRaw = p.types || p.promo_type || [];
      const promoType = Array.isArray(typesRaw)
        ? typesRaw.join(',')
        : String(typesRaw);
      const isBau = p.is_bau ? 1 : 0;
      const startDate = p.start_date || null;
      const endDate = p.end_date || null;

      let period = (p.period || '').trim();
      if (startDate && endDate) period = `${startDate} to ${endDate}`;
      else if (startDate) period = `From ${startDate}`;
      else if (endDate) period = `Until ${endDate}`;
      else if (!period) period = 'Ongoing';

      const preMatchId = p._matched_id ?? null;
      const dupId = preMatchId !== null
        ? preMatchId
        : findDuplicateId(db, bankId, title, highlight);

      if (dupId) {
        const existing = db.prepare('SELECT title FROM promotions WHERE id = ?').get(dupId) as any;
        const keepTitle = existing && title.length >= existing.title.length
          ? title
          : (existing ? existing.title : title);

        updateStmt.run(
          bankName, keepTitle, highlight,
          p.description || '', p.category || '',
          startDate || '', endDate || '', period,
          p.quota || '', p.cost || '',
          promoType, p.url || '', p.tc_link || '',
          isBau, JSON.stringify(p.analysis_points || []), today, dupId,
        );
        stats.updated++;
      } else {
        insertStmt.run(
          bankId, bankName, title, highlight,
          p.description || '', p.category || '',
          startDate, period, endDate,
          p.quota || '', p.cost || '',
          promoType, p.url || '', p.tc_link || '',
          isBau, JSON.stringify(p.analysis_points || []),
          currentRunId || null,
          today, today, today,
        );
        stats.new++;
      }
    }
  });

  try {
    transaction();
    const blockedTag = stats.blocked ? `  blocked:${stats.blocked}` : '';
    console.log(
      `  [${bankId}] saved → new:${stats.new}  updated:${stats.updated}  skipped:${stats.skipped}${blockedTag}`
    );
    return stats;
  } catch (exc) {
    console.log(`  ❌ savePromotions error: ${exc}`);
    throw exc;
  }
}

// ── 5. Mark stale / old inactive ──────────────────────────────────────────

export function markStaleAsInactive(bankIdsScraped: string[], todayStr?: string): number {
  if (!bankIdsScraped.length) return 0;
  const today = todayStr || hktToday();
  const db = getDb();

  const stmt = db.prepare(`
    UPDATE promotions SET active = 0
    WHERE bank_id        = ?
      AND active         = 1
      AND DATE(last_seen) < ?
      AND (
            end_date IS NULL
         OR end_date  = ''
         OR DATE(end_date) < ?
      )
  `);

  const transaction = db.transaction(() => {
    let total = 0;
    for (const bankId of bankIdsScraped) {
      const result = stmt.run(bankId, today, today);
      if (result.changes) {
        console.log(`  🗑️  ${bankId}: ${result.changes} promo(s) marked inactive (end_date past or unknown)`);
      }
      total += result.changes;
    }
    return total;
  });

  try {
    return transaction();
  } catch (exc) {
    console.log(`  ❌ markStaleAsInactive error: ${exc}`);
    return 0;
  }
}

export function markInactiveOld(daysThreshold: number = 90): number {
  const cutoff = new Date(Date.now() - daysThreshold * 86400000)
    .toISOString().slice(0, 19).replace('T', ' ');
  const db = getDb();

  try {
    const result = db.prepare(
      'UPDATE promotions SET active = 0 WHERE last_seen < ? AND active = 1'
    ).run(cutoff);
    console.log(`  🗑️  ${result.changes} old promos marked inactive (>${daysThreshold}d)`);
    return result.changes;
  } catch (exc) {
    console.log(`  ❌ markInactiveOld error: ${exc}`);
    return 0;
  }
}

export function reactivatePromotionsSeenOn(dateStr: string): number {
  const db = getDb();
  try {
    const result = db.prepare(
      'UPDATE promotions SET active = 1 WHERE DATE(last_seen) = ? AND active = 0'
    ).run(dateStr);
    if (result.changes) {
      console.log(`  🔄 Recovery: reactivated ${result.changes} promo(s) with last_seen=${dateStr}`);
    } else {
      console.log(`  ⚠️  Recovery: no promotions found with last_seen=${dateStr}`);
    }
    return result.changes;
  } catch (exc) {
    console.log(`  ❌ reactivatePromotionsSeenOn error: ${exc}`);
    return 0;
  }
}

export function reactivateMostRecentlySeen(windowDays: number = 7): number {
  const db = getDb();
  try {
    const row = db.prepare(
      'SELECT MAX(DATE(last_seen)) AS max_date FROM promotions'
    ).get() as any;

    if (!row || !row.max_date) {
      console.log('  ⚠️  Recovery: DB is completely empty — nothing to reactivate');
      return 0;
    }

    const maxDateStr = row.max_date;
    const maxDate = new Date(maxDateStr);
    const cutoff = new Date(maxDate.getTime() - windowDays * 86400000)
      .toISOString().slice(0, 10);

    const result = db.prepare(
      'UPDATE promotions SET active = 1 WHERE DATE(last_seen) >= ? AND active = 0'
    ).run(cutoff);

    if (result.changes) {
      console.log(
        `  🔄 Recovery: reactivated ${result.changes} promo(s) (last_seen >= ${cutoff}; most recent batch: ${maxDateStr})`
      );
    } else {
      console.log(
        `  ⚠️  Recovery: all promotions already active (most recent last_seen: ${maxDateStr})`
      );
    }
    return result.changes;
  } catch (exc) {
    console.log(`  ❌ reactivateMostRecentlySeen error: ${exc}`);
    return 0;
  }
}

// ── 5-b. Repair re-inserted promotions ────────────────────────────────────

export function repairReinsertedPromotions(dryRun: boolean = false): number {
  const today = hktToday();
  const db = getDb();

  const transaction = db.transaction(() => {
    let fixed = 0;
    const todayRows = db.prepare(
      'SELECT id, bank_id, title, highlight FROM promotions WHERE DATE(COALESCE(first_seen_at, created_at)) = ? AND active = 1'
    ).all(today) as any[];

    if (!todayRows.length) {
      console.log('  🔧 repairReinsertedPromotions: nothing to repair');
      return 0;
    }

    for (const row of todayRows) {
      const { bank_id: bankId, title, id: rowId } = row;
      const hiNew = (row.highlight || '').trim().slice(0, 120);

      const olderRows = db.prepare(
        'SELECT id, title, highlight, first_seen_at, created_at FROM promotions WHERE bank_id = ? AND id != ? AND DATE(COALESCE(first_seen_at, created_at)) < ?'
      ).all(bankId, rowId, today) as any[];

      let bestMatchId: number | null = null;
      let bestOlderDate: string | null = null;
      let bestReason = '';

      const codeNew = extractPromoCodeStem(title);
      const normNew = normalizeTitle(title);

      for (const older of olderRows) {
        const hiOld = (older.highlight || '').trim().slice(0, 120);
        const normOld = normalizeTitle(older.title);
        const codeOld = extractPromoCodeStem(older.title);

        let isMatch = false;
        let reason = '';

        if (codeNew && codeOld && codeNew === codeOld) {
          isMatch = true; reason = `code-stem=${codeNew}`;
        } else if (normNew && normOld && normNew === normOld) {
          isMatch = true; reason = 'exact-norm';
        } else if (normNew && normOld) {
          const minL = Math.min(normNew.length, normOld.length);
          const maxL = Math.max(normNew.length, normOld.length);
          if (minL >= MIN_NORM_LEN && minL >= maxL * 0.35 &&
              (normNew.includes(normOld) || normOld.includes(normNew))) {
            isMatch = true; reason = 'substring-norm';
          }
        }

        if (!isMatch) {
          const j = jaccardSimilarity(title, older.title);
          if (j >= JACCARD_THRESHOLD_INACTIVE) {
            isMatch = true; reason = `jaccard=${j.toFixed(2)}`;
          }
        }

        if (!isMatch && hiNew && hiOld && hiNew === hiOld) {
          isMatch = true; reason = 'same-highlight';
        }

        if (isMatch) {
          const olderDate = older.first_seen_at || older.created_at;
          if (olderDate && olderDate < today) {
            if (!bestOlderDate || olderDate < bestOlderDate) {
              bestMatchId = older.id;
              bestOlderDate = olderDate;
              bestReason = reason;
            }
          }
        }
      }

      if (bestMatchId && bestOlderDate) {
        const tag = dryRun ? '[DRY RUN] ' : '';
        console.log(
          `  🔧 ${tag}repair [${bankId}] "${title.slice(0, 55)}" first_seen_at ${today} → ${bestOlderDate} (reason: ${bestReason}, old_id=${bestMatchId})`
        );
        if (!dryRun) {
          db.prepare('UPDATE promotions SET first_seen_at = ? WHERE id = ?').run(bestOlderDate, rowId);
          db.prepare('UPDATE promotions SET active = 0 WHERE id = ?').run(bestMatchId);
        }
        fixed++;
      }
    }

    return fixed;
  });

  try {
    const fixed = transaction();
    const tag = dryRun ? '[DRY RUN] ' : '';
    console.log(`  🔧 ${tag}repairReinsertedPromotions: ${fixed} row(s) corrected`);
    return fixed;
  } catch (exc) {
    console.log(`  ❌ repairReinsertedPromotions error: ${exc}`);
    return 0;
  }
}

// ── 6. Queries ────────────────────────────────────────────────────────────

export function getNewPromotionsToday(includeBau: boolean = false): Record<string, any>[] {
  const today = hktToday();
  const db = getDb();
  try {
    const bauClause = includeBau ? '' : 'AND is_bau = 0';
    return db.prepare(`
      SELECT * FROM promotions
      WHERE active = 1
        AND COALESCE(DATE(first_seen_at), DATE(created_at)) = ?
        AND (end_date IS NULL OR end_date = '' OR DATE(end_date) >= ?)
        ${bauClause}
      ORDER BY bank_id ASC, id ASC
    `).all(today, today) as any[];
  } catch (exc) {
    console.log(`  ❌ getNewPromotionsToday error: ${exc}`);
    return [];
  }
}

export function getNewPromotionsForRun(currentRunId: number, includeBau: boolean = false): Record<string, any>[] {
  const db = getDb();
  try {
    const bauClause = includeBau ? '' : 'AND is_bau = 0';
    return db.prepare(`
      SELECT * FROM promotions
      WHERE first_run_id = ?
        AND active = 1
        ${bauClause}
      ORDER BY bank_id ASC, id ASC
    `).all(currentRunId) as any[];
  } catch (exc) {
    console.log(`  ❌ getNewPromotionsForRun error: ${exc}`);
    return [];
  }
}

export function getNewPromotionsLastNDays(
  days: number = 6,
  includeBau: boolean = false,
  excludeRunId?: number,
): Record<string, any>[] {
  const today = hktToday();
  const since = hktNDaysAgo(days);
  const db = getDb();
  try {
    const bauClause = includeBau ? '' : 'AND is_bau = 0';
    const runClause = excludeRunId !== undefined
      ? `AND (first_run_id IS NULL OR first_run_id != ${excludeRunId})`
      : '';
    return db.prepare(`
      SELECT * FROM promotions
      WHERE active = 1
        AND COALESCE(DATE(first_seen_at), DATE(created_at)) >= ?
        AND COALESCE(DATE(first_seen_at), DATE(created_at)) <  ?
        AND (end_date IS NULL OR end_date = '' OR DATE(end_date) >= ?)
        ${bauClause}
        ${runClause}
      ORDER BY COALESCE(first_seen_at, created_at) DESC, bank_id ASC
    `).all(since, today, today) as any[];
  } catch (exc) {
    console.log(`  ❌ getNewPromotionsLastNDays error: ${exc}`);
    return [];
  }
}

export function getActivePromotions(includeBau: boolean = true): Record<string, any>[] {
  const db = getDb();
  try {
    const bauClause = includeBau ? '' : 'AND is_bau = 0';
    return db.prepare(`
      SELECT * FROM promotions
      WHERE active = 1 ${bauClause}
      ORDER BY bank_id ASC, last_seen DESC
    `).all() as any[];
  } catch (exc) {
    console.log(`  ❌ getActivePromotions error: ${exc}`);
    return [];
  }
}

export function getExpiredPromotions(): Record<string, any>[] {
  const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
  const db = getDb();
  try {
    return db.prepare(`
      SELECT * FROM promotions
      WHERE active = 0
        AND is_bau  = 0
        AND DATE(last_seen) >= ?
      ORDER BY bank_id ASC, last_seen DESC
    `).all(yesterday) as any[];
  } catch (exc) {
    console.log(`  ❌ getExpiredPromotions error: ${exc}`);
    return [];
  }
}

export function getActivePromosForBank(bankId: string): Record<string, any>[] {
  const db = getDb();
  try {
    return db.prepare(
      'SELECT id, title FROM promotions WHERE bank_id = ? AND active = 1'
    ).all(bankId) as any[];
  } catch (exc) {
    console.log(`  ❌ getActivePromosForBank error: ${exc}`);
    return [];
  }
}

export function getPromotionsByBankName(bankName: string): Record<string, any>[] {
  const db = getDb();
  try {
    return db.prepare(
      'SELECT * FROM promotions WHERE bank_name = ? AND active = 1 ORDER BY last_seen DESC'
    ).all(bankName) as any[];
  } catch (exc) {
    console.log(`  ❌ getPromotionsByBankName error: ${exc}`);
    return [];
  }
}

export function getDbStats(): Record<string, any> {
  const db = getDb();
  try {
    const total = (db.prepare('SELECT COUNT(*) as cnt FROM promotions').get() as any).cnt;
    const active = (db.prepare('SELECT COUNT(*) as cnt FROM promotions WHERE active=1').get() as any).cnt;
    const bau = (db.prepare('SELECT COUNT(*) as cnt FROM promotions WHERE active=1 AND is_bau=1').get() as any).cnt;
    const runs = (db.prepare('SELECT COUNT(*) as cnt FROM scrape_runs').get() as any).cnt;
    const lastRun = db.prepare('SELECT run_at FROM scrape_runs ORDER BY id DESC LIMIT 1').get() as any;
    return {
      total_promotions: total,
      active_promotions: active,
      bau_promotions: bau,
      non_bau_active: active - bau,
      total_runs: runs,
      last_run_at: lastRun ? lastRun.run_at : null,
    };
  } catch (exc) {
    console.log(`  ❌ getDbStats error: ${exc}`);
    return {};
  }
}

export function generateDailyReport(currentRunId: number): Record<string, any> {
  const newPromos = getNewPromotionsToday(false);
  const expiredPromos = getExpiredPromotions();
  const allActive = getActivePromotions(false);

  const newIds = new Set(newPromos.map(p => p.id));
  const ongoing = allActive.filter(p => !newIds.has(p.id));

  const byBank: Record<string, number> = {};
  for (const p of allActive) {
    const bid = p.bank_id || 'unknown';
    byBank[bid] = (byBank[bid] || 0) + 1;
  }

  return {
    new: newPromos,
    active: ongoing,
    expired: expiredPromos,
    summary: {
      total_active: allActive.length,
      new_count: newPromos.length,
      expired_count: expiredPromos.length,
      by_bank: byBank,
    },
  };
}

// ── 7. Merge duplicates ──────────────────────────────────────────────────

export function mergeDuplicatePromotions(dryRun: boolean = true): number {
  const db = getDb();
  try {
    const rows = db.prepare(
      'SELECT id, bank_id, title, highlight FROM promotions WHERE active = 1 ORDER BY id ASC'
    ).all() as any[];

    const byBank: Record<string, any[]> = {};
    for (const row of rows) {
      (byBank[row.bank_id] ??= []).push(row);
    }

    const discardIds = new Set<number>();

    for (const [bankId, promos] of Object.entries(byBank)) {
      for (let i = 0; i < promos.length; i++) {
        const pa = promos[i];
        if (discardIds.has(pa.id)) continue;

        const normA = normalizeTitle(pa.title);
        const hiA = (pa.highlight || '').trim().slice(0, 150);
        const codeStemA = extractPromoCodeStem(pa.title);
        const toksA = tokenizeForJaccard(pa.title);

        for (let j = i + 1; j < promos.length; j++) {
          const pb = promos[j];
          if (discardIds.has(pb.id)) continue;

          const normB = normalizeTitle(pb.title);
          const hiB = (pb.highlight || '').trim().slice(0, 150);
          const codeStemB = extractPromoCodeStem(pb.title);
          const toksB = tokenizeForJaccard(pb.title);

          let isDup = false;
          let reason = '';

          if (codeStemA && codeStemB && codeStemA === codeStemB) {
            isDup = true; reason = `promo-code=${codeStemA}`;
          } else if (normA && normB) {
            if (normA === normB) {
              isDup = true; reason = 'exact';
            } else {
              const minLen = Math.min(normA.length, normB.length);
              const maxLen = Math.max(normA.length, normB.length);
              if (minLen >= MIN_NORM_LEN && minLen >= maxLen * 0.35 &&
                  (normA.includes(normB) || normB.includes(normA))) {
                isDup = true; reason = 'substring';
              }

              if (!isDup) {
                const shared = [...toksA].filter(t => toksB.has(t)).length;
                if (toksA.size >= MIN_TOKENS && toksB.size >= MIN_TOKENS && shared >= MIN_TOKENS) {
                  const jVal = shared / (toksA.size + toksB.size - shared);
                  if (jVal >= JACCARD_THRESHOLD) {
                    isDup = true; reason = `Jaccard=${jVal.toFixed(2)}`;
                  }
                }
              }

              if (!isDup && normA.length >= MIN_NORM_LEN && normB.length >= MIN_NORM_LEN) {
                const lcp = commonPrefixRatio(normA, normB);
                if (lcp >= LCP_THRESHOLD) {
                  isDup = true; reason = `LCP=${lcp.toFixed(2)}`;
                }
              }
            }
          }

          if (!isDup && hiA && hiB && hiA === hiB) {
            isDup = true; reason = 'same-highlight';
          }

          if (isDup) {
            const keep = pa.title.length >= pb.title.length ? pa : pb;
            const discard = keep.id === pa.id ? pb : pa;
            discardIds.add(discard.id);
            const tag = dryRun ? '[DRY RUN] ' : '';
            console.log(
              `    🔀 ${tag}[${reason}]\n       KEEP    #${String(keep.id).padStart(5)}  '${keep.title.slice(0, 70)}'\n       DISCARD #${String(discard.id).padStart(5)}  '${discard.title.slice(0, 70)}'`
            );
          }
        }
      }
    }

    if (!dryRun && discardIds.size) {
      const placeholders = Array.from(discardIds).map(() => '?').join(',');
      db.prepare(`UPDATE promotions SET active = 0 WHERE id IN (${placeholders})`)
        .run(...discardIds);
    }

    const tag = dryRun ? '[DRY RUN] ' : '';
    console.log(`  ${tag}mergeDuplicatePromotions: ${discardIds.size} removed`);
    return discardIds.size;
  } catch (exc) {
    console.log(`  ❌ mergeDuplicatePromotions error: ${exc}`);
    return 0;
  }
}

// ── 8. Maintenance ───────────────────────────────────────────────────────

export function vacuumDb(): void {
  const db = getDb();
  try {
    db.exec('VACUUM');
    console.log('  🧹 VACUUM completed');
  } catch (exc) {
    console.log(`  ❌ vacuumDb error: ${exc}`);
  }
}

// ── 9. Load & Export ──────────────────────────────────────────────────────

export function loadPromotions(activeOnly: boolean = true): Record<string, any>[] {
  const db = getDb();
  try {
    const where = activeOnly ? 'WHERE active = 1' : '';
    return db.prepare(`SELECT * FROM promotions ${where} ORDER BY bank_id ASC, last_seen DESC`).all() as any[];
  } catch (exc) {
    console.log(`  ❌ loadPromotions error: ${exc}`);
    return [];
  }
}

function parseJsonArray(val: any): string[] {
  if (Array.isArray(val)) return val;
  if (typeof val === 'string' && val.trim()) {
    try { const p = JSON.parse(val); return Array.isArray(p) ? p : []; } catch { return []; }
  }
  return [];
}

function sanitizeInsights(insights: Record<string, any> | null | undefined): Record<string, any> | null {
  if (!insights) return null;

  function toList(val: any): any[] {
    if (val == null) return [];
    if (Array.isArray(val)) return val.map((v: any) => String(v).trim()).filter(Boolean);
    if (typeof val === 'string') {
      for (const sep of ['\n', '•', ';']) {
        if (val.includes(sep)) {
          return val.split(sep).map(s => s.replace(/^[-–*·•\s]+/, '').trim()).filter(Boolean);
        }
      }
      return val.trim() ? [val.trim()] : [];
    }
    return [];
  }

  const bankAnalysis = insights.bank_analysis;
  if (bankAnalysis && typeof bankAnalysis === 'object') {
    for (const bankData of Object.values(bankAnalysis) as any[]) {
      if (typeof bankData !== 'object') continue;
      for (const field of ['strengths', 'vs_za_pros', 'vs_za_cons']) {
        bankData[field] = toList(bankData[field]);
      }
    }
  }

  const bestFor = insights.best_for;
  if (Array.isArray(bestFor)) {
    for (const item of bestFor) {
      if (typeof item !== 'object') continue;
      item.similar_banks = toList(item.similar_banks);
    }
  }

  return insights;
}

export function exportToJson(
  outputPath: string,
  strategicInsights?: Record<string, any> | null,
  aiUnavailable: boolean = false,
): void {
  const allPromos = loadPromotions(false);
  const records: Record<string, any>[] = [];

  for (const p of allPromos) {
    const rawType = p.promo_type || p.category || '';
    let typesList: string[];
    if (typeof rawType === 'string') {
      typesList = rawType.split(',').map((t: string) => t.trim()).filter(Boolean);
    } else {
      typesList = Array.isArray(rawType) ? rawType : [];
    }
    if (!typesList.length) typesList = ['Others'];

    records.push({
      id: p.id,
      bank_id: p.bank_id || '',
      bank_name: p.bank_name || '',
      title: p.title || '',
      highlight: p.highlight || '',
      description: p.description || '',
      category: p.category || '',
      start_date: p.start_date || null,
      end_date: p.end_date || null,
      period: p.period || 'Ongoing',
      quota: p.quota || '',
      cost: p.cost || '',
      types: typesList,
      url: p.url || '',
      tc_link: p.tc_link || p.url || '',
      is_bau: Boolean(p.is_bau),
      analysis_points: parseJsonArray(p.analysis_points),
      active: Boolean(p.active ?? 1),
      created_at: p.created_at || '',
      first_seen_at: p.first_seen_at || '',
      last_seen: p.last_seen || '',
    });
  }

  const cleanInsights = sanitizeInsights(strategicInsights || null);

  // ── Products export ──
  const allProducts = getActiveProducts(true);
  const productRecords: Record<string, any>[] = [];

  for (const p of allProducts) {
    productRecords.push({
      id: p.id,
      bank_id: p.bank_id || '',
      bank_name: p.bank_name || '',
      product_name: p.product_name || '',
      category: p.category || 'Others',
      subcategory: p.subcategory || '',
      description: p.description || '',
      features: parseJsonArray(p.features),
      interest_rate: p.interest_rate || '',
      fees: p.fees || '',
      min_deposit: p.min_deposit || '',
      min_balance: p.min_balance || '',
      url: p.url || '',
      is_bau: Boolean(p.is_bau ?? 1),
      za_comparison: (() => {
        if (!p.za_comparison) return null;
        try { return JSON.parse(p.za_comparison); } catch { return null; }
      })(),
      first_seen_at: p.first_seen_at || '',
      last_seen: p.last_seen || '',
    });
  }

  const output: Record<string, any> = {
    updated: new Date().toISOString().slice(0, 19).replace('T', ' '),
    promotions: records,
    products: productRecords,
  };
  if (cleanInsights) output.strategic_insights = cleanInsights;
  if (aiUnavailable) output.ai_unavailable = true;

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2), 'utf-8');

  const activeNonBau = records.filter(r => r.active && !r.is_bau).length;
  const bauN = records.filter(r => r.is_bau).length;
  const expiredN = records.filter(r => !r.active).length;
  const prodN = productRecords.length;
  const insightsTag = cleanInsights ? ' +insights' : '';
  const aiTag = aiUnavailable ? ' [AI unavailable — cached]' : '';
  console.log(
    `  📄 data.json → ${activeNonBau} active non-BAU (+${bauN} BAU), ${expiredN} expired, ${prodN} products${insightsTag}${aiTag} → ${outputPath}`
  );
}

// ── 10. Products: Save (upsert) ───────────────────────────────────────────

export interface ProductSaveStats {
  new: number;
  updated: number;
  skipped: number;
}

export function saveProducts(
  bankId: string,
  bankName: string,
  products: Record<string, any>[],
  todayStr?: string,
): ProductSaveStats {
  const today = todayStr || hktToday();
  const db = getDb();
  const stats: ProductSaveStats = { new: 0, updated: 0, skipped: 0 };

  const updateStmt = db.prepare(`
    UPDATE products SET
      bank_name     = ?,
      product_name  = ?,
      subcategory   = COALESCE(NULLIF(?, ''), subcategory),
      description   = COALESCE(NULLIF(?, ''), description),
      features      = COALESCE(NULLIF(?, '[]'), features),
      interest_rate = COALESCE(NULLIF(?, ''), interest_rate),
      fees          = COALESCE(NULLIF(?, ''), fees),
      min_deposit   = COALESCE(NULLIF(?, ''), min_deposit),
      min_balance   = COALESCE(NULLIF(?, ''), min_balance),
      url           = COALESCE(NULLIF(?, ''), url),
      is_bau        = ?,
      za_comparison = COALESCE(NULLIF(?, ''), za_comparison),
      active        = 1,
      last_seen     = ?
    WHERE id = ?
  `);

  const insertStmt = db.prepare(`
    INSERT INTO products
      (bank_id, bank_name, product_name, category, subcategory,
       description, features, interest_rate, fees, min_deposit,
       min_balance, url, is_bau, za_comparison, active, first_seen_at, last_seen)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
  `);

  const findDup = db.prepare(
    'SELECT id FROM products WHERE bank_id = ? AND category = ? AND active = 1'
  );

  const transaction = db.transaction(() => {
    for (const p of products) {
      const name = (p.product_name || p.name || '').trim();
      if (!name) {
        stats.skipped++;
        continue;
      }

      const category = p.category || 'Others';
      const isBau = p.is_bau !== false ? 1 : 0;
      const features = JSON.stringify(p.features || []);

      // Find existing product with same bank + category (simple dedup)
      let dupId: number | null = p._matched_id ?? null;
      if (!dupId) {
        const existing = findDup.all(bankId, category) as any[];
        for (const row of existing) {
          // Re-fetch to get product_name for comparison
          const ex = db.prepare('SELECT product_name FROM products WHERE id = ?').get(row.id) as any;
          if (ex && ex.product_name) {
            const normExisting = ex.product_name.toLowerCase().replace(/[\s\-_]+/g, '');
            const normNew = name.toLowerCase().replace(/[\s\-_]+/g, '');
            if (normExisting === normNew || normExisting.includes(normNew) || normNew.includes(normExisting)) {
              dupId = row.id;
              break;
            }
          }
        }
      }

      if (dupId) {
        const existing = db.prepare('SELECT product_name FROM products WHERE id = ?').get(dupId) as any;
        const keepName = existing && name.length >= existing.product_name.length
          ? name
          : (existing ? existing.product_name : name);

        updateStmt.run(
          bankName, keepName,
          p.subcategory || '', p.description || '',
          features,
          p.interest_rate || '', p.fees || '',
          p.min_deposit || '', p.min_balance || '',
          p.url || '', isBau,
          p.za_comparison || '',
          today, dupId,
        );
        stats.updated++;
      } else {
        insertStmt.run(
          bankId, bankName, name, category,
          p.subcategory || '', p.description || '',
          features,
          p.interest_rate || '', p.fees || '',
          p.min_deposit || '', p.min_balance || '',
          p.url || '', isBau,
          p.za_comparison || '',
          today, today,
        );
        stats.new++;
      }
    }
  });

  try {
    transaction();
    console.log(
      `  [${bankId}] products saved → new:${stats.new}  updated:${stats.updated}  skipped:${stats.skipped}`
    );
    return stats;
  } catch (exc) {
    console.log(`  ❌ saveProducts error: ${exc}`);
    throw exc;
  }
}

// ── 11. Products: Queries ─────────────────────────────────────────────────

export function getActiveProducts(includeBau: boolean = true): Record<string, any>[] {
  const db = getDb();
  try {
    const bauClause = includeBau ? '' : 'AND is_bau = 0';
    return db.prepare(`
      SELECT * FROM products
      WHERE active = 1 ${bauClause}
      ORDER BY bank_id ASC, category ASC, last_seen DESC
    `).all() as any[];
  } catch (exc) {
    console.log(`  ❌ getActiveProducts error: ${exc}`);
    return [];
  }
}

export function getNewProductsToday(includeBau: boolean = false): Record<string, any>[] {
  const today = hktToday();
  const db = getDb();
  try {
    const bauClause = includeBau ? '' : 'AND is_bau = 0';
    return db.prepare(`
      SELECT * FROM products
      WHERE active = 1
        AND DATE(first_seen_at) = ?
        ${bauClause}
      ORDER BY bank_id ASC, category ASC
    `).all(today) as any[];
  } catch (exc) {
    console.log(`  ❌ getNewProductsToday error: ${exc}`);
    return [];
  }
}

export function getNewProductsLastNDays(
  days: number = 6,
  includeBau: boolean = false,
): Record<string, any>[] {
  const today = hktToday();
  const since = hktNDaysAgo(days);
  const db = getDb();
  try {
    const bauClause = includeBau ? '' : 'AND is_bau = 0';
    return db.prepare(`
      SELECT * FROM products
      WHERE active = 1
        AND DATE(first_seen_at) >= ?
        AND DATE(first_seen_at) <  ?
        ${bauClause}
      ORDER BY first_seen_at DESC, bank_id ASC, category ASC
    `).all(since, today) as any[];
  } catch (exc) {
    console.log(`  ❌ getNewProductsLastNDays error: ${exc}`);
    return [];
  }
}

export function getProductsByBank(bankId: string): Record<string, any>[] {
  const db = getDb();
  try {
    return db.prepare(
      'SELECT * FROM products WHERE bank_id = ? AND active = 1 ORDER BY category ASC'
    ).all(bankId) as any[];
  } catch (exc) {
    console.log(`  ❌ getProductsByBank error: ${exc}`);
    return [];
  }
}

export function getProductsByCategory(category: string): Record<string, any>[] {
  const db = getDb();
  try {
    return db.prepare(
      'SELECT * FROM products WHERE category = ? AND active = 1 ORDER BY bank_id ASC'
    ).all(category) as any[];
  } catch (exc) {
    console.log(`  ❌ getProductsByCategory error: ${exc}`);
    return [];
  }
}

// ── 12. Products: Staleness ───────────────────────────────────────────────

export function markStaleProductsInactive(bankIdsScraped: string[], todayStr?: string): number {
  if (!bankIdsScraped.length) return 0;
  const today = todayStr || hktToday();
  const db = getDb();

  const stmt = db.prepare(`
    UPDATE products SET active = 0
    WHERE bank_id = ?
      AND active = 1
      AND DATE(last_seen) < ?
  `);

  const transaction = db.transaction(() => {
    let total = 0;
    for (const bankId of bankIdsScraped) {
      const result = stmt.run(bankId, today);
      if (result.changes) {
        console.log(`  🗑️  ${bankId}: ${result.changes} product(s) marked inactive`);
      }
      total += result.changes;
    }
    return total;
  });

  try {
    return transaction();
  } catch (exc) {
    console.log(`  ❌ markStaleProductsInactive error: ${exc}`);
    return 0;
  }
}

// ── 13. Products: Baseline scan tracking ──────────────────────────────────

export function hasBaselineScan(bankId: string): boolean {
  const db = getDb();
  try {
    const row = db.prepare(
      "SELECT id FROM product_scan_meta WHERE bank_id = ? AND scan_type = 'baseline' LIMIT 1"
    ).get(bankId) as any;
    return !!row;
  } catch (exc) {
    console.log(`  ❌ hasBaselineScan error: ${exc}`);
    return false;
  }
}

export function recordScan(bankId: string, scanType: string, productCount: number): void {
  const db = getDb();
  try {
    db.prepare(
      'INSERT INTO product_scan_meta (bank_id, scan_type, scanned_at, product_count) VALUES (?, ?, ?, ?)'
    ).run(bankId, scanType, hktToday(), productCount);
    console.log(`  📋 ${bankId}: ${scanType} scan recorded (${productCount} products)`);
  } catch (exc) {
    console.log(`  ❌ recordScan error: ${exc}`);
  }
}

// ── Close ─────────────────────────────────────────────────────────────────

export function closeDb(): void {
  if (_db) {
    _db.close();
    _db = null;
  }
}
