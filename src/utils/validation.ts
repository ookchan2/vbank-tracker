/**
 * Domain validation and content scrubbing utilities.
 * Ported from Copy3's request interception and content scrubbing logic.
 * @author Alfie
 */

import { URL } from 'url';
import { createHash } from 'crypto';

// ── Bank domain allow-list ──────────────────────────────────────────────────

const BANK_DOMAINS: Record<string, string[]> = {
  za:      ['bank.za.group', 'za.group'],
  mox:     ['mox.com'],
  livi:    ['livibank.com'],
  welab:   ['welab.bank'],
  pao:     ['pingandb.com'],
  airstar: ['elebank.com'],
  fusion:  ['fusionbank.com'],
  ant:     ['antbank.hk'],
};

/** CDN hosts allowed during scraping (CSS, fonts, JS libs) */
export const CDN_ALLOWLIST: Set<string> = new Set([
  'cdn.tailwindcss.com',
  'fonts.googleapis.com',
  'fonts.gstatic.com',
  'cdnjs.cloudflare.com',
  'unpkg.com',
  'jsdelivr.net',
]);

/** File extensions to block during content fetching */
const BLOCKED_EXTENSIONS = /\.(png|jpg|jpeg|gif|webp|ico|woff2?|ttf|eot|otf|mp4|mp3|pdf|zip)(\?.*)?$/i;

// ── Blocked content strings for scrubbing ────────────────────────────────────

const BLOCKED_CONTENT_STRINGS: string[] = [
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
];

const BLOCKED_CONTENT_RE = new RegExp(
  BLOCKED_CONTENT_STRINGS.map(s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|'),
  'i',
);

// ── URL validation ──────────────────────────────────────────────────────────

/**
 * Check if a URL belongs to the bank's allowed domains.
 * @param url - URL to validate
 * @param bankId - Bank identifier (e.g. 'za', 'mox')
 * @returns true if the URL's hostname is in the bank's allow-list
 */
export function isValidBankUrl(url: string, bankId: string): boolean {
  const allowed = BANK_DOMAINS[bankId];
  if (!allowed?.length) return true;
  try {
    const hostname = new URL(url).hostname || '';
    return allowed.some(d => hostname === d || hostname.endsWith('.' + d));
  } catch {
    return false;
  }
}

/**
 * Check if a URL has a blocked file extension.
 */
export function hasBlockedExtension(url: string): boolean {
  return BLOCKED_EXTENSIONS.test(url);
}

// ── Content scrubbing ───────────────────────────────────────────────────────

/**
 * Remove fragments containing blocked content from scraped text.
 * Splits on sentence boundaries and newlines, removes matching fragments.
 */
export function scrubBlockedContent(text: string, bankName: string = ''): string {
  if (!text) return text;

  const fragments = text.split(/(?<=[.。!?！？])\s+|\n+/);
  const clean: string[] = [];
  let removed = 0;

  for (const frag of fragments) {
    if (BLOCKED_CONTENT_RE.test(frag)) {
      removed++;
      const snippet = frag.trim().slice(0, 80);
      console.log(`    [SCRUB] Scrubbed blocked content: "${snippet}…"`);
    } else {
      clean.push(frag);
    }
  }

  if (removed) {
    const label = bankName ? ` [${bankName}]` : '';
    console.log(
      `    [SCRUB] Content scrubber${label}: ${removed} fragment(s) removed containing blocked URLs/phrases`,
    );
  }

  return clean.join(' ');
}

// ── Screenshot cache ────────────────────────────────────────────────────────

import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { join, resolve } from 'path';

const SCREENSHOT_CACHE_DIR = resolve(process.cwd(), '.screenshot_cache');

function screenshotPath(url: string): string {
  const fname = createHash('md5').update(url).digest('hex') + '.png';
  return join(SCREENSHOT_CACHE_DIR, fname);
}

/**
 * Save screenshot data to disk cache.
 * @returns Cache file path or null on error
 */
export function cacheScreenshot(url: string, data: Buffer): string | null {
  try {
    mkdirSync(SCREENSHOT_CACHE_DIR, { recursive: true });
    const fpath = screenshotPath(url);
    writeFileSync(fpath, data);
    return fpath;
  } catch (err: any) {
    console.log(`    ⚠  Screenshot cache write failed: ${err.message}`);
    return null;
  }
}

/**
 * Load screenshot from disk cache if available.
 * @returns Cached data or null
 */
export function loadCachedScreenshot(url: string): Buffer | null {
  try {
    const fpath = screenshotPath(url);
    if (existsSync(fpath)) {
      return readFileSync(fpath);
    }
  } catch { /* ignore */ }
  return null;
}
