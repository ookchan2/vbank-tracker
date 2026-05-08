/**
 * Content filters and bank name canonicalization utilities.
 * Merges reference patterns with Copy3's expanded non-bank filtering.
 * @author Alfie
 */

// ── Non-bank content guard ──────────────────────────────────────────────────

const NON_BANK_PATTERNS: string[] = [
  'taipofire.gov.hk',
  'taxdeduction.html',
  'hab033',
  'cefs.gov.hk',
  'wang fuk court',
  'wangfuk',
  '宏福苑',
  'support fund for wang fuk',
  'support fund for',
  '大埔宏福苑',
  '援助基金',
  'tai po fire',
  'inland revenue',
  'ird.gov.hk',
  'gov.hk/taxdeduction',
  'tax deduction for donation',
  'tax deduction arrangement',
  'donation receipt',
  'donation acknowledgement',
  'letter of appreciation',
  'government sincerely thanks',
  '政府衷心感謝',
  '捐款致謝',
  '稅務扣除安排',
  'charity donation',
  'relief fund',
  'disaster relief',
  'taipofire',
  'fire support',
  'fire.gov',
  'approved charitable donation',
  'inland revenue ordinance',
  'inland revenue department',
  'bank of china (hong kong) account number 012-875',
  '大埔',
  '捐款收據',
  '慈善捐款',
  // Scam alerts and security notices are NOT promotions
  'beware of scam',
  'beware of bogus',
  'bogus call',
  'scam alert',
  'fraud alert',
  'security alert',
  '防騙',
  '詐騙',
  '提防騙案',
  '假冒',
  '偽冒',
  // Service information is NOT a promotion
  '24x7 service',
  '24x7 banking',
  '24/7 service',
  'customer service hour',
  'service hour',
  'branch hour',
  'opening hour',
  'business hour',
];

const NON_BANK_DOMAINS: string[] = [
  'gov.hk',
  'ird.gov.hk',
  'taipofire.gov.hk',
  'taipofire',
  'cefs.gov.hk',
  'police.gov.hk',
  'welfare.gov.hk',
  'charities',
  'redcross',
];

const NON_BANK_COMPOUND: [string, string][] = [
  ['大埔', '捐款'],
  ['大埔', 'donation'],
  ['大埔', 'relief'],
  ['大埔', '援助'],
];

/**
 * Check if a promotion is non-bank content.
 * Checks title, description, highlight, and tc_link against patterns and domains.
 * @returns true if the content should be filtered out
 */
export function isNonBankContent(
  title: string,
  highlight: string = '',
  description: string = '',
  tcLink: string = '',
): boolean {
  const combined = `${title || ''} ${highlight || ''} ${description || ''} ${tcLink || ''}`.toLowerCase();

  for (const pat of NON_BANK_PATTERNS) {
    if (pat === '大埔') continue; // skip standalone 大埔
    if (combined.includes(pat.toLowerCase())) return true;
  }

  for (const [p1, p2] of NON_BANK_COMPOUND) {
    if (combined.includes(p1.toLowerCase()) && combined.includes(p2.toLowerCase())) {
      return true;
    }
  }

  // Check tc_link against non-bank domains
  const linkLower = (tcLink || '').toLowerCase();
  for (const d of NON_BANK_DOMAINS) {
    if (linkLower.includes(d.toLowerCase())) return true;
  }

  return false;
}

// ── Bank name canonicalization ──────────────────────────────────────────────

const BANK_NAME_CANONICAL: Record<string, string> = {
  'airstar bank': 'EleBank',
  'airstar': 'EleBank',
  'paobank': 'PADB',
  'pao bank': 'PADB',
  'paob': 'PADB',
};

/** Maps canonical name to all legacy aliases stored in DB */
export const BANK_NAME_LEGACY_ALIASES: Record<string, string[]> = {
  'EleBank': ['Airstar Bank', 'Airstar'],
  'PADB': ['PAObank', 'PAO Bank', 'PAOB'],
};

/**
 * Normalize a bank name to its canonical display name.
 * @param raw - Raw bank name from DB or scraper
 * @returns Canonical display name
 */
export function canonicalBankName(raw: string): string {
  if (!raw) return raw;
  return BANK_NAME_CANONICAL[raw.trim().toLowerCase()] || raw.trim();
}
