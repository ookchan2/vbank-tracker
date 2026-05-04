/**
 * Bank configuration for 8 HK virtual banks.
 * URLs updated to Copy3 versions (EleBank → elebank.com, expanded Fusion/Ant URLs).
 * @author Alfie
 */

export interface BankConfig {
  id: string;
  name: string;
  color: string;
  urls: string[];
  link: string;
  waitExtra?: number;
  maxRetries?: number;
  domains?: string[];
}

export const BANK_CONFIGS: Record<string, BankConfig> = {
  za: {
    id: 'za',
    name: 'ZA Bank',
    color: '#25CD9C',
    urls: [
      'https://bank.za.group/en/promotion',
      'https://bank.za.group/en',
      'https://bank.za.group/',
      'https://bank.za.group/hk/usstock',
      'https://bank.za.group/hkstock',
      'https://bank.za.group/hk/fund',
      'https://bank.za.group/hk/loan',
      'https://bank.za.group/hk/statement-instalment',
      'https://bank.za.group/hk/open-account-mgm',
      'https://bank.za.group/6th-anniversary-campaign',
    ],
    link: 'https://bank.za.group/en/promotion',
    waitExtra: 4000,
    domains: ['bank.za.group', 'za.group'],
  },
  mox: {
    id: 'mox',
    name: 'Mox Bank',
    color: '#ec4899',
    urls: [
      'https://mox.com/promotions/',
      'https://mox.com/zh/promotions/',
      'https://mox.com/',
      'https://mox.com/zh/promotions/moxsmart/',
      'https://mox.com/zh/promotions/The-Club/',
      'https://mox.com/zh/promotions/1500mox/',
      'https://mox.com/promotions/Personal-Accident-Cushion-Promotion-Jan2026/',
      'https://mox.com/promotions/CLUBLINK/',
      'https://mox.com/promotions/moxtrip25/',
      'https://mox.com/promotions/MOXHKT25/',
      'https://mox.com/zh/promotions/best-in-town-telco/',
      'https://mox.com/promotions/mox-zone-at-the-club-hkt/',
      'https://mox.com/zh/promotions/Mox-Referral-Programme/',
    ],
    link: 'https://mox.com/promotions/',
    waitExtra: 4000,
    maxRetries: 3,
    domains: ['mox.com'],
  },
  livi: {
    id: 'livi',
    name: 'livi bank',
    color: '#f97316',
    urls: [
      'https://www.livibank.com/',
      'https://www.livibank.com/zh_HK/',
    ],
    link: 'https://www.livibank.com/',
    waitExtra: 12000,
    maxRetries: 3,
    domains: ['livibank.com'],
  },
  welab: {
    id: 'welab',
    name: 'WeLab Bank',
    color: '#7c3aed',
    urls: [
      'https://www.welab.bank/en/feature/',
      'https://www.welab.bank/en/',
      'https://www.welab.bank/',
      'https://www.welab.bank/en/feature/2026-wm-april-cash-reward/',
      'https://www.welab.bank/zh/feature/dcp-easter-lucky-draw-2026/',
      'https://www.welab.bank/zh/feature/2-in-1-welcome-rewards-apr26/',
      'https://www.welab.bank/zh/feature/tesla-mega-combo/',
      'https://www.welab.bank/zh/feature/fund/',
      'https://www.welab.bank/en/feature/loan_mgm/',
      'https://www.welab.bank/zh/feature/loan_mgm/',
    ],
    link: 'https://www.welab.bank/',
    waitExtra: 4000,
    domains: ['welab.bank'],
  },
  pao: {
    id: 'pao',
    name: 'PADB',
    color: '#0ea5e9',
    urls: [
      'https://www.pingandb.com/en/',
      'https://www.pingandb.com/tc/',
      'https://www.pingandb.com/tc/money-market-fund.html',
      'https://www.pingandb.com/tc/investment.html',
      'https://www.pingandb.com/tc/stock.html',
    ],
    link: 'https://www.pingandb.com/en/',
    waitExtra: 6000,
    maxRetries: 3,
    domains: ['pingandb.com'],
  },
  airtar: {
    id: 'airtar',
    name: 'EleBank',
    color: '#06b6d4',
    urls: [
      'https://www.elebank.com/en-hk/promotion',
      'https://www.elebank.com/zh-hk/promotion',
      'https://www.elebank.com/zh-hk',
    ],
    link: 'https://www.elebank.com/en-hk/promotion',
    waitExtra: 4000,
    domains: ['elebank.com'],
  },
  fusion: {
    id: 'fusion',
    name: 'Fusion Bank',
    color: '#14b8a6',
    urls: [
      'https://www.fusionbank.com/?lang=en',
      'https://www.fusionbank.com/?lang=zh-HK',
      'https://www.fusionbank.com/common/detail.html?key=fxtd2023&lang=en',
      'https://www.fusionbank.com/common/detail.html?key=fxtd2023&lang=tc',
      'https://www.fusionbank.com/common/detail.html?key=fusionflash&lang=en',
      'https://www.fusionbank.com/common/detail.html?key=fusionflash&lang=tc',
      'https://www.fusionbank.com/common/detail.html?key=savinginterestplus&lang=en',
      'https://www.fusionbank.com/common/detail.html?key=savinginterestplus&lang=tc',
      'https://www.fusionbank.com/common/detail.html?key=mgm_4&lang=en',
      'https://www.fusionbank.com/common/detail.html?key=mgm_4&lang=tc',
    ],
    link: 'https://www.fusionbank.com/?lang=en',
    waitExtra: 7000,
    maxRetries: 3,
    domains: ['fusionbank.com'],
  },
  ant: {
    id: 'ant',
    name: 'Ant Bank',
    color: '#1677ff',
    urls: [
      'https://www.antbank.hk/em-plus-offer?lang=en_us',
      'https://www.antbank.hk/em-plus-offer?lang=zh_hk',
      'https://www.antbank.hk/',
      'https://www.antbank.hk/fund?lang=zh_hk',
      'https://www.antbank.hk/fund?lang=en_us',
    ],
    link: 'https://www.antbank.hk/em-plus-offer?lang=en_us',
    waitExtra: 9000,
    maxRetries: 3,
    domains: ['antbank.hk'],
  },
};

/** Bank display names for email/website */
export const BANK_DISPLAY_NAMES: Record<string, string> = {
  'ZA Bank': 'ZA',
  'EleBank': 'EleBank',
  'Airstar Bank': 'EleBank',
  'Ant Bank': 'Ant',
  'Fusion Bank': 'Fusion',
  'Mox Bank': 'Mox',
  'PADB': 'PADB',
  'PAObank': 'PADB',
  'WeLab Bank': 'WeLab',
  'livi bank': 'Livi',
};

/** Bank colors for email/website */
export const BANK_COLORS: Record<string, string> = {
  'ZA Bank': '#25CD9C',
  'Mox Bank': '#ec4899',
  'WeLab Bank': '#7c3aed',
  'livi bank': '#f97316',
  'PADB': '#0ea5e9',
  'PAObank': '#0ea5e9',
  'EleBank': '#06b6d4',
  'Airstar Bank': '#06b6d4',
  'Fusion Bank': '#14b8a6',
  'Ant Bank': '#1677ff',
};

/** Category metadata for email rendering */
export const CATEGORY_META: Record<string, { bg: string; emoji: string }> = {
  '迎新': { bg: '#10b981', emoji: '🎉' },
  '消費': { bg: '#f59e0b', emoji: '💳' },
  '投資': { bg: '#6366f1', emoji: '📈' },
  '旅遊': { bg: '#06b6d4', emoji: '✈️' },
  '保險': { bg: '#ef4444', emoji: '🛡️' },
  '貸款': { bg: '#dc2626', emoji: '💰' },
  '存款': { bg: '#3b82f6', emoji: '🏦' },
  '外匯': { bg: '#8b5cf6', emoji: '🌐' },
  '推薦': { bg: '#ec4899', emoji: '👥' },
  '新資金': { bg: '#0ea5e9', emoji: '💵' },
  'Others': { bg: '#6b7280', emoji: '📋' },
};

/** Product category metadata for frontend and email rendering */
export const PRODUCT_CATEGORY_META: Record<string, { bg: string; emoji: string; label: string }> = {
  'US Stock':               { bg: '#6366f1', emoji: '🇺🇸', label: 'US Stock' },
  'HK Stock':               { bg: '#e11d48', emoji: '🇭🇰', label: 'HK Stock' },
  'Crypto':                 { bg: '#f59e0b', emoji: '₿',  label: 'Crypto' },
  'Fund':                   { bg: '#8b5cf6', emoji: '📊', label: 'Fund' },
  'Credit Card':            { bg: '#ec4899', emoji: '💳', label: 'Credit Card' },
  'Saving/Current Deposit': { bg: '#3b82f6', emoji: '🏦', label: 'Savings' },
  'Time Deposit':           { bg: '#0ea5e9', emoji: '⏳', label: 'Time Deposit' },
  'Currency Exchange':      { bg: '#14b8a6', emoji: '💱', label: 'FX' },
  'Personal Loan':          { bg: '#ef4444', emoji: '💰', label: 'Loan' },
  'Others':                 { bg: '#6b7280', emoji: '📋', label: 'Others' },
};
