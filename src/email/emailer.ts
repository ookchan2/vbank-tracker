/**
 * Email builder + sender for VBank Tracker daily report.
 * Ported from Python vbank-tracker emailer.py.
 * Enhanced with Copy3's AI-unavailability banner and prefixed subject.
 * @author Alfie
 */

import nodemailer from 'nodemailer';
import fs from 'fs';
import path from 'path';
import { BANK_COLORS, BANK_DISPLAY_NAMES, CATEGORY_META, PRODUCT_CATEGORY_META } from '../config/banks.js';
import { hktNow } from '../utils/hkt.js';

// ── Category / bank helpers ────────────────────────────────────────────────

function bankColor(bankName: string): string {
  const nameLower = (bankName || '').toLowerCase().trim();
  const generic = ['bank', 'banking', 'digital', 'virtual', 'bank hk', ''];
  if (generic.includes(nameLower)) return '#6b7280';
  for (const [key, color] of Object.entries(BANK_COLORS)) {
    if (key.toLowerCase() === nameLower) return color;
  }
  for (const [key, color] of Object.entries(BANK_COLORS)) {
    const k = key.toLowerCase();
    if (k.includes(nameLower) || nameLower.includes(k)) return color;
  }
  return '#6b7280';
}

function bankDisplayName(bankName: string): string {
  const nameLower = (bankName || '').toLowerCase().trim();
  const generic = ['bank', 'banking', 'digital', 'virtual', 'bank hk', ''];
  if (generic.includes(nameLower)) return bankName;
  for (const [key, short] of Object.entries(BANK_DISPLAY_NAMES)) {
    if (key.toLowerCase() === nameLower) return short;
  }
  for (const [key, short] of Object.entries(BANK_DISPLAY_NAMES)) {
    const k = key.toLowerCase();
    if (k.includes(nameLower) || nameLower.includes(k)) return short;
  }
  return bankName;
}

function getCatMeta(typeStr: string): { bg: string; emoji: string } {
  if (CATEGORY_META[typeStr]) return CATEGORY_META[typeStr];
  const t = (typeStr || '').toLowerCase();
  if (['welcome', 'new customer', 'onboard', '迎新'].some(k => t.includes(k))) return CATEGORY_META['迎新'];
  if (['spend', 'cashback', 'card', '消費'].some(k => t.includes(k))) return CATEGORY_META['消費'];
  if (['invest', 'stock', 'fund', 'crypto', '投資'].some(k => t.includes(k))) return CATEGORY_META['投資'];
  if (['travel', 'flight', 'hotel', 'mile', '旅遊'].some(k => t.includes(k))) return CATEGORY_META['旅遊'];
  if (['insur', '保險'].some(k => t.includes(k))) return CATEGORY_META['保險'];
  if (['loan', 'borrow', '貸款'].some(k => t.includes(k))) return CATEGORY_META['貸款'];
  if (['deposit', 'saving', 'time deposit', '存款'].some(k => t.includes(k))) return CATEGORY_META['存款'];
  if (['fx', 'currency', 'exchange', 'remit', '外匯'].some(k => t.includes(k))) return CATEGORY_META['外匯'];
  if (['refer', '推薦'].some(k => t.includes(k))) return CATEGORY_META['推薦'];
  if (['new fund', 'fresh', '新資金'].some(k => t.includes(k))) return CATEGORY_META['新資金'];
  return CATEGORY_META['Others'];
}

function catTag(text: string): string {
  const meta = getCatMeta(text);
  return `<span style="display:inline-block;padding:3px 10px;margin:2px 3px 2px 0;` +
    `border-radius:20px;font-size:11px;color:#fff;font-weight:700;` +
    `background:${meta.bg};">${meta.emoji} ${text}</span>`;
}

function typesToList(typesRaw: any): string[] {
  if (Array.isArray(typesRaw)) return typesRaw.map(String).map(t => t.trim()).filter(Boolean);
  if (typeof typesRaw === 'string') return typesRaw.split(',').map(t => t.trim()).filter(Boolean);
  return [];
}

// ── Classification ──────────────────────────────────────────────────────────

function classifyPromo(p: Record<string, any>, todayD: Date, thresholdD: Date): 'past' | 'expiring' | 'active' {
  if (p.active != null && !p.active) return 'past';
  const ed = p.end_date;
  if (ed) {
    try {
      const endD = new Date(String(ed).slice(0, 10) + 'T00:00:00');
      if (endD < todayD) return 'past';
      if (endD <= thresholdD) return 'expiring';
    } catch { /* ignore */ }
  }
  return 'active';
}

function hktTodayDate(): Date {
  const now = new Date();
  const hktMs = now.getTime() + 8 * 3600000;
  const d = new Date(hktMs);
  return new Date(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

// ── Canonical count source ──────────────────────────────────────────────────

function resolveCountSource(
  promotionsData: Record<string, any>[],
  scrapedData: Record<string, any> | null,
): Record<string, any>[] {
  if (scrapedData && typeof scrapedData === 'object') {
    const jsonPromos = scrapedData.promotions;
    if (jsonPromos && Array.isArray(jsonPromos)) return jsonPromos;
  }
  return promotionsData || [];
}

// ── Recipients ──────────────────────────────────────────────────────────────

function collectRecipients(override?: string | string[] | null): string[] {
  const rawEmails: string[] = [];
  if (override) {
    const list = Array.isArray(override) ? override : [override];
    for (const item of list) rawEmails.push(...item.split(','));
  }
  for (const envVar of ['RECIPIENT_EMAIL', 'RECIPIENT_EMAIL_2', 'RECIPIENT_EMAIL_3', 'EMAIL_RECIPIENT', 'EMAIL_TO']) {
    const val = (process.env[envVar] || '').trim();
    if (val) rawEmails.push(...val.split(','));
  }
  const seen = new Set<string>();
  const result: string[] = [];
  for (const e of rawEmails) {
    const trimmed = e.trim();
    if (trimmed && !seen.has(trimmed)) {
      seen.add(trimmed);
      result.push(trimmed);
    }
  }
  return result;
}

// ── Promotion card HTML ─────────────────────────────────────────────────────

function promoCard(promo: Record<string, any>): string {
  const bankName = promo.bName || promo.bank_name || promo.bank || 'Unknown';
  const display = bankDisplayName(bankName);
  const color = bankColor(bankName);
  const title = (promo.title || promo.name || 'Untitled').slice(0, 120);
  const highlight = promo.highlight || promo.description || '';
  const period = promo.period || promo.validity || 'Ongoing';
  const quota = promo.quota || '';
  const cost = promo.cost || '';
  const tcLink = promo.tc_link || promo.url || promo.link || '';
  const typesRaw = promo.types || promo.type || promo.promo_type || '';
  const typeList = typesToList(typesRaw).slice(0, 4);
  const catTags = typeList.length ? typeList.map(t => catTag(t)).join('') : catTag('Others');

  let metaRows = '';
  if (quota) {
    metaRows += `<tr><td style="padding:6px 0 2px;">
      <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:.07em;margin-bottom:3px;">👥 Quota / Eligibility</div>
      <div style="font-size:13px;color:#374151;">${quota}</div>
    </td></tr>`;
  }
  if (cost) {
    metaRows += `<tr><td style="padding:6px 0 2px;">
      <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:.07em;margin-bottom:3px;">💲 Cost / Min Spend</div>
      <div style="font-size:13px;color:#374151;">${cost}</div>
    </td></tr>`;
  }

  let sourceBtn = '';
  if (tcLink) {
    sourceBtn = `<tr><td style="padding:12px 0 0;">
      <a href="${tcLink}" style="display:inline-block;padding:8px 20px;background:#6366f1;color:#ffffff;border-radius:8px;font-size:12px;font-weight:700;text-decoration:none;letter-spacing:.02em;">🔗 View Official Source ↗</a>
    </td></tr>`;
  }

  return `<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:18px;border-radius:14px;overflow:hidden;border:1px solid #e5e7eb;box-shadow:0 3px 10px rgba(0,0,0,0.08);">
  <tr><td bgcolor="${color}" style="background:${color};padding:13px 18px;">
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="vertical-align:middle;"><span style="font-weight:900;font-size:17px;color:#ffffff;">${display}</span></td>
      <td style="text-align:right;vertical-align:middle;"><span style="background:rgba(255,255,255,0.22);color:#ffffff;padding:3px 12px;border-radius:20px;font-size:11px;font-weight:600;">📅 ${period}</span></td>
    </tr></table>
  </td></tr>
  <tr><td style="background:#ffffff;padding:16px 18px;">
    <div style="margin-bottom:10px;">${catTags}</div>
    <div style="font-weight:800;font-size:15px;color:#1f2937;line-height:1.4;margin-bottom:10px;">${title}</div>
    <div style="font-size:13px;color:#4b5563;line-height:1.7;background:#f9fafb;border-radius:8px;padding:10px 14px;margin-bottom:12px;border-left:3px solid ${color};">${highlight}</div>
    <table width="100%" cellpadding="0" cellspacing="0" style="border-top:1px solid #f3f4f6;">${metaRows}${sourceBtn}</table>
  </td></tr>
</table>`;
}

// ── Product card HTML ────────────────────────────────────────────────────────

function productCard(product: Record<string, any>): string {
  const bankName = product.bank_name || 'Unknown';
  const display = bankDisplayName(bankName);
  const color = bankColor(bankName);
  const productName = (product.product_name || 'Untitled').slice(0, 100);
  const category = product.category || 'Others';
  const catMeta = PRODUCT_CATEGORY_META[category] || PRODUCT_CATEGORY_META['Others'];
  const description = product.description || '';
  const interestRate = product.interest_rate || 'N/A';
  const fees = product.fees || 'N/A';
  const minDeposit = product.min_deposit || 'None';
  const features = product.features || [];
  const zaComparison = product.za_comparison;

  let featureList = '';
  if (features.length) {
    const feats = features.slice(0, 4).map((f: string) => `<li style="margin:3px 0;font-size:12px;color:#4b5563;">${f}</li>`).join('');
    featureList = `<ul style="margin:8px 0;padding-left:18px;">${feats}</ul>`;
  }

  let zaSection = '';
  if (zaComparison && typeof zaComparison === 'object') {
    const pros = (zaComparison.pros_vs_za || []).slice(0, 3);
    const cons = (zaComparison.cons_vs_za || []).slice(0, 3);
    const verdict = zaComparison.verdict || '';

    let prosHtml = pros.map((p: string) => `<li style="margin:2px 0;font-size:11px;color:#059669;">▲ ${p}</li>`).join('');let consHtml = cons.map((c: string) => `<li style="margin:2px 0;font-size:11px;color:#dc2626;">▼ ${c}</li>`).join('');

    if (prosHtml || consHtml) {
      zaSection = `<tr><td style="padding:10px 0 0;border-top:1px solid #f3f4f6;">
        <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;margin-bottom:6px;">vs ZA Bank</div>
        <ul style="margin:0;padding-left:14px;">${prosHtml}${consHtml}</ul>
        ${verdict ? `<div style="font-size:11px;color:#6b7280;margin-top:6px;font-style:italic;">${verdict}</div>` : ''}
      </td></tr>`;
    }
  }

  return `<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:14px;border-radius:12px;overflow:hidden;border:1px solid #e5e7eb;box-shadow:0 2px 8px rgba(0,0,0,0.06);">
  <tr><td bgcolor="${color}" style="background:${color};padding:10px 14px;">
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="vertical-align:middle;"><span style="font-weight:800;font-size:14px;color:#ffffff;">${display}</span></td>
      <td style="text-align:right;vertical-align:middle;">
        <span style="background:${catMeta.bg};color:#fff;padding:2px 10px;border-radius:12px;font-size:10px;font-weight:700;">${catMeta.emoji} ${catMeta.label}</span>
      </td>
    </tr></table>
  </td></tr>
  <tr><td style="background:#ffffff;padding:14px;">
    <div style="font-weight:800;font-size:14px;color:#1f2937;margin-bottom:6px;">${productName}</div>
    ${description ? `<div style="font-size:12px;color:#6b7280;line-height:1.5;margin-bottom:8px;">${description.slice(0, 150)}</div>` : ''}
    <table width="100%" cellpadding="0" cellspacing="0" style="font-size:11px;color:#4b5563;">
      <tr>
        <td style="padding:4px 8px 4px 0;"><span style="color:#9ca3af;">Rate:</span> <b>${interestRate}</b></td>
        <td style="padding:4px 8px;"><span style="color:#9ca3af;">Fees:</span> <b>${fees}</b></td>
        <td style="padding:4px 0;"><span style="color:#9ca3af;">Min:</span> <b>${minDeposit}</b></td>
      </tr>
    </table>
    ${featureList}
    ${zaSection}
  </td></tr>
</table>`;
}

// ── Section builder ─────────────────────────────────────────────────────────

function sectionHtml(
  promos: Record<string, any>[],
  heading: string,
  subHeading: string,
  icon: string,
  headerColor: string,
  headerDark: string,
  emptyMsg: string,
  countLabel: string,
  skipIfEmpty: boolean = false,
): string {
  if (!promos.length) {
    if (skipIfEmpty) return '';
    return `<tr><td style="height:16px;"></td></tr>
<tr><td style="background:#f9fafb;border-radius:14px;padding:22px 20px;border:1px dashed #e5e7eb;text-align:center;">
  <div style="font-size:28px;margin-bottom:8px;">🔍</div>
  <div style="font-size:13px;font-weight:700;color:#6b7280;">${emptyMsg}</div>
</td></tr>`;
  }

  const cards = promos.map(p => promoCard(p)).join('');
  const count = promos.length;
  const label = countLabel.replace('{count}', String(count)).replace('{s}', count === 1 ? '' : 's');

  return `<tr><td style="height:20px;"></td></tr>
<tr><td style="background:#ffffff;border-radius:16px;padding:24px;box-shadow:0 2px 8px rgba(0,0,0,0.07);">
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:22px;">
    <tr><td bgcolor="${headerDark}" style="background-color:${headerDark};background:${headerColor};border-radius:12px;padding:16px 22px;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td style="vertical-align:middle;">
          <span style="font-size:22px;vertical-align:middle;">${icon}</span>
          <span style="font-weight:900;font-size:17px;color:#1f2937;vertical-align:middle;margin-left:10px;">${heading}</span>
          <div style="font-size:11px;color:rgba(0,0,0,0.45);margin-top:3px;margin-left:34px;">${subHeading}</div>
        </td>
        <td style="text-align:right;vertical-align:middle;white-space:nowrap;">
          <span style="background:rgba(0,0,0,0.15);color:#1f2937;padding:4px 14px;border-radius:20px;font-size:12px;font-weight:700;">${label}</span>
        </td>
      </tr></table>
    </td></tr>
  </table>
  ${cards}
</td></tr>`;
}

// ── Plain text builder ──────────────────────────────────────────────────────

function buildPlainText(
  promotionsData: Record<string, any>[],
  newPromos: Record<string, any>[],
  newPromosWeek: Record<string, any>[],
  aiUnavailable: boolean = false,
  countSource?: Record<string, any>[],
  // Product data
  newProducts?: Record<string, any>[],
  newProductsWeek?: Record<string, any>[],
  allProducts?: Record<string, any>[],
): string {
  const src = countSource || promotionsData || [];
  const nonBau = src.filter(p => !p.is_bau);

  const todayD = hktTodayDate();
  const thresholdD = new Date(todayD.getTime() + 30 * 86400000);

  let expCount = 0;
  let pastCount = 0;
  for (const p of nonBau) {
    const status = classifyPromo(p, todayD, thresholdD);
    if (status === 'past') pastCount++;
    else if (status === 'expiring') expCount++;
  }

  const totalShown = nonBau.length - pastCount;
  const activeCount = totalShown - expCount;

  const dateOnly = hktNow().split(' ')[0];
  const lines = [
    `VBank Tracker Daily Report — ${dateOnly}`,
    '='.repeat(50),
  ];

  if (aiUnavailable) {
    lines.push('', '⚠️  NOTICE: AI extraction was unavailable today.',
      '    Data shown is from the last successful AI run (cached).',
      '    Promotions may not reflect today\'s latest changes.');
  }

  // Section 1: New promotions today
  const newShow = (newPromos || []).filter(p => !p.is_bau);
  if (newShow.length) {
    lines.push('', `NEWLY LAUNCHED TODAY (${newShow.length}):`);
    for (const p of newShow) {
      const bank = p.bName || p.bank_name || '?';
      const title = p.title || p.name || '?';
      const tc = p.tc_link || p.url || '';
      lines.push(`  [${bank}] ${title}`);
      if (p.period) lines.push(`    Period      : ${p.period}`);
      if (p.quota) lines.push(`    Eligibility : ${p.quota}`);
      if (tc) lines.push(`    Source      : ${tc}`);
    }
    lines.push('');
  }

  // Section 2: New promotions this week
  const weekShow = (newPromosWeek || []).filter(p => !p.is_bau);
  if (weekShow.length) {
    lines.push(`PROMOTION NEWLY LAUNCHED WITHIN THIS WEEK — PAST 6 DAYS (${weekShow.length}):`);
    for (const p of weekShow) {
      const bank = p.bName || p.bank_name || '?';
      const title = p.title || p.name || '?';
      const tc = p.tc_link || p.url || '';
      lines.push(`  [${bank}] ${title}`);
      if (tc) lines.push(`    Source : ${tc}`);
    }
    lines.push('');
  } else if (!newShow.length) {
    lines.push('', 'PROMOTION NEWLY LAUNCHED WITHIN THIS WEEK — PAST 6 DAYS: None', '');
  }

  // Section 3: Total non-BAU promotion number
  lines.push(
    `TOTAL PROMOTIONS : ${totalShown}`,
    `ACTIVE           : ${activeCount}`,
    `EXPIRING SOON    : ${expCount}  (within 30 days)`,
    '',
  );

  // Section 4: New products today
  const newProds = newProducts || [];
  if (newProds.length) {
    lines.push(`NEW PRODUCTS TODAY (${newProds.length}):`);
    for (const p of newProds) {
      const bank = p.bank_name || '?';
      const name = p.product_name || '?';
      const cat = p.category || 'Others';
      lines.push(`  [${bank}] ${name} (${cat})`);
    }
    lines.push('');
  }

  // Section 5: New products this week
  const weekProds = newProductsWeek || [];
  if (weekProds.length) {
    lines.push(`NEW PRODUCTS THIS WEEK (${weekProds.length}):`);
    for (const p of weekProds) {
      const bank = p.bank_name || '?';
      const name = p.product_name || '?';
      lines.push(`  [${bank}] ${name}`);
    }
    lines.push('');
  }

  // Section 6: Total product number
  const totalProducts = (allProducts || []).length;
  lines.push(
    `TOTAL PRODUCTS : ${totalProducts}`,
    '',
  );

  lines.push(
    '—',
    'VBank Tracker • Auto-generated daily at 09:00 HKT',
    'Data sourced from official bank websites only.',
    'For full strategic insights visit the web dashboard.',
  );

  return lines.join('\n');
}

// ── Main HTML builder ───────────────────────────────────────────────────────

export function buildHtmlEmail(
  promotionsData: Record<string, any>[],
  scrapedData: Record<string, any> | null,
  strategicInsights?: Record<string, any> | null,
  newPromos?: Record<string, any>[],
  newPromosWeek?: Record<string, any>[],
  aiUnavailable: boolean = false,
  // Product data for 6-section email
  newProducts?: Record<string, any>[],
  newProductsWeek?: Record<string, any>[],
  allProducts?: Record<string, any>[],
): string {
  newPromos = newPromos || [];
  newPromosWeek = newPromosWeek || [];
  newProducts = newProducts || [];
  newProductsWeek = newProductsWeek || [];
  allProducts = allProducts || [];

  const dateOnly = hktNow().split(' ')[0];
  const countList = resolveCountSource(promotionsData, scrapedData);

  const nonBauData = countList.filter(p => !p.is_bau);
  const newPromosShow = newPromos.filter(p => !p.is_bau);
  const newPromosWkShow = newPromosWeek.filter(p => !p.is_bau);

  const todayD = hktTodayDate();
  const thresholdD = new Date(todayD.getTime() + 30 * 86400000);

  let expiringCount = 0;
  let pastEndCount = 0;
  for (const p of nonBauData) {
    const status = classifyPromo(p, todayD, thresholdD);
    if (status === 'past') pastEndCount++;
    else if (status === 'expiring') expiringCount++;
  }

  const totalPromos = nonBauData.length - pastEndCount;
  const activeCount = totalPromos - expiringCount;

  let aiNoticeHtml = '';
  if (aiUnavailable) {
    aiNoticeHtml = `<tr><td style="height:16px;"></td></tr>
<tr><td style="background:#fffbeb;border-radius:12px;padding:16px 20px;border:1px solid #fcd34d;border-left:4px solid #f59e0b;">
  <table width="100%" cellpadding="0" cellspacing="0"><tr>
    <td style="vertical-align:middle;width:36px;font-size:24px;">⚠️</td>
    <td style="vertical-align:middle;">
      <div style="font-size:13px;font-weight:800;color:#92400e;margin-bottom:3px;">AI Extraction Unavailable Today — Showing Cached Data</div>
      <div style="font-size:12px;color:#b45309;line-height:1.5;">The AI agent was not available during this run, so no new promotions were extracted or classified. The data shown below reflects the last successful AI run. Promotions may not include today's latest changes.</div>
    </td>
  </tr></table>
</td></tr>`;
  }

  // Section 1: New promotions today
  const todaySection = sectionHtml(
    newPromosShow, 'Newly Launched Today',
    '今日新推出優惠 · first_seen_at = today (HKT) · active only',
    '🆕', 'linear-gradient(135deg,#ff6b35 0%,#f7931e 100%)', '#f97316',
    'No new promotions today', '{count} new promotion{s}', true,
  );

  // Section 2: New promotions this week
  const weekSection = sectionHtml(
    newPromosWkShow, 'Promotion newly launched within this week',
    '本週新推出優惠 · first_seen_at in past 6 days (excl. today, HKT) · active only',
    '📅', 'linear-gradient(135deg,#6366f1 0%,#8b5cf6 100%)', '#6366f1',
    'No new promotions in the past 6 days', '{count} newly launched this week',
  );

  // Section 3: Total non-BAU promotion number
  const promoStatsSection = `<tr><td style="height:20px;"></td></tr>
<tr><td style="background:#ffffff;border-radius:14px;box-shadow:0 2px 8px rgba(0,0,0,0.07);">
  <table width="100%" cellpadding="0" cellspacing="0"><tr>
    <td width="33%" style="text-align:center;padding:24px 10px;border-right:1px solid #f3f4f6;">
      <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;">Total</div>
      <div style="font-size:38px;font-weight:900;color:#6366f1;line-height:1;">${totalPromos}</div>
      <div style="font-size:11px;color:#c4cad4;margin-top:5px;">non-BAU running</div>
    </td>
    <td width="33%" style="text-align:center;padding:24px 10px;border-right:1px solid #f3f4f6;">
      <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;">Active Promos</div>
      <div style="font-size:38px;font-weight:900;color:#10b981;line-height:1;">${activeCount}</div>
      <div style="font-size:11px;color:#c4cad4;margin-top:5px;">currently active</div>
    </td>
    <td width="33%" style="text-align:center;padding:24px 10px;">
      <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;">Expiring Soon</div>
      <div style="font-size:38px;font-weight:900;color:#f59e0b;line-height:1;">${expiringCount}</div>
      <div style="font-size:11px;color:#c4cad4;margin-top:5px;">within 30 days</div>
    </td>
  </tr></table>
</td></tr>`;

  // Section 4: New products today
  let newProductsSection = '';
  if (newProducts.length) {
    const cards = newProducts.map(p => productCard(p)).join('');
    newProductsSection = `<tr><td style="height:20px;"></td></tr>
<tr><td style="background:#ffffff;border-radius:16px;padding:24px;box-shadow:0 2px 8px rgba(0,0,0,0.07);">
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:22px;">
    <tr><td bgcolor="#10b981" style="background:#10b981;border-radius:12px;padding:16px 22px;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td style="vertical-align:middle;">
          <span style="font-size:22px;vertical-align:middle;">📦</span>
          <span style="font-weight:900;font-size:17px;color:#ffffff;vertical-align:middle;margin-left:10px;">New Products Today</span>
          <div style="font-size:11px;color:rgba(255,255,255,0.7);margin-top:3px;margin-left:34px;">今日新推出產品</div>
        </td>
        <td style="text-align:right;vertical-align:middle;white-space:nowrap;">
          <span style="background:rgba(255,255,255,0.2);color:#ffffff;padding:4px 14px;border-radius:20px;font-size:12px;font-weight:700;">${newProducts.length} new product${newProducts.length === 1 ? '' : 's'}</span>
        </td>
      </tr></table>
    </td></tr>
  </table>
  ${cards}
</td></tr>`;
  }

  // Section 5: New products this week
  let weekProductsSection = '';
  if (newProductsWeek.length) {
    const cards = newProductsWeek.map(p => productCard(p)).join('');
    weekProductsSection = `<tr><td style="height:20px;"></td></tr>
<tr><td style="background:#ffffff;border-radius:16px;padding:24px;box-shadow:0 2px 8px rgba(0,0,0,0.07);">
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:22px;">
    <tr><td bgcolor="#3b82f6" style="background:#3b82f6;border-radius:12px;padding:16px 22px;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td style="vertical-align:middle;">
          <span style="font-size:22px;vertical-align:middle;">🗓️</span>
          <span style="font-weight:900;font-size:17px;color:#ffffff;vertical-align:middle;margin-left:10px;">New Products This Week</span>
          <div style="font-size:11px;color:rgba(255,255,255,0.7);margin-top:3px;margin-left:34px;">本週新推出產品</div>
        </td>
        <td style="text-align:right;vertical-align:middle;white-space:nowrap;">
          <span style="background:rgba(255,255,255,0.2);color:#ffffff;padding:4px 14px;border-radius:20px;font-size:12px;font-weight:700;">${newProductsWeek.length} new this week</span>
        </td>
      </tr></table>
    </td></tr>
  </table>
  ${cards}
</td></tr>`;
  }

  // Section 6: Total product number
  const productCount = allProducts.length;
  const productStatsSection = `<tr><td style="height:20px;"></td></tr>
<tr><td style="background:#ffffff;border-radius:14px;padding:24px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,0.07);">
  <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;">Total Products Tracked</div>
  <div style="font-size:48px;font-weight:900;color:#10b981;line-height:1;">${productCount}</div>
  <div style="font-size:12px;color:#6b7280;margin-top:6px;">across 8 HK virtual banks</div>
</td></tr>`;

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>VBank Daily Report</title>
</head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0">
<tr><td align="center" style="padding:28px 12px;">
<table width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;">

  <!-- HEADER -->
  <tr><td style="background:#ffffff;border-radius:18px;padding:32px 28px;text-align:center;border:2px solid #e5e7eb;box-shadow:0 4px 16px rgba(0,0,0,0.08);">
    <div style="font-size:44px;margin-bottom:12px;">🏦</div>
    <div style="font-size:26px;font-weight:900;color:#111827;letter-spacing:-.5px;line-height:1.2;">VBank Tracker</div>
    <div style="font-size:12px;font-weight:700;color:#6b7280;margin-top:6px;letter-spacing:1.2px;text-transform:uppercase;">Daily Promotions & Products Report</div>
    <div style="display:inline-block;margin-top:14px;padding:6px 20px;background:#f3f4f6;border-radius:20px;font-size:13px;color:#374151;font-weight:700;border:1px solid #e5e7eb;">📅 ${dateOnly}</div>
  </td></tr>
  <tr><td style="height:20px;"></td></tr>

  ${aiNoticeHtml}

  <!-- Section 1: New promotions today -->
  ${todaySection}

  <!-- Section 2: New promotions this week -->
  ${weekSection}

  <!-- Section 3: Total non-BAU promotion number -->
  ${promoStatsSection}

  <!-- Section 4: New products today -->
  ${newProductsSection}

  <!-- Section 5: New products this week -->
  ${weekProductsSection}

  <!-- Section 6: Total product number -->
  ${productStatsSection}

  <!-- FOOTER -->
  <tr><td style="height:16px;"></td></tr>
  <tr><td style="text-align:center;padding:16px 12px;">
    <div style="font-size:12px;color:#9ca3af;line-height:1.8;">
      VBank Tracker &nbsp;·&nbsp; Auto-generated daily at 09:00 HKT<br>
      Data sourced from official bank websites only<br>
      <span style="font-size:11px;color:#c4cad4;">For strategic insights &amp; full analysis, visit the web dashboard</span>
    </div>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>`;
}

// ── Send email ──────────────────────────────────────────────────────────────

const SMTP_MAX_RETRIES = 3;

export async function sendEmail(
  htmlContent: string,
  subject?: string,
  recipient?: string | string[] | null,
  newPromos?: Record<string, any>[],
  newPromosWeek?: Record<string, any>[],
  promotionsData?: Record<string, any>[],
  aiUnavailable: boolean = false,
  scrapedData?: Record<string, any> | null,
  // Product data
  newProducts?: Record<string, any>[],
  newProductsWeek?: Record<string, any>[],
  allProducts?: Record<string, any>[],
): Promise<boolean> {
  const smtpHost = process.env.SMTP_HOST || 'smtp.gmail.com';
  const smtpPort = parseInt(process.env.SMTP_PORT || '587', 10);

  const smtpUser = process.env.GMAIL_ADDRESS || process.env.SMTP_USER || process.env.EMAIL_FROM || '';
  const smtpPass = process.env.GMAIL_APP_PASSWORD || process.env.SMTP_PASS || process.env.EMAIL_PASS || '';

  const allRecipients = collectRecipients(recipient);

  if (!smtpUser || !smtpPass) {
    const missing = [
      !smtpUser ? 'GMAIL_ADDRESS' : '',
      !smtpPass ? 'GMAIL_APP_PASSWORD' : '',
    ].filter(Boolean);
    console.log(`❌ Missing SMTP credentials: ${missing.join(', ')}`);
    return false;
  }

  if (!allRecipients.length) {
    console.log('❌ No recipient emails configured. Set RECIPIENT_EMAIL env var.');
    return false;
  }

  if (!subject) {
    const dateStr = hktNow().split(' ')[0];
    const base = `🏦 VBank Daily Report — ${dateStr}`;
    subject = aiUnavailable ? `${base} [Cached Data — AI Unavailable]` : base;
  }

  const countSource = resolveCountSource(promotionsData || [], scrapedData || null);
  const plainText = buildPlainText(
    promotionsData || [],
    newPromos || [],
    newPromosWeek || [],
    aiUnavailable,
    countSource,
    newProducts || [],
    newProductsWeek || [],
    allProducts || [],
  );

  let successCount = 0;
  for (const emailTo of allRecipients) {
    for (let attempt = 1; attempt <= SMTP_MAX_RETRIES; attempt++) {
      try {
        const transporter = nodemailer.createTransport({
          host: smtpHost,
          port: smtpPort,
          secure: false,
          auth: { user: smtpUser, pass: smtpPass },
        });

        await transporter.sendMail({
          from: smtpUser,
          to: emailTo,
          subject,
          text: plainText,
          html: htmlContent,
        });

        console.log(`  ✅ Email sent → ${emailTo}`);
        successCount++;
        break;
      } catch (err: any) {
        if (attempt < SMTP_MAX_RETRIES) {
          const wait = Math.pow(2, attempt);
          console.log(`  ⚠️  SMTP attempt ${attempt} failed for ${emailTo}: ${err.message} — retrying in ${wait}s`);
          await new Promise(r => setTimeout(r, wait * 1000));
        } else {
          console.log(`  ❌ Email send failed for ${emailTo} after ${SMTP_MAX_RETRIES} attempts: ${err.message}`);
        }
      }
    }
  }

  if (successCount === allRecipients.length) {
    console.log(`✅ All emails sent → ${allRecipients.length} recipient(s): ${allRecipients.join(', ')}`);
    return true;
  } else if (successCount > 0) {
    console.log(`⚠️  Email partial: ${successCount}/${allRecipients.length} recipients received it`);
    return true;
  } else {
    console.log(`❌ Email failed for all ${allRecipients.length} recipient(s)`);
    return false;
  }
}

/** Save HTML to file for preview */
export function saveHtmlFallback(html: string, filePath: string): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, html, 'utf-8');
  console.log(`  📄 HTML saved → ${filePath}`);
}
