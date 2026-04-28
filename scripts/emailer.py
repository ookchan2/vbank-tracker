# scripts/emailer.py
#
# DATA SOURCE CONTRACT — read this before modifying stats logic:
#
#   new_promos      -> database.get_new_promotions_today()
#   new_promos_week -> database.get_new_promotions_last_n_days(days=6)
#   scraped_data    -> the full data.json dict  ← ★ REQUIRED for correct stats
#
# ROOT CAUSE of email 52/45/7 vs website 47/41/6:
#   promotions_data (raw DB) contains stale rows whose active flag was never
#   set to False by the database writer even though the AI extraction removed
#   or expired them in data.json.  _classify_promo correctly handles active=False
#   but cannot expire rows that have active=True and no end_date in the DB.
#
# FIX: build_html_email and _build_plain_text now use
#   scraped_data['promotions']  (= data.json content, same source as the website)
#   as the canonical list for ALL stat computation.
#   promotions_data is kept for backward-compatibility but is only used as a
#   fallback when scraped_data has no 'promotions' key.
#
# CALLER UPDATE REQUIRED:
#   Pass scraped_data=<full data.json dict> to both build_html_email() and
#   send_email().  If scraped_data is omitted the code falls back to
#   promotions_data, which may reproduce the over-count.

import logging
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ── HKT timezone (UTC+8, no DST) ─────────────────────────────────────────────
# Already defined at module level for reuse
if '_HKT' not in globals():
    _HKT = timezone(timedelta(hours=8))

# ── Category metadata ─────────────────────────────────────────────────────────

CATEGORY_META = {
    "迎新":   {"bg": "#10b981", "emoji": "🎉"},
    "消費":   {"bg": "#f59e0b", "emoji": "💳"},
    "投資":   {"bg": "#6366f1", "emoji": "📈"},
    "旅遊":   {"bg": "#06b6d4", "emoji": "✈️"},
    "保險":   {"bg": "#ef4444", "emoji": "🛡️"},
    "貸款":   {"bg": "#dc2626", "emoji": "💰"},
    "存款":   {"bg": "#3b82f6", "emoji": "🏦"},
    "外匯":   {"bg": "#8b5cf6", "emoji": "🌐"},
    "推薦":   {"bg": "#ec4899", "emoji": "👥"},
    "新資金": {"bg": "#0ea5e9", "emoji": "💵"},
    "Others": {"bg": "#6b7280", "emoji": "📋"},
}

# ★ CHANGED:
#   "Airstar Bank" -> "EleBank"  (bank renamed; backward-compat alias kept)
#   "PAObank"      -> "PADB"     (bank renamed; backward-compat alias kept)

BANK_COLORS = {
    "ZA Bank":      "#25CD9C",
    "Mox Bank":     "#ec4899",
    "WeLab Bank":   "#7c3aed",
    "livi bank":    "#f97316",
    "PADB":         "#0ea5e9",   # ★ was PAObank
    "PAObank":      "#0ea5e9",   # backward-compat alias (existing DB rows)
    "EleBank":      "#06b6d4",   # ★ was Airstar Bank
    "Airstar Bank": "#06b6d4",   # backward-compat alias (existing DB rows)
    "Fusion Bank":  "#14b8a6",
    "Ant Bank":     "#1677ff",
}

BANK_DISPLAY_NAMES = {
    "ZA Bank":      "ZA",
    "EleBank":      "EleBank",   # ★ was Airstar Bank -> Airstar
    "Airstar Bank": "EleBank",   # backward-compat alias
    "Ant Bank":     "Ant",
    "Fusion Bank":  "Fusion",
    "Mox Bank":     "Mox",
    "PADB":         "PADB",      # ★ was PAObank -> PAO
    "PAObank":      "PADB",      # backward-compat alias
    "WeLab Bank":   "WeLab",
    "livi bank":    "Livi",
}

_BANK_NAME_GENERIC = {'bank', 'banking', 'digital', 'virtual', 'bank hk', ''}
_SMTP_MAX_RETRIES  = 3


# ── Helpers ───────────────────────────────────────────────────────────────────

def _bank_color(bank_name: str) -> str:
    name_lower = (bank_name or '').lower().strip()
    if name_lower in _BANK_NAME_GENERIC:
        return '#6b7280'
    for key, color in BANK_COLORS.items():
        if key.lower() == name_lower:
            return color
    for key, color in BANK_COLORS.items():
        key_lower = key.lower()
        if key_lower in name_lower or name_lower in key_lower:
            return color
    return '#6b7280'


def _bank_display_name(bank_name: str) -> str:
    name_lower = (bank_name or '').lower().strip()
    if name_lower in _BANK_NAME_GENERIC:
        return bank_name
    for key, short in BANK_DISPLAY_NAMES.items():
        if key.lower() == name_lower:
            return short
    for key, short in BANK_DISPLAY_NAMES.items():
        key_lower = key.lower()
        if key_lower in name_lower or name_lower in key_lower:
            return short
    return bank_name


def _get_cat_meta(type_str: str) -> dict:
    if type_str in CATEGORY_META:
        return CATEGORY_META[type_str]
    t = (type_str or '').lower()
    if any(k in t for k in ['welcome', 'new customer', 'onboard', '迎新']):
        return CATEGORY_META['迎新']
    if any(k in t for k in ['spend', 'cashback', 'card', '消費']):
        return CATEGORY_META['消費']
    if any(k in t for k in ['invest', 'stock', 'fund', 'crypto', '投資']):
        return CATEGORY_META['投資']
    if any(k in t for k in ['travel', 'flight', 'hotel', 'mile', '旅遊']):
        return CATEGORY_META['旅遊']
    if any(k in t for k in ['insur', '保險']):
        return CATEGORY_META['保險']
    if any(k in t for k in ['loan', 'borrow', '貸款']):
        return CATEGORY_META['貸款']
    if any(k in t for k in ['deposit', 'saving', 'time deposit', '存款']):
        return CATEGORY_META['存款']
    if any(k in t for k in ['fx', 'currency', 'exchange', 'remit', '外匯']):
        return CATEGORY_META['外匯']
    if any(k in t for k in ['refer', '推薦']):
        return CATEGORY_META['推薦']
    if any(k in t for k in ['new fund', 'fresh', '新資金']):
        return CATEGORY_META['新資金']
    return CATEGORY_META['Others']


def _cat_tag(text: str) -> str:
    meta  = _get_cat_meta(text)
    emoji = meta.get('emoji', '📋')
    bg    = meta.get('bg',    '#6b7280')
    return (
        f'<span style="display:inline-block;padding:3px 10px;margin:2px 3px 2px 0;'
        f'border-radius:20px;font-size:11px;color:#fff;font-weight:700;'
        f'background:{bg};">{emoji} {text}</span>'
    )


def _types_to_list(types_raw) -> list:
    if isinstance(types_raw, list):
        return [str(t).strip() for t in types_raw if str(t).strip()]
    if isinstance(types_raw, str):
        return [t.strip() for t in types_raw.split(',') if t.strip()]
    return []


# ── Shared classification helper ──────────────────────────────────────────────

def _classify_promo(p: dict, today_d, threshold_d) -> str:
    """
    Returns 'past' | 'expiring' | 'active'.

    Mirrors website getStatus() exactly:
      1. active === false  ->  'expired'  ->  'past'
      2. end_date < today  ->  'expired'  ->  'past'
      3. end_date within 30 days          ->  'expiring'
      4. everything else                  ->  'active'
    """
    active = p.get('active')
    if active is not None and not active:
        return 'past'

    ed = p.get('end_date')
    if ed:
        try:
            end_d = datetime.strptime(str(ed)[:10], '%Y-%m-%d').date()
            if end_d < today_d:
                return 'past'
            if end_d <= threshold_d:
                return 'expiring'
        except (ValueError, TypeError):
            pass
    return 'active'


def _hkt_now() -> datetime:
    """Current datetime in HKT (UTC+8)."""
    return datetime.now(_HKT)


def _hkt_today_and_threshold() -> tuple:
    """Return (today_date, threshold_date) both in HKT."""
    now = _hkt_now()
    return now.date(), (now + timedelta(days=30)).date()


# ── ★ Canonical count-source resolver ────────────────────────────────────────

def _resolve_count_source(
    promotions_data: list,
    scraped_data:    dict | None,
) -> list:
    if scraped_data and isinstance(scraped_data, dict):
        json_promos = scraped_data.get('promotions')
        if json_promos and isinstance(json_promos, list):
            return json_promos
    return promotions_data or []


# ── Multiple recipients helper ────────────────────────────────────────────────

def _collect_recipients(override: str | list[str] | None = None) -> list[str]:
    raw_emails: list[str] = []

    if override:
        if isinstance(override, list):
            for item in override:
                raw_emails.extend(str(item).split(','))
        else:
            raw_emails.extend(str(override).split(','))

    for env_var in (
        'RECIPIENT_EMAIL',
        'RECIPIENT_EMAIL_2',
        'RECIPIENT_EMAIL_3',
        'EMAIL_RECIPIENT',
        'EMAIL_TO',
    ):
        val = os.getenv(env_var, '').strip()
        if val:
            raw_emails.extend(val.split(','))

    seen:   set[str]  = set()
    result: list[str] = []
    for e in raw_emails:
        e = e.strip()
        if e and e not in seen:
            seen.add(e)
            result.append(e)

    return result


# ── Promotion card ────────────────────────────────────────────────────────────

def _new_promo_card(promo: dict) -> str:
    bank_name    = promo.get('bName') or promo.get('bank_name') or promo.get('bank') or 'Unknown'
    display_name = _bank_display_name(bank_name)
    color        = _bank_color(bank_name)
    title        = (promo.get('title') or promo.get('name') or 'Untitled')[:120]
    highlight    = promo.get('highlight') or promo.get('description') or ''
    period       = promo.get('period') or promo.get('validity') or 'Ongoing'
    quota        = promo.get('quota') or ''
    cost         = promo.get('cost') or ''
    tc_link      = promo.get('tc_link') or promo.get('url') or promo.get('link') or ''
    types_raw    = promo.get('types') or promo.get('type') or promo.get('promo_type') or ''
    type_list    = _types_to_list(types_raw)[:4]
    cat_tags     = ''.join(_cat_tag(t) for t in type_list) if type_list else _cat_tag('Others')

    meta_rows = ''
    if quota:
        meta_rows += f"""
<tr>
  <td style="padding:6px 0 2px;">
    <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;
                letter-spacing:.07em;margin-bottom:3px;">👥 Quota / Eligibility</div>
    <div style="font-size:13px;color:#374151;">{quota}</div>
  </td>
</tr>"""
    if cost:
        meta_rows += f"""
<tr>
  <td style="padding:6px 0 2px;">
    <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;
                letter-spacing:.07em;margin-bottom:3px;">💲 Cost / Min Spend</div>
    <div style="font-size:13px;color:#374151;">{cost}</div>
  </td>
</tr>"""

    source_btn = ''
    if tc_link:
        source_btn = f"""
<tr>
  <td style="padding:12px 0 0;">
    <a href="{tc_link}"
       style="display:inline-block;padding:8px 20px;
              background:#6366f1;color:#ffffff;border-radius:8px;
              font-size:12px;font-weight:700;text-decoration:none;
              letter-spacing:.02em;">
      🔗 View Official Source ↗
    </a>
  </td>
</tr>"""

    return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="margin-bottom:18px;border-radius:14px;overflow:hidden;
              border:1px solid #e5e7eb;box-shadow:0 3px 10px rgba(0,0,0,0.08);">
  <tr>
    <td bgcolor="{color}" style="background:{color};padding:13px 18px;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td style="vertical-align:middle;">
          <span style="font-weight:900;font-size:17px;color:#ffffff;">{display_name}</span>
        </td>
        <td style="text-align:right;vertical-align:middle;">
          <span style="background:rgba(255,255,255,0.22);color:#ffffff;
                       padding:3px 12px;border-radius:20px;font-size:11px;font-weight:600;">
            📅 {period}
          </span>
        </td>
      </tr></table>
    </td>
  </tr>
  <tr>
    <td style="background:#ffffff;padding:16px 18px;">
      <div style="margin-bottom:10px;">{cat_tags}</div>
      <div style="font-weight:800;font-size:15px;color:#1f2937;
                  line-height:1.4;margin-bottom:10px;">{title}</div>
      <div style="font-size:13px;color:#4b5563;line-height:1.7;
                  background:#f9fafb;border-radius:8px;
                  padding:10px 14px;margin-bottom:12px;
                  border-left:3px solid {color};">{highlight}</div>
      <table width="100%" cellpadding="0" cellspacing="0"
             style="border-top:1px solid #f3f4f6;">
        {meta_rows}{source_btn}
      </table>
    </td>
  </tr>
</table>"""


def _new_section_html(
    promos:        list,
    heading:       str,
    sub_heading:   str,
    icon:          str,
    header_color:  str,
    header_dark:   str,
    empty_msg:     str,
    count_label:   str,
    skip_if_empty: bool = False,
) -> str:
    if not promos:
        if skip_if_empty:
            return ''
        return f"""
<tr><td style="height:16px;"></td></tr>
<tr><td style="background:#f9fafb;border-radius:14px;padding:22px 20px;
               border:1px dashed #e5e7eb;text-align:center;">
  <div style="font-size:28px;margin-bottom:8px;">🔍</div>
  <div style="font-size:13px;font-weight:700;color:#6b7280;">{empty_msg}</div>
</td></tr>"""

    cards = ''.join(_new_promo_card(p) for p in promos)
    count = len(promos)
    label = count_label.format(count=count, s='' if count == 1 else 's')

    return f"""
<tr><td style="height:20px;"></td></tr>
<tr><td style="background:#ffffff;border-radius:16px;padding:24px;
               box-shadow:0 2px 8px rgba(0,0,0,0.07);">
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:22px;">
    <tr>
      <td bgcolor="{header_dark}"
          style="background-color:{header_dark};
                 background:{header_color};
                 border-radius:12px;padding:16px 22px;">
        <table width="100%" cellpadding="0" cellspacing="0"><tr>
          <td style="vertical-align:middle;">
            <span style="font-size:22px;vertical-align:middle;">{icon}</span>
            <span style="font-weight:900;font-size:17px;color:#1f2937;
                         vertical-align:middle;margin-left:10px;">{heading}</span>
            <div style="font-size:11px;color:rgba(0,0,0,0.45);margin-top:3px;
                        margin-left:34px;">{sub_heading}</div>
          </td>
          <td style="text-align:right;vertical-align:middle;white-space:nowrap;">
            <span style="background:rgba(0,0,0,0.15);color:#1f2937;
                         padding:4px 14px;border-radius:20px;
                         font-size:12px;font-weight:700;">
              {label}
            </span>
          </td>
        </tr></table>
      </td>
    </tr>
  </table>
  {cards}
</td></tr>"""


# ── Plain-text builder ────────────────────────────────────────────────────────

def _build_plain_text(
    promotions_data: list,
    new_promos:      list,
    new_promos_week: list,
    now:             str,
    ai_unavailable:  bool = False,
    count_source:    list = None,
) -> str:
    _src    = count_source if (count_source is not None) else (promotions_data or [])
    non_bau = [p for p in _src if not p.get('is_bau', False)]

    today_d, threshold = _hkt_today_and_threshold()

    exp_count  = 0
    past_count = 0
    for p in non_bau:
        status = _classify_promo(p, today_d, threshold)
        if status == 'past':
            past_count += 1
        elif status == 'expiring':
            exp_count += 1

    total_shown  = len(non_bau) - past_count
    active_count = total_shown - exp_count

    date_only = _hkt_now().strftime('%d %b %Y')

    lines = [
        f'VBank Tracker Daily Report - {date_only}',
        '=' * 50,
    ]
    if ai_unavailable:
        lines += [
            '',
            '[WARN]  NOTICE: AI extraction was unavailable today.',
            '    Data shown is from the last successful AI run (cached).',
            '    Promotions may not reflect today\'s latest changes.',
        ]

    new_show = [p for p in (new_promos or []) if not p.get('is_bau', False)]
    if new_show:
        lines += [
            '',
            f'NEWLY LAUNCHED TODAY ({len(new_show)}):',
        ]
        for p in new_show:
            bank  = p.get('bName') or p.get('bank_name') or '?'
            title = p.get('title') or p.get('name') or '?'
            tc    = p.get('tc_link') or p.get('url') or ''
            lines.append(f'  [{bank}] {title}')
            if p.get('period'): lines.append(f'    Period      : {p["period"]}')
            if p.get('quota'):  lines.append(f'    Eligibility : {p["quota"]}')
            if tc:              lines.append(f'    Source      : {tc}')
        lines.append('')

    week_show = [p for p in (new_promos_week or []) if not p.get('is_bau', False)]
    if week_show:
        lines += [
            f'PROMOTION NEWLY LAUNCHED WITHIN THIS WEEK - PAST 6 DAYS ({len(week_show)}):',
        ]
        for p in week_show:
            bank  = p.get('bName') or p.get('bank_name') or '?'
            title = p.get('title') or p.get('name') or '?'
            tc    = p.get('tc_link') or p.get('url') or ''
            lines.append(f'  [{bank}] {title}')
            if tc: lines.append(f'    Source : {tc}')
        lines.append('')
    elif not new_show:
        lines += ['', 'PROMOTION NEWLY LAUNCHED WITHIN THIS WEEK - PAST 6 DAYS: None', '']

    # Add new products section
    if new_products:
        lines += [
            f'{"="*60}',
            f'NEW PRODUCTS TODAY ({len(new_products)}):',
            f'{"="*60}',
        ]
        for p in new_products:
            bank = p.get('bank_name') or '?'
            name = p.get('product_name') or '?'
            cat = p.get('category', '').upper()
            subcat = p.get('subcategory', '')
            desc = p.get('description', '')
            rate = p.get('interest_rate', '')
            fees = p.get('fees', '')
            url = p.get('url', '')

            lines.append(f'\n  [{cat}] {name}')
            if subcat:
                lines.append(f'    Type       : {subcat}')
            if desc:
                lines.append(f'    Description: {desc}')
            if rate:
                lines.append(f'    Rate       : {rate}')
            if fees:
                lines.append(f'    Fees       : {fees}')
            if url:
                lines.append(f'    Source     : {url}')
        lines.append('')
    else:
        lines += ['', '='*60, 'NEW PRODUCTS TODAY: None', '='*60, '']

    lines += [
        f'TOTAL PROMOTIONS : {total_shown}',
        f'ACTIVE           : {active_count}',
        f'EXPIRING SOON    : {exp_count}  (within 30 days)',
        '',
    ]

    banks: dict = {}
    for p in non_bau:
        bn = p.get('bName') or p.get('bank_name') or 'Unknown'
        banks.setdefault(bn, []).append(p)

    lines.append('PROMOTIONS BY BANK:')
    for bname, promos in sorted(banks.items()):
        b_past = sum(1 for p in promos if _classify_promo(p, today_d, threshold) == 'past')
        lines.append(f'  {bname}: {len(promos) - b_past}')
    lines.append('')

    lines += [
        '-',
        'VBank Tracker • Auto-generated daily at 09:00 HKT',
        'Data sourced from official bank websites only.',
        'For full strategic insights visit the web dashboard.',
    ]
    return '\n'.join(lines)


# ── Main HTML builder ─────────────────────────────────────────────────────────

def build_html_email(
    promotions_data:    list,
    scraped_data:       dict,
    strategic_insights: dict = None,
    new_promos:         list = None,
    new_promos_week:    list = None,
    new_products:       list = None,
    ai_unavailable:     bool = False,
) -> str:
    new_promos      = new_promos      or []
    new_promos_week = new_promos_week or []
    new_products    = new_products    or []

    date_only = _hkt_now().strftime('%d %b %Y')

    count_list = _resolve_count_source(promotions_data, scraped_data)

    non_bau_data       = [p for p in count_list       if not p.get('is_bau', False)]
    new_promos_show    = [p for p in new_promos        if not p.get('is_bau', False)]
    new_promos_wk_show = [p for p in new_promos_week   if not p.get('is_bau', False)]

    banks: dict = {}
    for p in non_bau_data:
        bank = p.get('bName') or p.get('bank_name') or p.get('bank') or 'Unknown'
        banks.setdefault(bank, []).append(p)

    _today_d, _threshold = _hkt_today_and_threshold()

    expiring_count = 0
    past_end_count = 0
    for _p in non_bau_data:
        _status = _classify_promo(_p, _today_d, _threshold)
        if _status == 'past':
            past_end_count += 1
        elif _status == 'expiring':
            expiring_count += 1

    total_promos = len(non_bau_data) - past_end_count
    active_count = total_promos - expiring_count

    sorted_banks = sorted(
        banks.items(),
        key=lambda x: (0 if 'za' in x[0].lower() else 1, x[0]),
    )

    bank_rows = ''
    for bank_name, promos in sorted_banks:
        color        = _bank_color(bank_name)
        display_name = _bank_display_name(bank_name)

        b_exp  = 0
        b_past = 0
        for _p in promos:
            _status = _classify_promo(_p, _today_d, _threshold)
            if _status == 'past':
                b_past += 1
            elif _status == 'expiring':
                b_exp += 1

        b_active        = len(promos) - b_exp - b_past
        b_total_display = len(promos) - b_past

        exp_cell = (
            f'<span style="display:inline-block;background:#fef3c7;color:#92400e;'
            f'padding:3px 10px;border-radius:12px;font-size:11px;font-weight:700;">'
            f'⚡ {b_exp} expiring</span>'
            if b_exp else
            '<span style="font-size:13px;color:#d1d5db;">—</span>'
        )

        bank_rows += f"""
<tr style="border-bottom:1px solid #f3f4f6;">
  <td style="padding:12px 16px;">
    <span style="display:inline-block;width:10px;height:10px;border-radius:50%;
                 background:{color};vertical-align:middle;margin-right:9px;"></span>
    <span style="font-weight:700;font-size:14px;color:#1f2937;vertical-align:middle;">
      {display_name}
    </span>
  </td>
  <td style="padding:12px 16px;text-align:center;width:88px;">
    <div style="font-size:24px;font-weight:900;color:{color};line-height:1;">{b_active}</div>
    <div style="font-size:10px;color:#9ca3af;font-weight:700;text-transform:uppercase;
                letter-spacing:.05em;margin-top:2px;">active</div>
  </td>
  <td style="padding:12px 16px;text-align:center;width:140px;">{exp_cell}</td>
  <td style="padding:12px 16px;text-align:center;width:76px;">
    <div style="font-size:18px;font-weight:800;color:#6366f1;line-height:1;">{b_total_display}</div>
    <div style="font-size:10px;color:#9ca3af;font-weight:700;text-transform:uppercase;
                letter-spacing:.05em;margin-top:2px;">total</div>
  </td>
</tr>"""

    ai_notice_html = ''
    if ai_unavailable:
        ai_notice_html = """
<tr><td style="height:16px;"></td></tr>
<tr>
  <td style="background:#fffbeb;border-radius:12px;padding:16px 20px;
             border:1px solid #fcd34d;border-left:4px solid #f59e0b;">
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="vertical-align:middle;width:36px;font-size:24px;">[WARN]</td>
      <td style="vertical-align:middle;">
        <div style="font-size:13px;font-weight:800;color:#92400e;margin-bottom:3px;">
          AI Extraction Unavailable Today — Showing Cached Data
        </div>
        <div style="font-size:12px;color:#b45309;line-height:1.5;">
          The POE_API_KEY was not available during this run, so no new promotions
          were extracted or classified. The data shown below reflects the last
          successful AI run. Promotions may not include today's latest changes.
        </div>
      </td>
    </tr></table>
  </td>
</tr>"""

    today_section = _new_section_html(
        promos        = new_promos_show,
        heading       = 'Newly Launched Today',
        sub_heading   = '今日新推出優惠 · first_seen_at = today (HKT) · active only',
        icon          = '🆕',
        header_color  = 'linear-gradient(135deg,#ff6b35 0%,#f7931e 100%)',
        header_dark   = '#f97316',
        empty_msg     = 'No new promotions today',
        count_label   = '{count} new promotion{s}',
        skip_if_empty = True,
    )

    week_section = _new_section_html(
        promos        = new_promos_wk_show,
        heading       = 'Promotion newly launched within this week',
        sub_heading   = '本週新推出優惠 · first_seen_at in past 6 days (excl. today, HKT) · active only',
        icon          = '📅',
        header_color  = 'linear-gradient(135deg,#6366f1 0%,#8b5cf6 100%)',
        header_dark   = '#6366f1',
        empty_msg     = 'No new promotions in the past 6 days',
        count_label   = '{count} newly launched this week',
        skip_if_empty = False,
    )

    # Build new products HTML section
    products_section = ''
    if new_products:
        product_rows = ''
        for p in new_products:
            bank = p.get('bank_name', 'Unknown')
            name = p.get('product_name', '')
            cat = p.get('category', '').upper()
            subcat = p.get('subcategory', '')
            desc = p.get('description', '')
            rate = p.get('interest_rate', '')
            fees = p.get('fees', '')
            url = p.get('url', '')

            cat_color = {
                'DEPOSIT': '#10b981',
                'CARD': '#3b82f6',
                'INVESTMENT': '#8b5cf6',
                'LOAN': '#f59e0b',
            }.get(cat, '#6b7280')

            product_rows += f"""
<tr style="border-bottom:1px solid #f3f4f6;">
  <td style="padding:14px 16px;">
    <div style="margin-bottom:4px;">
      <span style="display:inline-block;background:{cat_color};color:#fff;
                   padding:2px 8px;border-radius:6px;font-size:10px;font-weight:700;">
        {cat}
      </span>
    </div>
    <div style="font-weight:700;font-size:14px;color:#1f2937;margin-bottom:3px;">{name}</div>
    <div style="font-size:12px;color:#6b7280;font-weight:500;">{bank}</div>
    {f'<div style="font-size:12px;color:#374151;margin-top:3px;">{desc}</div>' if desc else ''}
    {f'<div style="font-size:12px;color:#059669;font-weight:600;margin-top:2px;">Rate: {rate}</div>' if rate else ''}
    {f'<div style="font-size:12px;color:#dc2626;margin-top:2px;">Fees: {fees}</div>' if fees else ''}
    {f'<div style="font-size:11px;color:#9ca3af;margin-top:4px;"><a href="{url}" style="color:#3b82f6;text-decoration:none;">View Details →</a></div>' if url else ''}
  </td>
</tr>"""

        products_section = f"""
<tr><td style="height:18px;"></td></tr>
<tr>
  <td style="background:#ffffff;border-radius:14px;padding:22px 24px;
             border:1px solid #e5e7eb;box-shadow:0 2px 10px rgba(0,0,0,0.04);">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="vertical-align:middle;width:32px;font-size:26px;">🆕</td>
        <td style="vertical-align:middle;">
          <div style="font-size:15px;font-weight:800;color:#111827;line-height:1.3;">
            New Products Today
          </div>
          <div style="font-size:11px;color:#6b7280;font-weight:600;margin-top:2px;">
            Newly detected banking products · {len(new_products)} product{"s" if len(new_products) != 1 else ""}
          </div>
        </td>
      </tr>
      <tr><td style="height:14px;"></td></tr>
      <tr><td>
        <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
          {product_rows}
        </table>
      </td></tr>
    </table>
  </td>
</tr>"""
    else:
        products_section = """
<tr><td style="height:18px;"></td></tr>
<tr>
  <td style="background:#f9fafb;border-radius:14px;padding:18px 24px;
             border:1px dashed #d1d5db;text-align:center;">
    <div style="font-size:13px;font-weight:700;color:#6b7280;">No new products detected today</div>
  </td>
</tr>"""

    # Build all active promotions section (not just new ones)
    # Filter out BAU and already-shown promos to avoid duplication
    shown_ids = {p['id'] for p in new_promos_show + new_promos_wk_show}
    all_active_non_bau = [
        p for p in count_list
        if not p.get('is_bau', False)
        and p.get('active') is not False
        and p['id'] not in shown_ids
    ]

    if all_active_non_bau:
        active_cards = ''.join(_new_promo_card(p) for p in all_active_non_bau[:50])  # Cap at 50 to avoid email bloat
        active_count = len(all_active_non_bau)
        all_active_section = f"""
<tr><td style="height:20px;"></td></tr>
<tr><td style="background:#ffffff;border-radius:16px;padding:24px;
               box-shadow:0 2px 8px rgba(0,0,0,0.07);">
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:22px;">
    <tr>
      <td bgcolor="#10b981"
          style="background-color:#10b981;
                 background:linear-gradient(135deg,#10b981 0%,#059669 100%);
                 border-radius:12px;padding:16px 22px;">
        <table width="100%" cellpadding="0" cellspacing="0"><tr>
          <td style="vertical-align:middle;">
            <span style="font-size:22px;vertical-align:middle;">✅</span>
            <span style="font-weight:900;font-size:17px;color:#1f2937;
                         vertical-align:middle;margin-left:10px;">All Active Promotions</span>
            <div style="font-size:11px;color:rgba(0,0,0,0.45);margin-top:3px;
                        margin-left:34px;">所有進行中優惠 · currently active & valid · detailed view</div>
          </td>
          <td style="text-align:right;vertical-align:middle;white-space:nowrap;">
            <span style="background:rgba(0,0,0,0.15);color:#1f2937;
                         padding:4px 14px;border-radius:20px;
                         font-size:12px;font-weight:700;">
              {active_count} promotion{"s" if active_count != 1 else ""}
            </span>
          </td>
        </tr></table>
      </td>
    </tr>
  </table>
  {active_cards}
  {f'<div style="text-align:center;padding:20px;color:#9ca3af;font-size:12px;font-weight:600;">... and {active_count - 50} more (email capped at 50)</div>' if active_count > 50 else ''}
</td></tr>"""
    else:
        all_active_section = ''


    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>VBank Daily Report</title>
</head>
<body style="margin:0;padding:0;background:#f1f5f9;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0">
<tr><td align="center" style="padding:28px 12px;">
<table width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;">

  <!-- HEADER -->
  <tr><td style="background:#ffffff;border-radius:18px;padding:32px 28px;
                 text-align:center;border:2px solid #e5e7eb;
                 box-shadow:0 4px 16px rgba(0,0,0,0.08);">
    <div style="font-size:44px;margin-bottom:12px;">🏦</div>
    <div style="font-size:26px;font-weight:900;color:#111827;
                letter-spacing:-.5px;line-height:1.2;">
      VBank Tracker
    </div>
    <div style="font-size:12px;font-weight:700;color:#6b7280;
                margin-top:6px;letter-spacing:1.2px;text-transform:uppercase;">
      Daily Promotions Report
    </div>
    <div style="display:inline-block;margin-top:14px;padding:6px 20px;
                background:#f3f4f6;border-radius:20px;
                font-size:13px;color:#374151;font-weight:700;
                border:1px solid #e5e7eb;">
      📅 {date_only}
    </div>
  </td></tr>
  <tr><td style="height:20px;"></td></tr>

  {ai_notice_html}
  {today_section}
  {week_section}
  {products_section}
  {all_active_section}

  <tr><td style="height:20px;"></td></tr>

  <!-- Overall stats -->
  <tr><td style="background:#ffffff;border-radius:14px;
                 box-shadow:0 2px 8px rgba(0,0,0,0.07);">
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td width="33%" style="text-align:center;padding:24px 10px;
                              border-right:1px solid #f3f4f6;">
        <div style="font-size:10px;font-weight:700;color:#9ca3af;
                    text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;">
          Total
        </div>
        <div style="font-size:38px;font-weight:900;color:#6366f1;line-height:1;">
          {total_promos}
        </div>
        <div style="font-size:11px;color:#c4cad4;margin-top:5px;">non-BAU running</div>
      </td>
      <td width="33%" style="text-align:center;padding:24px 10px;
                              border-right:1px solid #f3f4f6;">
        <div style="font-size:10px;font-weight:700;color:#9ca3af;
                    text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;">
          Active Promos
        </div>
        <div style="font-size:38px;font-weight:900;color:#10b981;line-height:1;">
          {active_count}
        </div>
        <div style="font-size:11px;color:#c4cad4;margin-top:5px;">currently active</div>
      </td>
      <td width="33%" style="text-align:center;padding:24px 10px;">
        <div style="font-size:10px;font-weight:700;color:#9ca3af;
                    text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;">
          Expiring Soon
        </div>
        <div style="font-size:38px;font-weight:900;color:#f59e0b;line-height:1;">
          {expiring_count}
        </div>
        <div style="font-size:11px;color:#c4cad4;margin-top:5px;">within 30 days</div>
      </td>
    </tr></table>
  </td></tr>
  <tr><td style="height:20px;"></td></tr>

  <!-- Bank breakdown -->
  <tr><td style="background:#ffffff;border-radius:14px;padding:22px 22px 16px;
                 box-shadow:0 2px 8px rgba(0,0,0,0.07);">
    <div style="font-size:17px;font-weight:800;color:#1f2937;margin-bottom:4px;">
      📊 Promotions by Bank
    </div>
    <div style="font-size:12px;color:#9ca3af;margin-bottom:18px;">
      Excluding BAU permanent features · past-ended promotions not counted
    </div>
    <table width="100%" cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;border:1px solid #f3f4f6;
                  border-radius:10px;overflow:hidden;">
      <thead>
        <tr style="background:#f9fafb;border-bottom:2px solid #e5e7eb;">
          <th style="padding:10px 16px;text-align:left;font-size:10px;color:#6b7280;
                     font-weight:700;text-transform:uppercase;letter-spacing:.08em;">Bank</th>
          <th style="padding:10px 16px;text-align:center;width:88px;font-size:10px;
                     color:#6b7280;font-weight:700;text-transform:uppercase;
                     letter-spacing:.08em;">Active</th>
          <th style="padding:10px 16px;text-align:center;width:140px;font-size:10px;
                     color:#6b7280;font-weight:700;text-transform:uppercase;
                     letter-spacing:.08em;">Expiring</th>
          <th style="padding:10px 16px;text-align:center;width:76px;font-size:10px;
                     color:#6b7280;font-weight:700;text-transform:uppercase;
                     letter-spacing:.08em;">Total</th>
        </tr>
      </thead>
      <tbody>{bank_rows}</tbody>
    </table>
  </td></tr>

  <!-- FOOTER -->
  <tr><td style="height:16px;"></td></tr>
  <tr><td style="text-align:center;padding:16px 12px;">
    <div style="font-size:12px;color:#9ca3af;line-height:1.8;">
      VBank Tracker &nbsp;·&nbsp; Auto-generated daily at 09:00 HKT<br>
      Data sourced from official bank websites only<br>
      <span style="font-size:11px;color:#c4cad4;">
        For strategic insights &amp; full analysis, visit the web dashboard
      </span>
    </div>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# ── Sender ────────────────────────────────────────────────────────────────────

def send_email(
    html_content:    str,
    subject:         str                    = None,
    recipient:       str | list[str] | None = None,
    new_promos:      list                   = None,
    new_promos_week: list                   = None,
    promotions_data: list                   = None,
    ai_unavailable:  bool                   = False,
    scraped_data:    dict                   = None,
) -> bool:
    smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))

    smtp_user = (
        os.getenv('GMAIL_ADDRESS') or
        os.getenv('SMTP_USER')     or
        os.getenv('EMAIL_FROM')
    )
    smtp_pass = (
        os.getenv('GMAIL_APP_PASSWORD') or
        os.getenv('SMTP_PASS')          or
        os.getenv('EMAIL_PASS')
    )

    all_recipients = _collect_recipients(override=recipient)

    if not all([smtp_user, smtp_pass]):
        missing = [
            name for name, val in [
                ('GMAIL_ADDRESS',      smtp_user),
                ('GMAIL_APP_PASSWORD', smtp_pass),
            ] if not val
        ]
        print(f'[ERR] Missing SMTP credentials: {", ".join(missing)}')
        return False

    if not all_recipients:
        print('[ERR] No recipient emails configured. '
              'Set RECIPIENT_EMAIL (and optionally RECIPIENT_EMAIL_2) env vars.')
        return False

    if not subject:
        date_str = _hkt_now().strftime('%d %b %Y')
        base     = f'VBank Daily Report - {date_str}'
        subject  = f'{base} [Cached Data - AI Unavailable]' if ai_unavailable else base

    now_str = _hkt_now().strftime('%d %b %Y, %H:%M HKT')

    _count_source = _resolve_count_source(promotions_data or [], scraped_data)

    plain_text = _build_plain_text(
        promotions_data = promotions_data or [],
        new_promos      = new_promos      or [],
        new_promos_week = new_promos_week or [],
        now             = now_str,
        ai_unavailable  = ai_unavailable,
        count_source    = _count_source,
    )

    success_count = 0
    for email_to in all_recipients:
        msg            = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From']    = smtp_user
        msg['To']      = email_to
        msg.attach(MIMEText(plain_text,   'plain', 'utf-8'))
        msg.attach(MIMEText(html_content, 'html',  'utf-8'))

        for attempt in range(1, _SMTP_MAX_RETRIES + 1):
            try:
                with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                    server.ehlo()
                    server.starttls()
                    server.ehlo()
                    server.login(smtp_user, smtp_pass)
                    server.sendmail(smtp_user, [email_to], msg.as_string())
                logger.info(f'Email sent successfully -> {email_to}')
                print(f'  [OK] Email sent -> {email_to}')
                success_count += 1
                break
            except smtplib.SMTPAuthenticationError as exc:
                logger.error(f'SMTP auth failed for {email_to}: {exc}')
                print(f'  [ERR] SMTP authentication failed for {email_to}: Check GMAIL_APP_PASSWORD')
                break  # Don't retry auth failures
            except smtplib.SMTPRecipientsRefused as exc:
                logger.error(f'Recipient refused for {email_to}: {exc}')
                print(f'  [ERR] Recipient rejected by server for {email_to}: Invalid email address?')
                break  # Don't retry invalid recipients
            except smtplib.SMTPException as exc:
                if attempt < _SMTP_MAX_RETRIES:
                    wait = 2 ** attempt
                    logger.warning(f'SMTP attempt {attempt} failed for {email_to}: {exc} - retrying in {wait}s')
                    print(f'  [WARN]  SMTP attempt {attempt} failed for {email_to}: '
                          f'{exc} - retrying in {wait}s...')
                    time.sleep(wait)
                else:
                    logger.error(f'Email send failed for {email_to} after {_SMTP_MAX_RETRIES} attempts: {exc}')
                    print(f'  [ERR] Email send failed for {email_to} after '
                          f'{_SMTP_MAX_RETRIES} attempts: {exc}')
            except ConnectionRefusedError as exc:
                logger.error(f'SMTP connection refused for {email_to}: {exc}')
                print(f'  [ERR] SMTP connection refused for {email_to}: Check SMTP_HOST/PORT')
                break
            except Exception as exc:
                logger.error(f'Unexpected email error for {email_to}: {type(exc).__name__}: {exc}')
                print(f'  [ERR] Email send error for {email_to}: {exc}')
                break

    if success_count == len(all_recipients):
        print(f'[OK] All emails sent -> {len(all_recipients)} recipient(s): '
              f'{", ".join(all_recipients)}')
        return True
    elif success_count > 0:
        print(f'[WARN]  Email partial: {success_count}/{len(all_recipients)} recipients received it')
        return True
    else:
        print(f'[ERR] Email failed for all {len(all_recipients)} recipient(s)')
        return False