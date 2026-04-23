# scripts/emailer.py
#
# IMPORTANT — caller contract for "New Today" / "New This Week" data:
#
#   new_promos      → pass output of database.get_new_promotions_today()
#                     (NOT get_new_promotions_for_run — that uses run-ID, not date)
#   new_promos_week → pass output of database.get_new_promotions_last_n_days(days=6)
#
# Using the date-based functions ensures the email and the website show
# identical promotion sets (the website uses first_seen_at date logic in JS,
# mirroring COALESCE(first_seen_at, created_at) in the Python queries).

import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone     # ← timezone added

# ── HKT timezone ──────────────────────────────────────────────────────────────
# Hong Kong does NOT observe DST, so UTC+8 is always correct.
# The website always computes "today" in HKT:
#   new Date(Date.now() + 8*3600_000).toISOString().slice(0,10)
# Python must use the same calendar date; datetime.now() uses server local
# time which differs from HKT on any non-HKT server (e.g. UTC, US zones).
# ROOT CAUSE of active/expiring mismatch: if server local date ≠ HKT date,
# the 30-day threshold boundary shifts by 1 day, flipping promotions near
# the boundary between 'active' and 'expiring' while leaving totals unchanged.
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

BANK_COLORS = {
    "ZA Bank":      "#25CD9C",
    "Mox Bank":     "#ec4899",
    "WeLab Bank":   "#7c3aed",
    "livi bank":    "#f97316",
    "PAObank":      "#0ea5e9",
    "Airstar Bank": "#06b6d4",
    "Fusion Bank":  "#14b8a6",
    "Ant Bank":     "#1677ff",
}

BANK_DISPLAY_NAMES = {
    "ZA Bank":      "ZA",
    "Airstar Bank": "Airstar",
    "Ant Bank":     "Ant",
    "Fusion Bank":  "Fusion",
    "Mox Bank":     "Mox",
    "PAObank":      "PAO",
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
      1. active === false  →  'expired'  →  'past'
      2. end_date < today  →  'expired'  →  'past'
      3. end_date within 30 days (diffDays <= 30)  →  'expiring'
      4. everything else   →  'active'

    today_d and threshold_d MUST be derived from HKT (UTC+8) to match
    the website's _PAGE_TODAY_STR computation.  Pass values from
    _hkt_today() / _hkt_threshold() — never from datetime.now().date()
    directly, which uses server-local time.
    """
    # Mirror JS: getStatus() returns 'expired' when p.active === false
    active = p.get('active')
    if active is not None and not active:   # False or 0, but NOT None
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
    """Current datetime in HKT (UTC+8) — matches the website's date arithmetic."""
    return datetime.now(_HKT)


def _hkt_today_and_threshold() -> tuple:
    """
    Return (today_date, threshold_date) both in HKT.

    today_date    = HKT calendar date  (matches JS _PAGE_TODAY_STR)
    threshold_date = today + 30 days   (matches JS diffDays <= 30)
    """
    now = _hkt_now()
    return now.date(), (now + timedelta(days=30)).date()


# ── Multiple recipients helper ────────────────────────────────────────────────

def _collect_recipients(override: str = None) -> list[str]:
    raw_emails: list[str] = []

    if override:
        raw_emails.extend(override.split(','))

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
) -> str:
    non_bau = [p for p in (promotions_data or []) if not p.get('is_bau', False)]

    # FIX: use _hkt_today_and_threshold() so today_d is the HKT calendar date,
    # matching the website's _PAGE_TODAY_STR = new Date(Date.now()+8h).toISOString()[:10].
    # Previously datetime.now().date() used server-local time; on a non-HKT server
    # (e.g. UTC, US timezones) the date can differ from HKT, shifting the 30-day
    # threshold boundary and causing the active/expiring split to diverge from the
    # website while both totals remain equal.
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

    # FIX: display date also uses HKT so the report header date matches HKT reality
    date_only = _hkt_now().strftime('%d %b %Y')

    lines = [
        f'VBank Tracker Daily Report — {date_only}',
        '=' * 50,
    ]
    if ai_unavailable:
        lines += [
            '',
            '⚠️  NOTICE: AI extraction was unavailable today.',
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
            f'PROMOTION NEWLY LAUNCHED WITHIN THIS WEEK — PAST 6 DAYS ({len(week_show)}):',
        ]
        for p in week_show:
            bank  = p.get('bName') or p.get('bank_name') or '?'
            title = p.get('title') or p.get('name') or '?'
            tc    = p.get('tc_link') or p.get('url') or ''
            lines.append(f'  [{bank}] {title}')
            if tc: lines.append(f'    Source : {tc}')
        lines.append('')
    elif not new_show:
        lines += ['', 'PROMOTION NEWLY LAUNCHED WITHIN THIS WEEK — PAST 6 DAYS: None', '']

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
        '—',
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
    ai_unavailable:     bool = False,
) -> str:
    """
    Build the full HTML email.

    Active / expiring counts now always match the website because:
      _hkt_now() pins datetime to UTC+8 (HKT), matching the website's
      new Date(Date.now() + 8*3600_000) computation.  Previously
      datetime.now() used server-local time; on a non-HKT server the
      30-day boundary shifted by the UTC offset, flipping promotions
      near the boundary between active and expiring while leaving totals
      unchanged (since neither side changed whether a promo was counted).
    """
    new_promos      = new_promos      or []
    new_promos_week = new_promos_week or []

    # FIX: use HKT for display date so the email header shows the correct HKT date
    date_only = _hkt_now().strftime('%d %b %Y')

    non_bau_data       = [p for p in (promotions_data or []) if not p.get('is_bau', False)]
    new_promos_show    = [p for p in new_promos      if not p.get('is_bau', False)]
    new_promos_wk_show = [p for p in new_promos_week if not p.get('is_bau', False)]

    banks: dict = {}
    for p in non_bau_data:
        bank = p.get('bName') or p.get('bank_name') or p.get('bank') or 'Unknown'
        banks.setdefault(bank, []).append(p)

    # FIX: _hkt_today_and_threshold() — both today_d and threshold_d are in HKT.
    # This is the single source of truth for all active/expiring classification
    # in this function; changing it here fixes both the header stats and the
    # per-bank breakdown table simultaneously.
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

    # ── AI unavailable notice banner ──────────────────────────────────────────
    ai_notice_html = ''
    if ai_unavailable:
        ai_notice_html = """
<tr><td style="height:16px;"></td></tr>
<tr>
  <td style="background:#fffbeb;border-radius:12px;padding:16px 20px;
             border:1px solid #fcd34d;border-left:4px solid #f59e0b;">
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="vertical-align:middle;width:36px;font-size:24px;">⚠️</td>
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

    # ── Section 1: Newly Launched Today ───────────────────────────────────────
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

    # ── Section 2: Promotion newly launched within this week ──────────────────
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

  <!-- AI UNAVAILABLE NOTICE -->
  {ai_notice_html}

  <!-- SECTION 1: NEWLY LAUNCHED TODAY (omitted when empty) -->
  {today_section}

  <!-- SECTION 2: PROMOTION NEWLY LAUNCHED WITHIN THIS WEEK (always shown) -->
  {week_section}

  <!-- SECTION 3: OVERALL STATS + BANK BREAKDOWN -->
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
    subject:         str  = None,
    recipient:       str  = None,
    new_promos:      list = None,
    new_promos_week: list = None,
    promotions_data: list = None,
    ai_unavailable:  bool = False,
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
        print(f'❌ Missing SMTP credentials: {", ".join(missing)}')
        return False

    if not all_recipients:
        print('❌ No recipient emails configured. '
              'Set RECIPIENT_EMAIL (and optionally RECIPIENT_EMAIL_2) env vars.')
        return False

    if not subject:
        # FIX: use HKT date for the email subject line
        date_str = _hkt_now().strftime('%d %b %Y')
        base     = f'🏦 VBank Daily Report — {date_str}'
        subject  = f'{base} [Cached Data — AI Unavailable]' if ai_unavailable else base

    # FIX: use HKT timestamp in plain-text header
    now_str    = _hkt_now().strftime('%d %b %Y, %H:%M HKT')
    plain_text = _build_plain_text(
        promotions_data or [],
        new_promos      or [],
        new_promos_week or [],
        now_str,
        ai_unavailable = ai_unavailable,
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
                print(f'  ✅ Email sent → {email_to}')
                success_count += 1
                break
            except smtplib.SMTPException as exc:
                if attempt < _SMTP_MAX_RETRIES:
                    wait = 2 ** attempt
                    print(f'  ⚠️  SMTP attempt {attempt} failed for {email_to}: '
                          f'{exc} — retrying in {wait}s…')
                    time.sleep(wait)
                else:
                    print(f'  ❌ Email send failed for {email_to} after '
                          f'{_SMTP_MAX_RETRIES} attempts: {exc}')
            except Exception as exc:
                print(f'  ❌ Email send error for {email_to}: {exc}')
                break

    if success_count == len(all_recipients):
        print(f'✅ Email sent → {len(all_recipients)} recipient(s)')
        return True
    elif success_count > 0:
        print(f'⚠️  Email partial: {success_count}/{len(all_recipients)} recipients received it')
        return True
    else:
        print(f'❌ Email failed for all {len(all_recipients)} recipient(s)')
        return False