# scripts/emailer.py

import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta

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

CATEGORY_EMOJIS = {k: v["emoji"] for k, v in CATEGORY_META.items()}

_BANK_NAME_GENERIC = {'bank', 'banking', 'digital', 'virtual', 'bank hk', ''}

_SMTP_MAX_RETRIES = 3


# ── Helpers ───────────────────────────────────────────────────────────────────

def _bank_color(bank_name: str) -> str:
    """
    Exact match first, then bidirectional substring match.
    The exact-first check prevents 'ZA Bank' matching 'Ant Bank' via substring
    when the lookup table key 'ZA Bank' is checked against 'Ant Bank'.
    """
    name_lower = (bank_name or '').lower().strip()
    if name_lower in _BANK_NAME_GENERIC:
        return '#6b7280'
    # Exact match
    for key, color in BANK_COLORS.items():
        if key.lower() == name_lower:
            return color
    # Bidirectional substring fallback
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


def _bau_tag() -> str:
    return (
        '<span style="display:inline-block;padding:3px 10px;margin:2px 3px 2px 0;'
        'border-radius:20px;font-size:11px;color:#fff;font-weight:700;'
        'background:#64748b;">⚙️ BAU</span>'
    )


def _types_to_list(types_raw) -> list:
    if isinstance(types_raw, list):
        return [str(t).strip() for t in types_raw if str(t).strip()]
    if isinstance(types_raw, str):
        return [t.strip() for t in types_raw.split(',') if t.strip()]
    return []


# ── Promotion card ────────────────────────────────────────────────────────────

def _promo_card(promo: dict, color: str) -> str:
    title     = (promo.get('title') or promo.get('name') or 'Untitled')[:100]
    highlight = promo.get('highlight') or promo.get('description') or ''
    period    = promo.get('period')    or promo.get('validity') or 'Ongoing'
    quota     = promo.get('quota')     or ''
    cost      = promo.get('cost')      or ''
    tc_link   = promo.get('tc_link')   or promo.get('url') or promo.get('link') or ''
    types_raw = promo.get('types')     or promo.get('type') or promo.get('promo_type') or ''
    is_bau    = bool(promo.get('is_bau', False))

    type_list = _types_to_list(types_raw)[:4]
    cat_tags  = ''.join(_cat_tag(t) for t in type_list) if type_list else _cat_tag('Others')
    if is_bau:
        cat_tags += _bau_tag()

    quota_row = (
        f'<span style="font-size:11px;color:#6b7280;margin-right:12px;">👥 {quota}</span>'
        if quota else ''
    )
    cost_row = (
        f'<span style="font-size:11px;color:#6b7280;margin-right:12px;">💲 {cost}</span>'
        if cost else ''
    )
    tc_row = (
        f'<a href="{tc_link}" style="font-size:11px;color:#6366f1;text-decoration:none;">'
        f'📄 T&amp;C</a>'
        if tc_link else ''
    )

    return f"""
<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:12px;">
<tr><td style="background:#ffffff;border-radius:10px;padding:14px 16px;
               border-left:4px solid {color};
               box-shadow:0 1px 3px rgba(0,0,0,0.08);">
  <div style="margin-bottom:8px;">{cat_tags}</div>
  <div style="font-weight:700;font-size:14px;color:#1f2937;margin-bottom:6px;">{title}</div>
  <div style="font-size:13px;color:#4b5563;line-height:1.6;margin-bottom:8px;">{highlight}</div>
  <div style="margin-top:6px;">
    <span style="font-size:11px;color:#9ca3af;margin-right:12px;">📅 {period}</span>
    {quota_row}{cost_row}{tc_row}
  </div>
</td></tr>
</table>"""


# ── Bank section ──────────────────────────────────────────────────────────────

def _bank_section(bank_name: str, promos: list) -> str:
    color        = _bank_color(bank_name)
    display_name = _bank_display_name(bank_name)
    count        = len(promos)
    cards        = ''.join(_promo_card(p, color) for p in promos)
    return f"""
<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
<tr><td style="padding-bottom:10px;border-bottom:3px solid {color};">
  <table width="100%" cellpadding="0" cellspacing="0"><tr>
    <td style="vertical-align:middle;">
      <span style="display:inline-block;width:12px;height:12px;border-radius:50%;
                   background:{color};vertical-align:middle;margin-right:8px;"></span>
      <span style="font-weight:800;font-size:17px;color:#1f2937;vertical-align:middle;">
        {display_name}
      </span>
    </td>
    <td style="text-align:right;vertical-align:middle;">
      <span style="background:{color};color:#fff;padding:3px 12px;border-radius:20px;
                   font-size:12px;font-weight:700;">
        {count} promo{"s" if count != 1 else ""}
      </span>
    </td>
  </tr></table>
</td></tr>
<tr><td style="padding-top:12px;">{cards}</td></tr>
</table>"""


# ── Strategic Insights ────────────────────────────────────────────────────────

def _insights_html(insights: dict) -> str:
    if not insights:
        return ''

    best_rows = ''
    for item in insights.get('best_for', []):
        cat    = item.get('category', '')
        bank   = item.get('bank', '')
        detail = item.get('detail', '')
        bc     = _bank_color(bank)
        em     = _get_cat_meta(cat).get('emoji', '🏆')
        best_rows += f"""
<tr style="border-bottom:1px solid #f3f4f6;">
  <td style="padding:9px 12px;font-size:13px;color:#374151;font-weight:600;white-space:nowrap;">
    {em}&nbsp;Best for {cat}
  </td>
  <td style="padding:9px 12px;white-space:nowrap;">
    <span style="background:{bc};color:#fff;padding:3px 10px;border-radius:20px;
                 font-size:12px;font-weight:700;">{_bank_display_name(bank)}</span>
  </td>
  <td style="padding:9px 12px;font-size:13px;color:#6b7280;">{detail}</td>
</tr>"""

    best_table = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="border-collapse:collapse;background:#ffffff;border-radius:10px;
              overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,0.08);margin-bottom:24px;">
  <thead>
    <tr style="background:#f9fafb;">
      <th style="padding:9px 12px;text-align:left;font-size:11px;color:#6b7280;
                 font-weight:700;text-transform:uppercase;">Category</th>
      <th style="padding:9px 12px;text-align:left;font-size:11px;color:#6b7280;
                 font-weight:700;text-transform:uppercase;">Winner</th>
      <th style="padding:9px 12px;text-align:left;font-size:11px;color:#6b7280;
                 font-weight:700;text-transform:uppercase;">Why</th>
    </tr>
  </thead>
  <tbody>{best_rows}</tbody>
</table>"""

    bank_cards   = ''
    sorted_banks = sorted(
        insights.get('bank_analysis', {}).items(),
        key=lambda x: (0 if 'za' in x[0].lower() else 1, x[0]),
    )

    for bank_name, data in sorted_banks:
        bc           = _bank_color(bank_name)
        count        = data.get('count', 0)
        focus        = data.get('focus', '')
        strengths    = data.get('strengths', [])[:3]
        expiring     = data.get('expiring_alert', '')
        pros         = data.get('vs_za_pros')
        cons         = data.get('vs_za_cons')
        is_za        = 'za' in bank_name.lower()
        hdr_bg       = bc if is_za else '#f9fafb'
        hdr_color    = '#ffffff' if is_za else '#1f2937'
        display_name = _bank_display_name(bank_name)

        strengths_html = ''.join(
            f'<tr><td style="padding:3px 0;font-size:13px;color:#374151;">✓&nbsp;{s}</td></tr>'
            for s in strengths
        )
        expiring_html = (
            f'<tr><td style="background:#fef3c7;border:1px solid #fcd34d;border-radius:8px;'
            f'padding:7px 12px;font-size:12px;color:#92400e;font-weight:600;margin-top:10px;">'
            f'⚡&nbsp;{expiring}</td></tr>'
            if expiring else ''
        )

        vs_za_html = ''
        if is_za:
            vs_za_html = (
                '<tr><td style="background:#fef2f2;border-radius:8px;padding:6px 12px;'
                'font-size:12px;color:#dc2626;font-weight:700;margin-top:10px;">'
                '🏆 Base Comparison Bank</td></tr>'
            )
        elif pros or cons:
            pros_row = (
                f'<tr><td style="font-size:12px;color:#059669;padding:2px 0;">✅&nbsp;{pros}</td></tr>'
                if pros else ''
            )
            cons_row = (
                f'<tr><td style="font-size:12px;color:#dc2626;padding:2px 0;">❌&nbsp;{cons}</td></tr>'
                if cons else ''
            )
            vs_za_html = f"""
<tr><td style="font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;
               padding:8px 0 4px;letter-spacing:.05em;">vs ZA Bank</td></tr>
{pros_row}{cons_row}"""

        bank_cards += f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="margin-bottom:16px;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;">
  <tr>
    <td style="background:{hdr_bg};padding:12px 16px;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td>
          <span style="font-weight:800;font-size:15px;color:{hdr_color};">{display_name}</span>
        </td>
        <td style="text-align:right;">
          <span style="background:rgba(0,0,0,0.1);color:{hdr_color};padding:2px 10px;
                       border-radius:20px;font-size:12px;font-weight:700;">{count} active</span>
        </td>
      </tr></table>
    </td>
  </tr>
  <tr>
    <td style="background:#ffffff;padding:14px 16px;">
      <div style="font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;
                  margin-bottom:4px;">Focus</div>
      <div style="font-size:13px;color:#374151;margin-bottom:12px;">{focus}</div>
      <div style="font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;
                  margin-bottom:6px;">Key Strengths</div>
      <table cellpadding="0" cellspacing="0">
        {strengths_html}{expiring_html}{vs_za_html}
      </table>
    </td>
  </tr>
</table>"""

    return f"""
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f8fafc;border-radius:16px;">
  <tr><td style="padding:24px;">
    <div style="font-size:20px;font-weight:800;color:#1f2937;margin-bottom:4px;">
      🧠 Strategic Insights
    </div>
    <div style="font-size:13px;color:#6b7280;margin-bottom:20px;">
      AI-generated analysis • Updated daily • Base comparison: ZA Bank
    </div>
    <div style="font-size:12px;font-weight:700;color:#374151;text-transform:uppercase;
                margin-bottom:12px;">🏆 Best in Category</div>
    {best_table}
    <div style="font-size:12px;font-weight:700;color:#374151;text-transform:uppercase;
                margin-bottom:14px;">📋 Bank-by-Bank Analysis</div>
    {bank_cards}
  </td></tr>
</table>"""


# ── Plain-text builder ────────────────────────────────────────────────────────

def _build_plain_text(
    promotions_data: list,
    new_promos:      list,
    now:             str,
) -> str:
    """
    Minimal plain-text alternative required by RFC 2822 / spam filters.
    A message with only an HTML part scores higher on SpamAssassin and is
    more likely to land in spam.  This plain-text version is shown by
    mail clients that don't render HTML (e.g. command-line readers).
    """
    lines = [
        f'VBank Tracker Daily Report — {now}',
        '=' * 50,
        '',
    ]

    if new_promos:
        lines.append(f'NEW THIS RUN ({len(new_promos)}):')
        for p in new_promos:
            bank  = p.get('bName') or p.get('bank_name') or '?'
            title = p.get('title') or p.get('name') or '?'
            lines.append(f'  [{bank}] {title}')
        lines.append('')

    banks: dict = {}
    for p in (promotions_data or []):
        bn = p.get('bName') or p.get('bank_name') or 'Unknown'
        banks.setdefault(bn, []).append(p)

    lines.append(f'ALL ACTIVE PROMOTIONS ({len(promotions_data)}):')
    lines.append('')
    for bank_name, promos in sorted(banks.items()):
        lines.append(f'── {bank_name} ({len(promos)}) ──')
        for p in promos:
            title  = p.get('title') or p.get('name') or '?'
            period = p.get('period') or 'Ongoing'
            tc     = p.get('tc_link') or p.get('url') or ''
            lines.append(f'  • {title} | {period}')
            if tc:
                lines.append(f'    {tc}')
        lines.append('')

    lines += [
        '—',
        'VBank Tracker • Auto-generated daily at 09:00 HKT',
        'Data sourced from official bank websites only.',
    ]
    return '\n'.join(lines)


# ── Main HTML builder ─────────────────────────────────────────────────────────

def build_html_email(
    promotions_data:    list,
    scraped_data:       dict,
    strategic_insights: dict = None,
    new_promos:         list = None,
) -> str:
    new_promos = new_promos or []
    now        = datetime.now().strftime('%d %b %Y, %H:%M HKT')

    # FIX: promotions_data is already filtered to non-BAU in main.py before
    # being passed here.  The original code re-filtered it here, which was
    # redundant.  We keep it for safety but note the outer filter is the
    # canonical one.
    non_bau_data    = [p for p in (promotions_data or []) if not p.get('is_bau', False)]
    new_promos_show = [p for p in new_promos              if not p.get('is_bau', False)]

    banks: dict = {}
    for p in non_bau_data:
        bank = p.get('bName') or p.get('bank_name') or p.get('bank') or 'Unknown'
        banks.setdefault(bank, []).append(p)

    total_promos = len(non_bau_data)
    total_banks  = len(BANK_COLORS)

    # ── Expiring soon ─────────────────────────────────────────────
    _now       = datetime.now()
    _today_d   = _now.date()
    _threshold = (_now + timedelta(days=30)).date()
    _this_m    = _now.strftime('%b').lower()
    _next_m    = ['jan','feb','mar','apr','may','jun',
                  'jul','aug','sep','oct','nov','dec'][_now.month % 12]

    expiring_count = 0
    # FIX: track promos whose end_date has already passed but are still
    # active=1 in the DB (not yet cleaned up by mark_stale_as_inactive).
    # The website counts these as "Expired", so the email stat must exclude
    # them from "Active Promos" to stay consistent.
    past_end_count = 0
    for _p in non_bau_data:
        _ed = _p.get('end_date')
        if _ed:
            try:
                _end_d = datetime.strptime(str(_ed)[:10], '%Y-%m-%d').date()
                if _end_d < _today_d:
                    past_end_count += 1
                elif _today_d <= _end_d <= _threshold:
                    expiring_count += 1
            except (ValueError, TypeError):
                pass
        else:
            _period = str(_p.get('period', '')).lower()
            if _this_m in _period or _next_m in _period:
                expiring_count += 1

    # FIX: active_count mirrors the website's "Active ✅" bucket:
    # total non-BAU  minus  expiring  minus  already-past-end_date.
    active_count = total_promos - expiring_count - past_end_count

    # ── Scrape status rows ────────────────────────────────────────
    # FIX: removed dead result.get("status") and result.get("count") lookups.
    # Scrape results (ScrapeResult.to_dict) have neither key.  The success
    # flag is in result.get("success") and count is derived from the banks
    # dict which is built from the same DB snapshot as data.json.
    scrape_rows = ''
    for bank_name, result in sorted((scraped_data or {}).items()):
        ok           = result.get('success') is True
        count        = len(banks.get(bank_name, []))
        dot          = '#10b981' if ok else '#ef4444'
        label        = f"{'✅' if ok else '❌'} {'success' if ok else 'failed'}"
        display      = _bank_display_name(bank_name)
        scrape_rows += f"""
<tr style="border-bottom:1px solid #f3f4f6;">
  <td style="padding:9px 12px;font-size:13px;color:#374151;font-weight:600;">{display}</td>
  <td style="padding:9px 12px;text-align:center;font-size:13px;color:{dot};">{label}</td>
  <td style="padding:9px 12px;text-align:center;font-size:14px;font-weight:800;
             color:#6366f1;">{count}</td>
</tr>"""

    sorted_banks   = sorted(banks.items(), key=lambda x: (0 if 'za' in x[0].lower() else 1, x[0]))
    promos_html    = ''.join(_bank_section(bn, bp) for bn, bp in sorted_banks)
    insights_block = _insights_html(strategic_insights) if strategic_insights else ''
    insights_row   = (
        f"<tr><td>{insights_block}</td></tr><tr><td style='height:16px;'></td></tr>"
        if insights_block else ''
    )

    # ── Newly launched section ────────────────────────────────────
    if new_promos_show:
        new_rows = ''
        for p in new_promos_show:
            bank_name    = p.get('bName') or p.get('bank_name') or p.get('bank') or '—'
            display_name = _bank_display_name(bank_name)
            title        = p.get('title') or p.get('name') or '—'
            types_raw    = p.get('types') or p.get('type') or p.get('promo_type') or ''
            types_str    = ', '.join(_types_to_list(types_raw)) if types_raw else '—'
            period       = p.get('period') or p.get('validity') or 'Ongoing'
            bc           = _bank_color(bank_name)
            new_rows += f"""
<tr style="border-bottom:1px solid #f0f0f0;">
  <td style="padding:8px 12px;font-size:13px;font-weight:700;color:{bc};">{display_name}</td>
  <td style="padding:8px 12px;font-size:13px;color:#374151;">{title}</td>
  <td style="padding:8px 12px;font-size:13px;color:#6b7280;">{types_str}</td>
  <td style="padding:8px 12px;font-size:13px;color:#6b7280;">{period}</td>
</tr>"""

        new_section_html = f"""
<tr><td style="border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.08);">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr>
      <td style="background:linear-gradient(135deg,#ff6b35,#f7931e);padding:14px 20px;">
        <table width="100%" cellpadding="0" cellspacing="0"><tr>
          <td>
            <span style="font-size:18px;">🆕</span>
            <span style="font-weight:800;font-size:16px;color:#fff;margin-left:8px;">
              NEWLY LAUNCHED PROMOTIONS
            </span>
          </td>
          <td style="text-align:right;">
            <span style="background:rgba(255,255,255,0.25);color:#fff;border-radius:20px;
                         padding:3px 12px;font-size:12px;font-weight:700;">
              {len(new_promos_show)} new
            </span>
          </td>
        </tr></table>
      </td>
    </tr>
    <tr>
      <td style="background:#fffdf9;">
        <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
          <thead>
            <tr style="background:#fff3e8;">
              <th style="padding:10px 12px;text-align:left;color:#e65c00;font-weight:700;font-size:12px;">Bank</th>
              <th style="padding:10px 12px;text-align:left;color:#e65c00;font-weight:700;font-size:12px;">Promotion</th>
              <th style="padding:10px 12px;text-align:left;color:#e65c00;font-weight:700;font-size:12px;">Category</th>
              <th style="padding:10px 12px;text-align:left;color:#e65c00;font-weight:700;font-size:12px;">Period</th>
            </tr>
          </thead>
          <tbody>{new_rows}</tbody>
        </table>
      </td>
    </tr>
  </table>
</td></tr>
<tr><td style="height:16px;"></td></tr>"""
    else:
        new_section_html = """
<tr><td style="background:#f9f9f9;border-radius:12px;padding:16px 20px;
               border:1px dashed #ddd;text-align:center;
               color:#9ca3af;font-size:13px;">
  🔍 No new promotions detected since the last run.
</td></tr>
<tr><td style="height:16px;"></td></tr>"""

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
<tr><td align="center" style="padding:20px 10px;">
<table width="100%" cellpadding="0" cellspacing="0" style="max-width:660px;">

  <!-- HEADER -->
  <tr><td style="background:linear-gradient(135deg,#1a1a2e 0%,#16213e 50%,#0f3460 100%);
                 border-radius:16px;padding:28px;text-align:center;">
    <div style="font-size:36px;margin-bottom:8px;">🏦</div>
    <div style="font-size:22px;font-weight:800;color:#fff;">VBank Tracker Daily Report</div>
    <div style="font-size:13px;color:rgba(255,255,255,0.65);margin-top:6px;">{now}</div>
  </td></tr>
  <tr><td style="height:16px;"></td></tr>

  <!-- STATS -->
  <tr><td style="background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.08);">
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td width="33%" style="text-align:center;padding:18px 12px;border-right:1px solid #f3f4f6;">
        <div style="font-size:30px;font-weight:800;color:#6366f1;">{active_count}</div>
        <div style="font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;margin-top:4px;">Active Promos</div>
      </td>
      <td width="33%" style="text-align:center;padding:18px 12px;border-right:1px solid #f3f4f6;">
        <div style="font-size:30px;font-weight:800;color:#10b981;">{total_banks}</div>
        <div style="font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;margin-top:4px;">Banks Tracked</div>
      </td>
      <td width="33%" style="text-align:center;padding:18px 12px;">
        <div style="font-size:30px;font-weight:800;color:#f59e0b;">{expiring_count}</div>
        <div style="font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;margin-top:4px;">Expiring Soon</div>
      </td>
    </tr></table>
  </td></tr>
  <tr><td style="height:16px;"></td></tr>

  <!-- SCRAPE STATUS -->
  <tr><td style="background:#fff;border-radius:12px;padding:20px;
                 box-shadow:0 1px 3px rgba(0,0,0,0.08);">
    <div style="font-size:16px;font-weight:800;color:#1f2937;margin-bottom:14px;">
      📡 Today's Scrape Status
    </div>
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <thead><tr style="background:#f9fafb;">
        <th style="padding:9px 12px;text-align:left;font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;">Bank</th>
        <th style="padding:9px 12px;text-align:center;font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;">Status</th>
        <th style="padding:9px 12px;text-align:center;font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;">Count</th>
      </tr></thead>
      <tbody>{scrape_rows}</tbody>
    </table>
  </td></tr>
  <tr><td style="height:16px;"></td></tr>

  {insights_row}
  {new_section_html}

  <!-- ALL ACTIVE PROMOTIONS -->
  <tr><td style="background:#fff;border-radius:12px;padding:24px;
                 box-shadow:0 1px 3px rgba(0,0,0,0.08);">
    <div style="font-size:18px;font-weight:800;color:#1f2937;margin-bottom:20px;">
      🎯 All Active Promotions
    </div>
    {promos_html}
  </td></tr>
  <tr><td style="height:16px;"></td></tr>

  <!-- FOOTER -->
  <tr><td style="text-align:center;padding:12px;">
    <div style="font-size:12px;color:#9ca3af;">
      VBank Tracker • Auto-generated daily at 09:00 HKT<br>
      Data sourced from official bank websites only
    </div>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# ── Sender ────────────────────────────────────────────────────────────────────

def send_email(
    html_content: str,
    subject:      str = None,
    recipient:    str = None,
    new_promos:   list = None,
    promotions_data: list = None,
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
    email_to = (
        recipient                    or
        os.getenv('RECIPIENT_EMAIL') or
        os.getenv('EMAIL_RECIPIENT') or
        os.getenv('EMAIL_TO')
    )

    if not all([smtp_user, smtp_pass, email_to]):
        missing = [
            name for name, val in [
                ('GMAIL_ADDRESS',      smtp_user),
                ('GMAIL_APP_PASSWORD', smtp_pass),
                ('RECIPIENT_EMAIL',    email_to),
            ] if not val
        ]
        print(f'❌ Missing env vars: {", ".join(missing)}')
        return False

    subject = subject or f'🏦 VBank Daily Report — {datetime.now().strftime("%d %b %Y")}'

    # FIX: add plain-text alternative to satisfy RFC 2822 and reduce spam
    # score.  SpamAssassin penalises messages that have HTML but no text part.
    now_str    = datetime.now().strftime('%d %b %Y, %H:%M HKT')
    plain_text = _build_plain_text(
        promotions_data or [],
        new_promos      or [],
        now_str,
    )

    msg            = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = smtp_user
    msg['To']      = email_to
    # text/plain must be attached BEFORE text/html per RFC 2045 — mail
    # clients display the last part they can render, so HTML wins.
    msg.attach(MIMEText(plain_text,   'plain', 'utf-8'))
    msg.attach(MIMEText(html_content, 'html',  'utf-8'))

    # FIX: SMTP retry with exponential backoff.
    # Transient failures (connection reset, 421 rate-limit) are common with
    # Gmail at 09:00 when many services send at once.
    for attempt in range(1, _SMTP_MAX_RETRIES + 1):
        try:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(smtp_user, smtp_pass)
                server.sendmail(smtp_user, [email_to], msg.as_string())
            print(f'✅ Email sent → {email_to}')
            return True
        except smtplib.SMTPException as exc:
            if attempt < _SMTP_MAX_RETRIES:
                wait = 2 ** attempt   # 2 s, 4 s
                print(f'  ⚠️  SMTP attempt {attempt} failed: {exc} — retrying in {wait}s…')
                time.sleep(wait)
            else:
                print(f'❌ Email send failed after {_SMTP_MAX_RETRIES} attempts: {exc}')
                return False
        except Exception as exc:
            # Non-SMTP errors (e.g. DNS failure) are not retried.
            print(f'❌ Email send error: {exc}')
            return False

    return False