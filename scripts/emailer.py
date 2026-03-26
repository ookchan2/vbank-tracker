# scripts/emailer.py
import os
import json
import smtplib
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from collections import defaultdict

# ── Language ──────────────────────────────────────────────────────────────────
LANGUAGE = os.getenv('LANGUAGE', 'zh_HK')

UI_TEXT = {
    'zh_HK': {
        'title':          '🏦 香港虛擬銀行優惠追蹤',
        'subtitle':       '每日摘要',
        'total_active':   '有效優惠',
        'new_today':      '今日新增',
        'expired_today':  '今日到期',
        'expiring_7d':    '7日內到期',
        'scrape_status':  '🔍 今日抓取狀態',
        'by_bank':        '📊 各銀行有效優惠',
        'all_promos':     '📋 全部有效優惠',
        'new_section':    '🎉 今日新增優惠',
        'expiring_sec':   '⚠️ 即將到期優惠',
        'new_badge':      '新增',
        'expiring_badge': '⚠️ 即將到期',
        'ongoing':        '長期',
        'active_unit':    '個有效',
        'promo_unit':     '個優惠',
        'more_promos':    '更多優惠',
        'no_promos':      '暫無優惠資料。',
        'view_details':   '查看詳情 →',
        'footer':         '由香港虛擬銀行優惠追蹤系統提供',
        'chars_unit':     '字符',
        'subject':        '🏦 香港虛擬銀行優惠 – {date}',
        'plain_text':     '香港虛擬銀行優惠每日摘要 – {date}\n請使用支援 HTML 的郵件客戶端查看。',
    },
    'zh_CN': {
        'title':          '🏦 香港虚拟银行优惠追踪',
        'subtitle':       '每日摘要',
        'total_active':   '有效优惠',
        'new_today':      '今日新增',
        'expired_today':  '今日到期',
        'expiring_7d':    '7日内到期',
        'scrape_status':  '🔍 今日抓取状态',
        'by_bank':        '📊 各银行有效优惠',
        'all_promos':     '📋 全部有效优惠',
        'new_section':    '🎉 今日新增优惠',
        'expiring_sec':   '⚠️ 即将到期优惠',
        'new_badge':      '新增',
        'expiring_badge': '⚠️ 即将到期',
        'ongoing':        '长期',
        'active_unit':    '个有效',
        'promo_unit':     '个优惠',
        'more_promos':    '更多优惠',
        'no_promos':      '暂无优惠资料。',
        'view_details':   '查看详情 →',
        'footer':         '由香港虚拟银行优惠追踪系统提供',
        'chars_unit':     '字符',
        'subject':        '🏦 香港虚拟银行优惠 – {date}',
        'plain_text':     '香港虚拟银行优惠每日摘要 – {date}\n请使用支持 HTML 的邮件客户端查看。',
    },
    'en': {
        'title':          '🏦 HK Virtual Bank Promotions',
        'subtitle':       'Daily Digest',
        'total_active':   'Total Active',
        'new_today':      'New Today',
        'expired_today':  'Expired Today',
        'expiring_7d':    'Expiring ≤7d',
        'scrape_status':  '🔍 Today\'s Scrape Status',
        'by_bank':        '📊 Active Promotions by Bank',
        'all_promos':     '📋 All Active Promotions',
        'new_section':    '🎉 New Promotions Detected Today',
        'expiring_sec':   '⚠️ Promotions Expiring Soon',
        'new_badge':      'NEW',
        'expiring_badge': '⚠️ EXPIRING',
        'ongoing':        'Ongoing',
        'active_unit':    'active',
        'promo_unit':     'promotions',
        'more_promos':    'more promotions',
        'no_promos':      'No promotions found.',
        'view_details':   'View Details →',
        'footer':         'Powered by HK Virtual Bank Promotions Tracker',
        'chars_unit':     'chars',
        'subject':        '🏦 HK Virtual Bank Promotions – {date}',
        'plain_text':     'HK Virtual Bank Promotions Daily Digest – {date}\nPlease view in an HTML-enabled email client.',
    },
}

T = UI_TEXT.get(LANGUAGE, UI_TEXT['zh_HK'])

# ── Brand colours (8 banks) ───────────────────────────────────────────────────
BANK_COLORS = {
    'za':      '#25CD9C',   # ✅ correct ZA Bank green
    'welab':   '#7c3aed',
    'pao':     '#0ea5e9',
    'livi':    '#f97316',
    'airstar': '#06b6d4',
    'fusion':  '#14b8a6',
    'mox':     '#ec4899',
    'ant':     '#1677ff',
}

BANK_NAMES = {
    'za':      'ZA Bank',
    'welab':   'WeLab Bank',
    'pao':     'PAObank',
    'livi':    'livi bank',
    'airstar': 'Airstar Bank',
    'fusion':  'Fusion Bank',
    'mox':     'Mox Bank',
    'ant':     'Ant Bank',
}

# Fixed display order
BANK_ORDER = ['za', 'mox', 'welab', 'livi', 'ant', 'airstar', 'pao', 'fusion']


# ── Helpers ───────────────────────────────────────────────────────────────────
def _today_str() -> str:
    return datetime.now().strftime('%Y-%m-%d')


def _is_expired(end_date: str) -> bool:
    if not end_date:
        return False
    try:
        return datetime.strptime(end_date, '%Y-%m-%d') < datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    except Exception:
        return False


def _expires_within(end_date: str, days: int = 7) -> bool:
    if not end_date:
        return False
    try:
        target = datetime.strptime(end_date, '%Y-%m-%d')
        now    = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        return now <= target <= now + timedelta(days=days)
    except Exception:
        return False


def _parse_types(types_val) -> list:
    """Safely parse types — may be list or JSON string."""
    if isinstance(types_val, list):
        return types_val
    if isinstance(types_val, str):
        try:
            parsed = json.loads(types_val)
            return parsed if isinstance(parsed, list) else [types_val]
        except Exception:
            return [types_val] if types_val else ['Others']
    return ['Others']


def _type_tags_html(types, color: str) -> str:
    tags = _parse_types(types) or ['Others']
    html = ''
    for t in tags[:3]:
        html += (
            f'<span style="background:{color}22;color:{color};font-size:11px;'
            f'font-weight:600;padding:2px 9px;border-radius:99px;margin-right:4px;">'
            f'{t}</span>'
        )
    return html


# ── Single promotion card (full size) ────────────────────────────────────────
def _promo_card_html(p: dict, is_new: bool = False, is_expiring: bool = False) -> str:
    bid    = p.get('bank', '')
    bname  = p.get('bName', BANK_NAMES.get(bid, bid))
    color  = BANK_COLORS.get(bid, '#6b7280')
    name   = p.get('name', 'Promotion')
    hi     = p.get('highlight', '')
    period = p.get('period', T['ongoing'])
    types  = p.get('types', [])
    desc   = p.get('description', '')
    link   = p.get('link', '#')

    # Status badge
    if is_new:
        badge = (
            f'<span style="background:#dcfce7;color:#15803d;font-size:11px;'
            f'font-weight:700;padding:3px 10px;border-radius:5px;margin-left:6px;">'
            f'{T["new_badge"]}</span>'
        )
    elif is_expiring:
        badge = (
            f'<span style="background:#fef3c7;color:#d97706;font-size:11px;'
            f'font-weight:700;padding:3px 10px;border-radius:5px;margin-left:6px;">'
            f'{T["expiring_badge"]}</span>'
        )
    else:
        badge = ''

    hi_html = ''
    if hi:
        hi_html = (
            f'<div style="background:#fef9e7;border-radius:6px;padding:10px 14px;'
            f'margin-bottom:8px;font-size:13px;color:#92400e;">{hi}</div>'
        )

    desc_html = ''
    if desc:
        short = (desc[:180] + '…') if len(desc) > 180 else desc
        desc_html = (
            f'<div style="font-size:12px;color:#6b7280;margin-bottom:8px;'
            f'line-height:1.5;">{short}</div>'
        )

    tags_html = _type_tags_html(types, color)

    return f"""
    <div style="border:1px solid #e5e7eb;border-left:4px solid {color};
                border-radius:10px;padding:16px;margin-bottom:12px;background:#fff;">
      <div style="margin-bottom:10px;">
        <span style="background:{color};color:#fff;font-size:11px;font-weight:700;
                     padding:3px 10px;border-radius:5px;text-transform:uppercase;">{bname}</span>
        {badge}
      </div>
      <div style="font-size:15px;font-weight:700;color:#111827;margin-bottom:8px;">{name}</div>
      {hi_html}
      {desc_html}
      <div style="font-size:12px;color:#9ca3af;margin-bottom:8px;">📅 {period}</div>
      <div style="margin-bottom:8px;">{tags_html}</div>
      <a href="{link}" style="color:{color};text-decoration:none;font-size:12px;font-weight:600;">
        {T['view_details']}
      </a>
    </div>"""


# ── Compact promo row (used in All Active section) ────────────────────────────
def _promo_row_html(p: dict, is_new: bool = False) -> str:
    bid    = p.get('bank', '')
    color  = BANK_COLORS.get(bid, '#6b7280')
    name   = p.get('name', 'Promotion')
    types  = _parse_types(p.get('types', []))
    type_s = ' / '.join(types[:2]) if types else 'Others'

    dot = (
        f'<span style="background:#22c55e;color:#fff;font-size:10px;font-weight:700;'
        f'padding:1px 6px;border-radius:99px;margin-left:6px;">{T["new_badge"]}</span>'
        if is_new else ''
    )

    return f"""
    <tr>
      <td style="padding:7px 0;border-bottom:1px solid #f9fafb;
                 font-size:13px;color:#374151;line-height:1.4;">
        {name}{dot}
      </td>
      <td style="padding:7px 0;border-bottom:1px solid #f9fafb;
                 text-align:right;font-size:11px;color:#9ca3af;
                 white-space:nowrap;padding-left:8px;">
        <span style="color:{color};">{type_s}</span>
      </td>
    </tr>"""


# ── Main builder ──────────────────────────────────────────────────────────────
def build_html_email(promotions_data: list = None,
                     scraped_summary: dict = None) -> str:
    """
    Parameters
    ----------
    promotions_data : list
        Promotion dicts loaded from DB (fields: bank, bName, name, types,
        period, end_date, highlight, description, quota, cost, link,
        first_seen, last_seen, is_active).
    scraped_summary : dict, optional
        { bank_id: { success, chars, count } }
        If omitted, the status section is derived from promotions_data.
    """
    promos   = promotions_data or []
    today    = _today_str()
    date_fmt = datetime.now().strftime('%d %B %Y')

    # ── Classify ─────────────────────────────────────────────────────
    active_p   = [p for p in promos if not _is_expired(p.get('end_date'))]
    new_p      = [p for p in active_p  if p.get('first_seen') == today]
    expiring_p = [p for p in active_p  if _expires_within(p.get('end_date'), 7)]
    expired_p  = [
        p for p in promos
        if p.get('last_seen') == today and _is_expired(p.get('end_date'))
    ]

    total_active  = len(active_p)
    new_today     = len(new_p)
    expired_today = len(expired_p)
    expiring_7d   = len(expiring_p)

    # ── Scrape Status rows ───────────────────────────────────────────
    by_bank: dict = defaultdict(list)
    for p in active_p:
        by_bank[p.get('bank', '')].append(p)

    scrape_rows = ''
    for bid in BANK_ORDER:
        bname = BANK_NAMES.get(bid, bid)
        color = BANK_COLORS.get(bid, '#6b7280')

        if scraped_summary and bid in scraped_summary:
            r       = scraped_summary[bid]
            success = r.get('success', False)
            chars   = r.get('chars', 0)
            count   = r.get('count', 0)
            icon    = '✅' if success else '❌'
            detail  = (
                f'{chars:,} {T["chars_unit"]}'
                f'&nbsp;·&nbsp;{count} {T["promo_unit"]}'
            )
        else:
            cnt     = len(by_bank.get(bid, []))
            success = cnt > 0
            icon    = '✅' if success else '⚪'
            detail  = f'{cnt} {T["active_unit"]}'

        scrape_rows += f"""
        <tr>
          <td style="padding:9px 0;border-bottom:1px solid #f3f4f6;font-size:14px;">
            {icon}&nbsp;
            <span style="color:{color};font-weight:600;">{bname}</span>
          </td>
          <td style="padding:9px 0;border-bottom:1px solid #f3f4f6;
                     text-align:right;color:#9ca3af;font-size:13px;">
            {detail}
          </td>
        </tr>"""

    # ── New Today section ────────────────────────────────────────────
    new_section = ''
    if new_p:
        cards = ''.join(_promo_card_html(p, is_new=True) for p in new_p)
        new_section = f"""
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#fff;border-radius:14px;padding:24px;">
      <div style="font-size:15px;font-weight:700;color:#22c55e;margin-bottom:16px;">
        {T['new_section']} ({new_today})
      </div>
      {cards}
    </div>
  </td></tr>"""

    # ── Expiring Soon section ────────────────────────────────────────
    expiring_section = ''
    if expiring_p:
        cards   = ''.join(_promo_card_html(p, is_expiring=True) for p in expiring_p[:5])
        more_s  = (
            f'<div style="text-align:center;font-size:12px;color:#9ca3af;margin-top:8px;">'
            f'+ {len(expiring_p) - 5} {T["more_promos"]}</div>'
            if len(expiring_p) > 5 else ''
        )
        expiring_section = f"""
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#fff;border-radius:14px;padding:24px;">
      <div style="font-size:15px;font-weight:700;color:#d97706;margin-bottom:16px;">
        {T['expiring_sec']} ({expiring_7d})
      </div>
      {cards}
      {more_s}
    </div>
  </td></tr>"""

    # ── Active by Bank summary table ─────────────────────────────────
    bank_summary_rows = ''
    for bid in BANK_ORDER:
        bname = BANK_NAMES.get(bid, bid)
        color = BANK_COLORS.get(bid, '#6b7280')
        cnt   = len(by_bank.get(bid, []))
        bank_summary_rows += f"""
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #f3f4f6;font-size:14px;">
            <span style="display:inline-block;width:10px;height:10px;border-radius:50%;
                         background:{color};margin-right:8px;vertical-align:middle;"></span>
            {bname}
          </td>
          <td style="padding:10px 0;border-bottom:1px solid #f3f4f6;
                     text-align:right;font-size:13px;color:#6b7280;">
            {cnt} {T['active_unit']}
          </td>
        </tr>"""

    # ── All promotions listed per bank ───────────────────────────────
    all_bank_html = ''
    for bid in BANK_ORDER:
        bank_promos = by_bank.get(bid, [])
        if not bank_promos:
            continue
        bname  = BANK_NAMES.get(bid, bid)
        color  = BANK_COLORS.get(bid, '#6b7280')
        rows   = ''.join(
            _promo_row_html(p, is_new=(p.get('first_seen') == today))
            for p in bank_promos[:12]
        )
        more_s = (
            f'<div style="font-size:12px;color:#9ca3af;text-align:center;margin-top:8px;">'
            f'+ {len(bank_promos) - 12} {T["more_promos"]}</div>'
            if len(bank_promos) > 12 else ''
        )
        all_bank_html += f"""
    <div style="border-left:4px solid {color};padding-left:14px;margin-bottom:22px;">
      <div style="font-size:14px;font-weight:700;color:{color};margin-bottom:10px;">
        {bname}
        <span style="font-size:12px;font-weight:400;color:#9ca3af;">
          &nbsp;{len(bank_promos)} {T['promo_unit']}
        </span>
      </div>
      <table width="100%" cellpadding="0" cellspacing="0">{rows}</table>
      {more_s}
    </div>"""

    no_data = (
        f'<div style="color:#9ca3af;font-size:14px;text-align:center;padding:20px 0;">'
        f'{T["no_promos"]}</div>'
    )

    # ── Full HTML ─────────────────────────────────────────────────────
    return f"""<!DOCTYPE html>
<html lang="zh-HK">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="margin:0;padding:0;background:#f3f4f6;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#f3f4f6;padding:24px 12px;">
<tr><td>
<table width="600" cellpadding="0" cellspacing="0"
       style="max-width:600px;margin:0 auto;width:100%;">

  <!-- HEADER -->
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#111827;border-radius:14px;padding:28px 24px;text-align:center;">
      <div style="font-size:22px;font-weight:800;color:#fff;">{T['title']}</div>
      <div style="color:#6b7280;font-size:13px;margin-top:6px;">
        {date_fmt} · {T['subtitle']}
      </div>
    </div>
  </td></tr>

  <!-- STATS -->
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#fff;border-radius:14px;padding:24px;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td style="text-align:center;padding:8px 4px;">
          <div style="font-size:36px;font-weight:900;color:#f97316;line-height:1.1;">{total_active}</div>
          <div style="font-size:12px;color:#9ca3af;margin-top:6px;">{T['total_active']}</div>
        </td>
        <td style="text-align:center;padding:8px 4px;border-left:1px solid #f3f4f6;">
          <div style="font-size:36px;font-weight:900;color:#22c55e;line-height:1.1;">{new_today}</div>
          <div style="font-size:12px;color:#9ca3af;margin-top:6px;">{T['new_today']}</div>
        </td>
        <td style="text-align:center;padding:8px 4px;border-left:1px solid #f3f4f6;">
          <div style="font-size:36px;font-weight:900;color:#ef4444;line-height:1.1;">{expired_today}</div>
          <div style="font-size:12px;color:#9ca3af;margin-top:6px;">{T['expired_today']}</div>
        </td>
        <td style="text-align:center;padding:8px 4px;border-left:1px solid #f3f4f6;">
          <div style="font-size:36px;font-weight:900;color:#eab308;line-height:1.1;">{expiring_7d}</div>
          <div style="font-size:12px;color:#9ca3af;margin-top:6px;">{T['expiring_7d']}</div>
        </td>
      </tr></table>
    </div>
  </td></tr>

  <!-- SCRAPE STATUS -->
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#fff;border-radius:14px;padding:24px;">
      <div style="font-size:15px;font-weight:700;color:#111827;margin-bottom:14px;">
        {T['scrape_status']}
      </div>
      <table width="100%" cellpadding="0" cellspacing="0">
        {scrape_rows}
      </table>
    </div>
  </td></tr>

  {new_section}
  {expiring_section}

  <!-- ACTIVE BY BANK SUMMARY -->
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#fff;border-radius:14px;padding:24px;">
      <div style="font-size:15px;font-weight:700;color:#111827;margin-bottom:14px;">
        {T['by_bank']}
      </div>
      <table width="100%" cellpadding="0" cellspacing="0">
        {bank_summary_rows}
      </table>
    </div>
  </td></tr>

  <!-- ALL PROMOTIONS DETAIL -->
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#fff;border-radius:14px;padding:24px;">
      <div style="font-size:15px;font-weight:700;color:#111827;margin-bottom:20px;">
        {T['all_promos']}
      </div>
      {all_bank_html or no_data}
    </div>
  </td></tr>

  <!-- FOOTER -->
  <tr><td>
    <div style="background:#111827;border-radius:14px;padding:16px;text-align:center;">
      <div style="color:#4b5563;font-size:12px;">
        {T['footer']} &nbsp;·&nbsp; {date_fmt}
      </div>
    </div>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# ── send_email ────────────────────────────────────────────────────────────────
def send_email(html_content: str):
    """
    Send pre-built HTML email via Gmail SMTP.
    main.py calls build_html_email() first, then passes result here.

    Required env vars:
        GMAIL_ADDRESS       sender Gmail address
        GMAIL_APP_PASSWORD  Gmail App Password (not account password)
        EMAIL_RECIPIENT     recipient address
    Optional:
        LANGUAGE            zh_HK (default) | zh_CN | en
    """
    sender    = os.environ.get('GMAIL_ADDRESS', '').strip()
    password  = os.environ.get('GMAIL_APP_PASSWORD', '').strip()
    recipient = os.environ.get('EMAIL_RECIPIENT', '').strip()

    if not all([sender, password, recipient]):
        raise ValueError(
            'Missing required env vars: GMAIL_ADDRESS / GMAIL_APP_PASSWORD / EMAIL_RECIPIENT'
        )

    date_fmt = datetime.now().strftime('%d %B %Y')
    subject  = T['subject'].format(date=date_fmt)
    plain    = T['plain_text'].format(date=date_fmt)

    print('=' * 52)
    print('📧  SENDING EMAIL')
    print('=' * 52)
    print(f'  📤 From:    {sender}')
    print(f'  📥 To:      {recipient}')
    print(f'  📌 Subject: {subject}')
    print(f'  🌐 Lang:    {LANGUAGE}')
    print(f'  🔌 Connecting to smtp.gmail.com:465 …')

    msg            = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = sender
    msg['To']      = recipient

    msg.attach(MIMEText(plain,        'plain', 'utf-8'))
    msg.attach(MIMEText(html_content, 'html',  'utf-8'))

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(sender, password)
        smtp.sendmail(sender, recipient, msg.as_string())

    print(f'  ✅ Email sent successfully to {recipient}!')
    print('=' * 52)