# scripts/emailer.py

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime


# ── Colour maps ────────────────────────────────────────────────────────────────

BANK_COLORS = {
    "ZA Bank":      "#e63946",
    "Mox Bank":     "#ff6b35",
    "WeLab Bank":   "#2196f3",
    "Ant Bank":     "#1890ff",
    "Livi Bank":    "#722ed1",
    "PAOB":         "#00b96b",
    "Airstar Bank": "#13c2c2",
    "Fusion Bank":  "#f97316",
}

CATEGORY_COLORS = {
    "investment": "#6366f1",
    "spending":   "#f59e0b",
    "cashback":   "#f59e0b",
    "travel":     "#06b6d4",
    "welcome":    "#10b981",
    "loan":       "#ef4444",
    "fx":         "#8b5cf6",
    "fund":       "#3b82f6",
    "referral":   "#ec4899",
}

CATEGORY_EMOJIS = {
    "investment": "📈", "spending": "💳", "cashback": "💳",
    "welcome":    "🎁", "bonus":    "🎁", "travel":   "✈️",
    "loan":       "💰", "fx":       "🌐", "currency": "🌐",
    "fund":       "📊", "referral": "👥",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _bank_color(bank_name: str) -> str:
    for key, color in BANK_COLORS.items():
        if key.lower() in (bank_name or "").lower():
            return color
    return "#6b7280"


def _type_color(type_str: str) -> str:
    t = (type_str or "").lower()
    for key, color in CATEGORY_COLORS.items():
        if key in t:
            return color
    return "#6b7280"


def _cat_emoji(category: str) -> str:
    c = (category or "").lower()
    for key, em in CATEGORY_EMOJIS.items():
        if key in c:
            return em
    return "🏆"


def _tag(text: str, bg: str) -> str:
    return (
        f'<span style="display:inline-block;padding:2px 9px;margin:2px;'
        f'border-radius:20px;font-size:11px;color:#fff;font-weight:700;'
        f'background:{bg};">{text}</span>'
    )


def _types_to_list(types_raw) -> list:
    """
    ✅ FIX 5: Safely convert types field to a clean list regardless of
    whether it arrived as a list, comma-separated string, or other.
    """
    if isinstance(types_raw, list):
        return [str(t).strip() for t in types_raw if str(t).strip()]
    if isinstance(types_raw, str):
        return [t.strip() for t in types_raw.split(",") if t.strip()]
    return []


# ── Promotion card ─────────────────────────────────────────────────────────────

def _promo_card(promo: dict, color: str) -> str:
    title     = (promo.get("title") or promo.get("name") or "Untitled")[:100]
    highlight = promo.get("highlight") or promo.get("description") or ""
    period    = promo.get("period") or promo.get("validity") or "Ongoing"
    types_raw = promo.get("types") or promo.get("type") or ""

    # ✅ FIX 4 + 5: convert to list first, cap at 4 items, THEN join HTML
    type_list = _types_to_list(types_raw)[:4]
    tags_html = "".join(_tag(t, _type_color(t)) for t in type_list)

    return f"""
<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:10px;">
<tr><td style="background:#ffffff;border-radius:10px;padding:14px 16px;
               border-left:4px solid {color};
               box-shadow:0 1px 3px rgba(0,0,0,0.08);">
  <div style="font-weight:700;font-size:14px;color:#1f2937;margin-bottom:6px;">{title}</div>
  <div style="font-size:13px;color:#4b5563;line-height:1.6;margin-bottom:8px;">{highlight}</div>
  <div>
    <span style="font-size:11px;color:#9ca3af;margin-right:8px;">📅 {period}</span>
    {tags_html}
  </div>
</td></tr>
</table>"""


# ── Bank section (header + all cards) ─────────────────────────────────────────

def _bank_section(bank_name: str, promos: list) -> str:
    color = _bank_color(bank_name)
    count = len(promos)
    cards = "".join(_promo_card(p, color) for p in promos)

    return f"""
<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
<tr><td style="padding-bottom:10px;border-bottom:3px solid {color};">
  <table width="100%" cellpadding="0" cellspacing="0"><tr>
    <td style="vertical-align:middle;">
      <span style="display:inline-block;width:12px;height:12px;border-radius:50%;
                   background:{color};vertical-align:middle;margin-right:8px;"></span>
      <span style="font-weight:800;font-size:17px;color:#1f2937;vertical-align:middle;">
        {bank_name}
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


# ── Strategic Insights renderer ────────────────────────────────────────────────

def _insights_html(insights: dict) -> str:
    if not insights:
        return ""

    # ── Best-for table ──────────────────────────────
    best_rows = ""
    for item in insights.get("best_for", []):
        cat    = item.get("category", "")
        bank   = item.get("bank", "")
        detail = item.get("detail", "")
        bc     = _bank_color(bank)
        em     = _cat_emoji(cat)
        best_rows += f"""
<tr style="border-bottom:1px solid #f3f4f6;">
  <td style="padding:9px 12px;font-size:13px;color:#374151;font-weight:600;white-space:nowrap;">
    {em}&nbsp;Best for {cat}
  </td>
  <td style="padding:9px 12px;white-space:nowrap;">
    <span style="background:{bc};color:#fff;padding:3px 10px;border-radius:20px;
                 font-size:12px;font-weight:700;">{bank}</span>
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
                 font-weight:700;text-transform:uppercase;letter-spacing:.05em;">Category</th>
      <th style="padding:9px 12px;text-align:left;font-size:11px;color:#6b7280;
                 font-weight:700;text-transform:uppercase;letter-spacing:.05em;">Winner</th>
      <th style="padding:9px 12px;text-align:left;font-size:11px;color:#6b7280;
                 font-weight:700;text-transform:uppercase;letter-spacing:.05em;">Why</th>
    </tr>
  </thead>
  <tbody>{best_rows}</tbody>
</table>"""

    # ── Per-bank analysis cards ─────────────────────
    bank_cards = ""
    sorted_banks = sorted(
        insights.get("bank_analysis", {}).items(),
        key=lambda x: (0 if "za" in x[0].lower() else 1, x[0]),
    )

    for bank_name, data in sorted_banks:
        bc        = _bank_color(bank_name)
        count     = data.get("count", 0)
        focus     = data.get("focus", "")
        strengths = data.get("strengths", [])[:3]
        expiring  = data.get("expiring_alert", "")
        pros      = data.get("vs_za_pros")
        cons      = data.get("vs_za_cons")
        is_za     = "za" in bank_name.lower()

        hdr_bg    = bc if is_za else "#f9fafb"
        hdr_color = "#ffffff" if is_za else "#1f2937"
        cnt_bg    = "rgba(255,255,255,0.25)" if is_za else "#e5e7eb"

        strengths_html = "".join(
            f'<tr><td style="padding:3px 0;font-size:13px;color:#374151;">✓&nbsp;{s}</td></tr>'
            for s in strengths
        )

        expiring_html = (
            f'<table width="100%" cellpadding="0" cellspacing="0" style="margin-top:10px;">'
            f'<tr><td style="background:#fef3c7;border:1px solid #fcd34d;border-radius:8px;'
            f'padding:7px 12px;font-size:12px;color:#92400e;font-weight:600;">'
            f'⚡&nbsp;{expiring}</td></tr></table>'
            if expiring else ""
        )

        if is_za:
            vs_za_html = (
                '<table width="100%" cellpadding="0" cellspacing="0" style="margin-top:10px;">'
                '<tr><td style="background:#fef2f2;border-radius:8px;padding:6px 12px;'
                'font-size:12px;color:#dc2626;font-weight:700;">'
                '🏆 Base Comparison Bank</td></tr></table>'
            )
        elif pros or cons:
            pros_row = (
                f'<tr><td style="padding:2px 0;font-size:12px;color:#059669;">✅&nbsp;{pros}</td></tr>'
                if pros else ""
            )
            cons_row = (
                f'<tr><td style="padding:2px 0;font-size:12px;color:#dc2626;">❌&nbsp;{cons}</td></tr>'
                if cons else ""
            )
            vs_za_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="margin-top:10px;border-top:1px solid #f3f4f6;">
  <tr><td style="font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;
                 padding:8px 0 4px;letter-spacing:.05em;">vs ZA Bank</td></tr>
  {pros_row}{cons_row}
</table>"""
        else:
            vs_za_html = ""

        bank_cards += f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="margin-bottom:16px;border:1px solid #e5e7eb;border-radius:12px;
              overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,0.06);">
  <tr>
    <td style="background:{hdr_bg};padding:12px 16px;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td style="vertical-align:middle;">
          <span style="font-weight:800;font-size:15px;color:{hdr_color};">{bank_name}</span>
        </td>
        <td style="text-align:right;vertical-align:middle;">
          <span style="background:{cnt_bg};color:{hdr_color};padding:2px 10px;
                       border-radius:20px;font-size:12px;font-weight:700;">{count} active</span>
        </td>
      </tr></table>
    </td>
  </tr>
  <tr>
    <td style="background:#ffffff;padding:14px 16px;">
      <div style="font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;
                  letter-spacing:.05em;margin-bottom:4px;">Focus</div>
      <div style="font-size:13px;color:#374151;margin-bottom:12px;">{focus}</div>
      <div style="font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;
                  letter-spacing:.05em;margin-bottom:6px;">Key Strengths</div>
      <table cellpadding="0" cellspacing="0">{strengths_html}</table>
      {expiring_html}
      {vs_za_html}
    </td>
  </tr>
</table>"""

    return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#f8fafc;border-radius:16px;">
  <tr><td style="padding:24px;">
    <div style="font-size:20px;font-weight:800;color:#1f2937;margin-bottom:4px;">
      🧠 Strategic Insights
    </div>
    <div style="font-size:13px;color:#6b7280;margin-bottom:20px;">
      AI-generated analysis • Updated daily • Base comparison: ZA Bank
    </div>
    <div style="font-size:12px;font-weight:700;color:#374151;text-transform:uppercase;
                letter-spacing:.05em;margin-bottom:12px;">🏆 Best in Category</div>
    {best_table}
    <div style="font-size:12px;font-weight:700;color:#374151;text-transform:uppercase;
                letter-spacing:.05em;margin-bottom:14px;">📋 Bank-by-Bank Analysis</div>
    {bank_cards}
  </td></tr>
</table>"""


# ── Main builder ───────────────────────────────────────────────────────────────

def build_html_email(
    promotions_data: list,
    scraped_data: dict,
    strategic_insights: dict = None,
) -> str:
    now = datetime.now().strftime("%d %b %Y, %H:%M HKT")

    # ✅ FIX 3: Use 'bName' (display name set by _stamp()) with fallbacks
    banks: dict = {}
    for p in promotions_data or []:
        bank = p.get("bName") or p.get("bank_name") or p.get("bank") or "Unknown"
        banks.setdefault(bank, []).append(p)

    total_promos = len(promotions_data or [])
    total_banks  = len(banks)

    this_month = datetime.now().strftime("%b").lower()
    next_month = ["jan","feb","mar","apr","may","jun",
                  "jul","aug","sep","oct","nov","dec"][datetime.now().month % 12].lower()
    expiring_count = sum(
        1 for p in (promotions_data or [])
        if this_month in str(p.get("period", "")).lower()
        or next_month in str(p.get("period", "")).lower()
    )

    # ✅ FIX 6: Accept both boolean 'success' and string 'status' from scraper
    scrape_rows = ""
    for bank_name, result in sorted((scraped_data or {}).items()):
        raw_status = result.get("status")
        raw_ok     = result.get("success")
        ok = (raw_status == "success") or (raw_ok is True)
        # Count: prefer explicit key, fall back to matching promotions list
        count = result.get("count") or len(banks.get(bank_name, []))
        dot   = "#10b981" if ok else "#ef4444"
        label = f"{'✅' if ok else '❌'} {'success' if ok else (raw_status or 'failed')}"
        scrape_rows += f"""
<tr style="border-bottom:1px solid #f3f4f6;">
  <td style="padding:9px 12px;font-size:13px;color:#374151;font-weight:600;">{bank_name}</td>
  <td style="padding:9px 12px;text-align:center;font-size:13px;color:{dot};">{label}</td>
  <td style="padding:9px 12px;text-align:center;font-size:14px;font-weight:800;color:#6366f1;">
    {count}
  </td>
</tr>"""

    sorted_banks = sorted(
        banks.items(), key=lambda x: (0 if "za" in x[0].lower() else 1, x[0])
    )
    promos_html = "".join(_bank_section(bname, bpromos) for bname, bpromos in sorted_banks)

    insights_block = _insights_html(strategic_insights) if strategic_insights else ""
    insights_row = (
        f"<tr><td>{insights_block}</td></tr><tr><td style='height:16px;'></td></tr>"
        if insights_block else ""
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
<tr><td align="center" style="padding:20px 10px;">
<table width="100%" cellpadding="0" cellspacing="0" style="max-width:660px;">

  <!-- HEADER -->
  <tr><td style="background:linear-gradient(135deg,#e63946 0%,#c1121f 100%);
                 border-radius:16px;padding:28px;text-align:center;">
    <div style="font-size:36px;margin-bottom:8px;">🏦</div>
    <div style="font-size:22px;font-weight:800;color:#fff;letter-spacing:.02em;">
      VBank Tracker Daily Report
    </div>
    <div style="font-size:13px;color:rgba(255,255,255,0.75);margin-top:6px;">{now}</div>
  </td></tr>
  <tr><td style="height:16px;"></td></tr>

  <!-- STATS -->
  <tr><td style="background:#fff;border-radius:12px;
                 box-shadow:0 1px 3px rgba(0,0,0,0.08);">
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td width="33%" style="text-align:center;padding:18px 12px;border-right:1px solid #f3f4f6;">
        <div style="font-size:30px;font-weight:800;color:#6366f1;">{total_promos}</div>
        <div style="font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;margin-top:4px;">
          Active Promos
        </div>
      </td>
      <td width="33%" style="text-align:center;padding:18px 12px;border-right:1px solid #f3f4f6;">
        <div style="font-size:30px;font-weight:800;color:#10b981;">{total_banks}</div>
        <div style="font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;margin-top:4px;">
          Banks Tracked
        </div>
      </td>
      <td width="33%" style="text-align:center;padding:18px 12px;">
        <div style="font-size:30px;font-weight:800;color:#f59e0b;">{expiring_count}</div>
        <div style="font-size:11px;color:#6b7280;font-weight:700;text-transform:uppercase;margin-top:4px;">
          Expiring Soon
        </div>
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
      <thead>
        <tr style="background:#f9fafb;">
          <th style="padding:9px 12px;text-align:left;font-size:11px;color:#6b7280;
                     font-weight:700;text-transform:uppercase;letter-spacing:.05em;">Bank</th>
          <th style="padding:9px 12px;text-align:center;font-size:11px;color:#6b7280;
                     font-weight:700;text-transform:uppercase;letter-spacing:.05em;">Status</th>
          <th style="padding:9px 12px;text-align:center;font-size:11px;color:#6b7280;
                     font-weight:700;text-transform:uppercase;letter-spacing:.05em;">Count</th>
        </tr>
      </thead>
      <tbody>{scrape_rows}</tbody>
    </table>
  </td></tr>
  <tr><td style="height:16px;"></td></tr>

  <!-- STRATEGIC INSIGHTS -->
  {insights_row}

  <!-- ALL PROMOTIONS -->
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
      VBank Tracker • Auto-generated daily report<br>
      Data sourced from official bank websites
    </div>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# ── Sender ─────────────────────────────────────────────────────────────────────

def send_email(
    html_content: str,
    subject: str = None,
    recipient: str = None,
) -> bool:
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))

    # ✅ added GMAIL_ADDRESS as fallback
    smtp_user = (
        os.getenv("GMAIL_ADDRESS")
        or os.getenv("SMTP_USER")
        or os.getenv("EMAIL_FROM")
    )
    # ✅ added GMAIL_APP_PASSWORD as fallback
    smtp_pass = (
        os.getenv("GMAIL_APP_PASSWORD")
        or os.getenv("SMTP_PASS")
        or os.getenv("EMAIL_PASS")
    )
    # ✅ RECIPIENT_EMAIL first — matches your GitHub Secret name
    email_to = (
        recipient
        or os.getenv("RECIPIENT_EMAIL")
        or os.getenv("EMAIL_RECIPIENT")
        or os.getenv("EMAIL_TO")
    )

    if not all([smtp_user, smtp_pass, email_to]):
        missing = [
            name for name, val in [
                ("GMAIL_ADDRESS / SMTP_USER / EMAIL_FROM",           smtp_user),
                ("GMAIL_APP_PASSWORD / SMTP_PASS / EMAIL_PASS",      smtp_pass),
                ("RECIPIENT_EMAIL / EMAIL_RECIPIENT / EMAIL_TO",     email_to),
            ]
            if not val
        ]
        print(f"❌ Missing env vars: {', '.join(missing)}")
        return False

    subject = subject or f"🏦 VBank Daily Report — {datetime.now().strftime('%d %b %Y')}"

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = smtp_user
    msg["To"]      = email_to
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, [email_to], msg.as_string())
        print(f"✅ Email sent → {email_to}")
        return True
    except Exception as e:
        print(f"❌ Email send failed: {e}")
        return False