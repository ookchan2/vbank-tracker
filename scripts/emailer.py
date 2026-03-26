# scripts/emailer.py
import os
import json
import smtplib
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from collections import Counter


# ── Parse AI analysis into structured promotions ──────────────────────────────
def parse_promotions(bank_name: str, color: str, analysis) -> list:
    promos = []

    if isinstance(analysis, list):
        for item in analysis:
            if isinstance(item, dict):
                promos.append({
                    "bank":          bank_name,
                    "color":         color,
                    "title":         item.get("title") or item.get("name", "Promotion"),
                    "highlight":     item.get("highlight") or item.get("description", ""),
                    "date_range":    item.get("date_range") or item.get("validity", "Ongoing"),
                    "tags":          item.get("tags") or item.get("categories", []),
                    "is_new":        item.get("is_new", True),
                    "active":        item.get("active", True),
                    "expired_today": item.get("expired_today", False),
                    "expiring_soon": item.get("expiring_soon", False),
                })
        return promos

    if isinstance(analysis, str):
        # Try JSON parse first
        try:
            parsed = json.loads(analysis)
            return parse_promotions(bank_name, color, parsed)
        except Exception:
            pass

        # Fall back: split text into blocks
        blocks = [b.strip() for b in re.split(r'\n\n+', analysis) if len(b.strip()) > 30]
        for block in blocks[:8]:
            title = block.split('\n')[0][:80]
            promos.append({
                "bank":          bank_name,
                "color":         color,
                "title":         title,
                "highlight":     block[:200],
                "date_range":    "See bank website",
                "tags":          [],
                "is_new":        True,
                "active":        True,
                "expired_today": False,
                "expiring_soon": False,
            })

    return promos


# ── Build HTML ────────────────────────────────────────────────────────────────
def build_html_email(scraped_data: dict, promotions_list: list, date_str: str) -> str:

    # Parse all promotions
    all_promos = []
    for p in promotions_list:
        bank_name = p.get("bank", "")
        color     = p.get("color", "#333333")
        analysis  = p.get("promotions", "")
        all_promos.extend(parse_promotions(bank_name, color, analysis))

    # Stats
    active_p   = [p for p in all_promos if p.get("active", True)]
    new_p      = [p for p in all_promos if p.get("is_new", True)]
    expired_p  = [p for p in all_promos if p.get("expired_today", False)]
    expiring_p = [p for p in all_promos if p.get("expiring_soon", False)]

    total_active  = len(active_p)
    new_today     = len(new_p)
    expired_today = len(expired_p)
    expiring_7d   = len(expiring_p)

    # ── Scrape Status rows ────────────────────────────────────────────
    scrape_rows = ""
    for bank_name, r in scraped_data.items():
        success  = r.get("success", False)
        sections = r.get("sections_found", 0)
        color    = r.get("color", "#333333")
        icon     = "✅" if success else "❌"
        scrape_rows += f"""
        <tr>
          <td style="padding:9px 0;border-bottom:1px solid #f3f4f6;font-size:14px;">
            {icon}&nbsp;
            <span style="color:{color};font-weight:600;">{bank_name}</span>
          </td>
          <td style="padding:9px 0;border-bottom:1px solid #f3f4f6;
                     text-align:right;color:#9ca3af;font-size:13px;">
            {sections} sections found
          </td>
        </tr>"""

    # ── Promotion Cards ───────────────────────────────────────────────
    promo_cards = ""
    for p in new_p:
        b_name  = p.get("bank", "")
        b_color = p.get("color", "#333333")
        title   = p.get("title", "")
        hi      = p.get("highlight", "")
        dr      = p.get("date_range", "Ongoing")
        tags    = p.get("tags", [])
        if isinstance(tags, str):
            tags = [tags]
        tags_html = "".join(
            f'<span style="background:#f3f4f6;color:#374151;font-size:11px;'
            f'padding:3px 10px;border-radius:99px;margin-right:4px;">{t}</span>'
            for t in tags[:4]
        )
        promo_cards += f"""
        <div style="border:1px solid #e5e7eb;border-left:4px solid {b_color};
                    border-radius:10px;padding:16px;margin-bottom:12px;background:#fff;">
          <div style="margin-bottom:10px;">
            <span style="background:{b_color};color:#fff;font-size:11px;font-weight:700;
                         padding:3px 10px;border-radius:5px;text-transform:uppercase;">{b_name}</span>
            <span style="background:#dcfce7;color:#15803d;font-size:11px;font-weight:700;
                         padding:3px 10px;border-radius:5px;margin-left:6px;">NEW</span>
          </div>
          <div style="font-size:15px;font-weight:700;color:#111827;margin-bottom:8px;">{title}</div>
          <div style="background:#fef9e7;border-radius:6px;padding:10px 14px;
                      margin-bottom:10px;font-size:13px;color:#92400e;">
            🏆 {hi}
          </div>
          <div style="font-size:12px;color:#9ca3af;margin-bottom:8px;">📅 {dr}</div>
          <div>{tags_html}</div>
        </div>"""

    # ── Active by Bank ────────────────────────────────────────────────
    bank_counts = Counter(p.get("bank") for p in active_p)
    bank_rows = ""
    for b_name, cnt in sorted(bank_counts.items(), key=lambda x: -x[1]):
        b_color = scraped_data.get(b_name, {}).get("color", "#333333")
        bank_rows += f"""
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #f3f4f6;font-size:14px;">
            <span style="display:inline-block;width:10px;height:10px;border-radius:50%;
                         background:{b_color};margin-right:8px;vertical-align:middle;"></span>
            {b_name}
          </td>
          <td style="padding:10px 0;border-bottom:1px solid #f3f4f6;
                     text-align:right;font-size:13px;color:#6b7280;">{cnt} active</td>
        </tr>"""

    # ── New Promotions Section ────────────────────────────────────────
    new_section = ""
    if new_p:
        new_section = f"""
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#fff;border-radius:14px;padding:24px;">
      <div style="font-size:15px;font-weight:700;color:#22c55e;margin-bottom:16px;">
        🎉 {len(new_p)} New Promotion(s) Detected
      </div>
      {promo_cards}
    </div>
  </td></tr>"""

    # ── Full HTML ─────────────────────────────────────────────────────
    return f"""<!DOCTYPE html>
<html lang="en">
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
      <div style="font-size:22px;font-weight:800;color:#fff;">
        🏦 HK Virtual Bank Promotions
      </div>
      <div style="color:#6b7280;font-size:13px;margin-top:6px;">{date_str}</div>
    </div>
  </td></tr>

  <!-- STATS -->
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#fff;border-radius:14px;padding:24px;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td style="text-align:center;padding:8px 4px;">
          <div style="font-size:36px;font-weight:900;color:#f97316;line-height:1.1;">{total_active}</div>
          <div style="font-size:12px;color:#9ca3af;margin-top:6px;">Total Active</div>
        </td>
        <td style="text-align:center;padding:8px 4px;border-left:1px solid #f3f4f6;">
          <div style="font-size:36px;font-weight:900;color:#22c55e;line-height:1.1;">{new_today}</div>
          <div style="font-size:12px;color:#9ca3af;margin-top:6px;">New Today</div>
        </td>
        <td style="text-align:center;padding:8px 4px;border-left:1px solid #f3f4f6;">
          <div style="font-size:36px;font-weight:900;color:#ef4444;line-height:1.1;">{expired_today}</div>
          <div style="font-size:12px;color:#9ca3af;margin-top:6px;">Expired Today</div>
        </td>
        <td style="text-align:center;padding:8px 4px;border-left:1px solid #f3f4f6;">
          <div style="font-size:36px;font-weight:900;color:#eab308;line-height:1.1;">{expiring_7d}</div>
          <div style="font-size:12px;color:#9ca3af;margin-top:6px;">Expiring ≤7d</div>
        </td>
      </tr></table>
    </div>
  </td></tr>

  <!-- SCRAPE STATUS -->
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#fff;border-radius:14px;padding:24px;">
      <div style="font-size:15px;font-weight:700;color:#111827;margin-bottom:14px;">
        🔍 Today's Scrape Status
      </div>
      <table width="100%" cellpadding="0" cellspacing="0">
        {scrape_rows}
      </table>
    </div>
  </td></tr>

  {new_section}

  <!-- ACTIVE BY BANK -->
  <tr><td style="padding:0 0 16px 0;">
    <div style="background:#fff;border-radius:14px;padding:24px;">
      <div style="font-size:15px;font-weight:700;color:#111827;margin-bottom:14px;">
        📊 Active Promotions by Bank
      </div>
      <table width="100%" cellpadding="0" cellspacing="0">
        {bank_rows}
      </table>
    </div>
  </td></tr>

  <!-- FOOTER -->
  <tr><td>
    <div style="background:#111827;border-radius:14px;padding:16px;text-align:center;">
      <div style="color:#4b5563;font-size:12px;">
        Powered by HK Virtual Bank Promotions Tracker &nbsp;·&nbsp; {date_str}
      </div>
    </div>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# ── send_email ────────────────────────────────────────────────────────────────
def send_email(subject: str, text_content: str,
               promotions_data: list, scraped_data: dict = None):

    sender    = os.environ.get('GMAIL_ADDRESS')
    password  = os.environ.get('GMAIL_APP_PASSWORD')
    recipient = os.environ.get('RECIPIENT_EMAIL')

    if not all([sender, password, recipient]):
        raise ValueError("Missing GMAIL_ADDRESS / GMAIL_APP_PASSWORD / RECIPIENT_EMAIL")

    date_str     = datetime.now().strftime('%Y-%m-%d')
    html_content = build_html_email(scraped_data or {}, promotions_data, date_str)

    print("=" * 50)
    print("📧 SENDING EMAIL")
    print("=" * 50)
    print(f"  📤 From: {sender}")
    print(f"  📥 To: {recipient}")
    print(f"  📌 Subject: {subject}")
    print(f"  🔌 Connecting to Gmail...")

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = recipient

    msg.attach(MIMEText(text_content,  "plain", "utf-8"))
    msg.attach(MIMEText(html_content,  "html",  "utf-8"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, password)
        smtp.sendmail(sender, recipient, msg.as_string())

    print(f"  ✅ Email sent successfully to {recipient}!")