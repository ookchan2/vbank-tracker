# scripts/emailer.py
"""
VBank Tracker — daily HTML email digest.
"""

from __future__ import annotations

import smtplib
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional


# ── Colour maps ───────────────────────────────────────────────────────────────

_BANK_COLORS: dict[str, str] = {
    "ZA Bank":      "#25CD9C",
    "Mox Bank":     "#ec4899",
    "WeLab Bank":   "#7c3aed",
    "livi bank":    "#f97316",
    "PAObank":      "#0ea5e9",
    "Airstar Bank": "#06b6d4",
    "Fusion Bank":  "#14b8a6",
    "Ant Bank":     "#1677ff",
}

_CAT_COLORS: dict[str, str] = {
    "迎新":   "#10b981",
    "消費":   "#f59e0b",
    "投資":   "#6366f1",
    "旅遊":   "#06b6d4",
    "保險":   "#ef4444",
    "貸款":   "#dc2626",
    "存款":   "#3b82f6",
    "定期存款": "#0369a1",
    "外匯":   "#8b5cf6",
    "推薦":   "#ec4899",
    "新資金": "#0ea5e9",
    "Others": "#6b7280",
}


def _bank_color(name: str) -> str:
    n = (name or "").lower()
    for k, v in _BANK_COLORS.items():
        if k.lower() in n:
            return v
    return "#6b7280"


def _status(p: dict) -> tuple[str, str]:
    """Return (label, hex_colour)."""
    if not p.get("active"):
        return "Expired", "#9ca3af"
    end = p.get("end_date")
    if not end or (p.get("period") or "").lower() == "ongoing":
        return "Valid", "#10b981"
    try:
        diff = (datetime.strptime(end, "%Y-%m-%d").date() - date.today()).days
        if diff < 0:
            return "Expired", "#9ca3af"
        if diff <= 30:
            return f"{diff}d left", "#f97316"
        return "Valid", "#10b981"
    except Exception:
        return "Valid", "#10b981"


def _cat_tags(types: list[str]) -> str:
    html = []
    for t in types or []:
        color = "#6b7280"
        for k, v in _CAT_COLORS.items():
            if k.lower() in t.lower():
                color = v
                break
        html.append(
            f'<span style="display:inline-block;background:{color};color:#fff;'
            f'padding:1px 7px;border-radius:20px;font-size:10px;font-weight:700;'
            f'margin:1px 2px;">{t}</span>'
        )
    return "".join(html)


# ── Section builders ──────────────────────────────────────────────────────────

def _new_today_section(new_promotions: dict[str, list]) -> str:
    if not new_promotions:
        return ""

    rows = ""
    total = 0
    for bank_name, promos in new_promotions.items():
        color = _bank_color(bank_name)
        for p in promos:
            label, badge_color = _status(p)
            title     = p.get("title") or p.get("name") or "—"
            highlight = p.get("highlight") or ""
            cats      = _cat_tags(p.get("types") or [])
            link      = p.get("tc_link") or p.get("url") or "#"
            rows += f"""
            <tr>
              <td style="padding:8px 12px;border-bottom:1px solid #fde68a;white-space:nowrap;">
                <span style="display:inline-block;width:8px;height:8px;border-radius:50%;
                             background:{color};margin-right:5px;vertical-align:middle;"></span>
                <b style="color:#92400e;">{bank_name}</b>
              </td>
              <td style="padding:8px 12px;border-bottom:1px solid #fde68a;max-width:260px;">
                <a href="{link}" style="color:#1d4ed8;font-weight:600;text-decoration:none;">
                  {title}</a><br>
                <span style="color:#6b7280;font-size:11px;">{highlight}</span>
              </td>
              <td style="padding:8px 12px;border-bottom:1px solid #fde68a;">{cats}</td>
              <td style="padding:8px 12px;border-bottom:1px solid #fde68a;white-space:nowrap;">
                <span style="background:{badge_color};color:#fff;padding:2px 8px;
                             border-radius:20px;font-size:11px;font-weight:700;">{label}</span>
              </td>
            </tr>"""
            total += 1

    if not total:
        return ""

    return f"""
    <div style="margin-bottom:24px;">
      <div style="background:linear-gradient(to right,#f97316,#fbbf24);
                  border-radius:12px 12px 0 0;padding:14px 18px;">
        <span style="font-size:20px;">🆕</span>
        <span style="color:#fff;font-weight:800;font-size:16px;margin-left:8px;">
          New Promotions Today</span><br>
        <span style="color:#fed7aa;font-size:11px;">今日首次偵測到的優惠</span>
        <span style="float:right;background:rgba(255,255,255,0.25);color:#fff;
                     font-weight:700;padding:3px 12px;border-radius:20px;font-size:13px;">
          {total} new</span>
      </div>
      <div style="background:#fffbeb;border:1px solid #fde68a;border-top:none;
                  border-radius:0 0 12px 12px;overflow-x:auto;">
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <thead>
            <tr style="background:#fef3c7;">
              <th style="text-align:left;padding:8px 12px;color:#92400e;
                         font-size:11px;text-transform:uppercase;">Bank</th>
              <th style="text-align:left;padding:8px 12px;color:#92400e;
                         font-size:11px;text-transform:uppercase;">Promotion</th>
              <th style="text-align:left;padding:8px 12px;color:#92400e;
                         font-size:11px;text-transform:uppercase;">Category</th>
              <th style="text-align:left;padding:8px 12px;color:#92400e;
                         font-size:11px;text-transform:uppercase;">Status</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>"""


def _insights_section(insights: dict) -> str:
    best_for = (insights or {}).get("best_for") or []
    cards = ""
    for item in best_for:
        cat    = item.get("category") or ""
        bank   = item.get("bank")     or ""
        detail = item.get("detail")   or ""
        is_bau = item.get("is_bau",   False)
        if bank.lower() in ("none", "", "n/a"):
            continue
        color    = _bank_color(bank)
        bau_pill = (
            '<span style="background:#64748b;color:#fff;font-size:10px;'
            'padding:1px 6px;border-radius:10px;margin-left:5px;">BAU</span>'
            if is_bau else ""
        )
        cards += f"""
        <div style="border:1px solid #e5e7eb;border-left:4px solid {color};
                    border-radius:8px;padding:10px 14px;margin-bottom:8px;
                    background:#fff;">
          <div style="font-size:11px;color:#6b7280;text-transform:uppercase;
                      font-weight:700;letter-spacing:.04em;">{cat}</div>
          <div style="font-weight:800;color:{color};margin-top:3px;">
            {bank}{bau_pill}</div>
          <div style="font-size:12px;color:#374151;margin-top:3px;">{detail}</div>
        </div>"""

    if not cards:
        return ""

    return f"""
    <div style="margin-bottom:24px;">
      <div style="background:linear-gradient(to right,#4f46e5,#7c3aed);
                  border-radius:12px 12px 0 0;padding:14px 18px;">
        <span style="font-size:18px;">🏆</span>
        <span style="color:#fff;font-weight:800;font-size:16px;margin-left:8px;">
          Best In Market Today</span><br>
        <span style="color:#c7d2fe;font-size:11px;">今日最佳優惠類別</span>
      </div>
      <div style="background:#f5f3ff;border:1px solid #ede9fe;border-top:none;
                  border-radius:0 0 12px 12px;padding:14px;">
        {cards}
      </div>
    </div>"""


def _bank_overview_section(all_promotions: list[dict]) -> str:
    counts: dict[str, dict[str, int]] = {}
    for p in all_promotions:
        if p.get("is_bau"):
            continue
        bname = p.get("bank_name") or "Unknown"
        if bname not in counts:
            counts[bname] = {"active": 0, "expiring": 0}
        label, _ = _status(p)
        if "left" in label:
            counts[bname]["expiring"] += 1
        elif label == "Valid":
            counts[bname]["active"] += 1

    rows = ""
    for bname in sorted(counts):
        color    = _bank_color(bname)
        active   = counts[bname]["active"]
        expiring = counts[bname]["expiring"]
        if expiring:
            exp_html = (
                f'<span style="background:#fed7aa;color:#92400e;font-size:11px;'
                f'font-weight:700;padding:2px 8px;border-radius:20px;">'
                f'⚡ {expiring} expiring</span>'
            )
        else:
            exp_html = "—"
        rows += f"""
        <tr>
          <td style="padding:7px 14px;border-bottom:1px solid #f3f4f6;">
            <span style="display:inline-block;width:8px;height:8px;border-radius:50%;
                         background:{color};margin-right:6px;vertical-align:middle;"></span>
            <b style="color:#374151;">{bname}</b>
          </td>
          <td style="padding:7px 14px;border-bottom:1px solid #f3f4f6;text-align:center;">
            <span style="background:#d1fae5;color:#065f46;font-size:11px;
                         font-weight:700;padding:2px 8px;border-radius:20px;">
              {active} active</span>
          </td>
          <td style="padding:7px 14px;border-bottom:1px solid #f3f4f6;text-align:center;">
            {exp_html}
          </td>
        </tr>"""

    return f"""
    <div style="margin-bottom:24px;">
      <div style="background:linear-gradient(to right,#111827,#374151);
                  border-radius:12px 12px 0 0;padding:14px 18px;">
        <span style="font-size:18px;">🏦</span>
        <span style="color:#fff;font-weight:800;font-size:16px;margin-left:8px;">
          Bank Overview</span>
      </div>
      <div style="border:1px solid #e5e7eb;border-top:none;
                  border-radius:0 0 12px 12px;overflow:hidden;">
        <table style="width:100%;border-collapse:collapse;font-size:13px;background:#fff;">
          <thead>
            <tr style="background:#f9fafb;">
              <th style="text-align:left;padding:8px 14px;color:#6b7280;
                         font-size:11px;text-transform:uppercase;">Bank</th>
              <th style="text-align:center;padding:8px 14px;color:#6b7280;
                         font-size:11px;text-transform:uppercase;">Active</th>
              <th style="text-align:center;padding:8px 14px;color:#6b7280;
                         font-size:11px;text-transform:uppercase;">Expiring ≤30d</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>"""


# ── Public API ────────────────────────────────────────────────────────────────

def send_daily_email(
    sender_address: str,
    app_password:   str,
    recipient:      str,
    all_promotions: list[dict],
    new_promotions: dict[str, list],
    insights:       Optional[dict] = None,
    run_date:       str            = "",
) -> None:
    run_date  = run_date or datetime.now().strftime("%Y-%m-%d")
    new_total = sum(len(v) for v in new_promotions.values())
    non_bau   = [p for p in all_promotions if not p.get("is_bau")]
    total_n   = len(non_bau)

    subject = (
        f"🏦 VBank Tracker {run_date} — "
        + (f"🆕 {new_total} new promotion(s)" if new_total else "Daily digest")
    )

    body = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background:#f3f4f6;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="max-width:680px;margin:24px auto;background:#fff;border-radius:16px;
              box-shadow:0 4px 24px rgba(0,0,0,0.08);overflow:hidden;">

    <!-- Header -->
    <div style="background:linear-gradient(to right,#111827,#374151);padding:24px 28px;">
      <div style="font-size:24px;font-weight:900;color:#fff;letter-spacing:-0.5px;">
        🏦 HK Virtual Bank Promotions</div>
      <div style="color:#9ca3af;font-size:13px;margin-top:6px;">
        Daily Digest · {run_date} · 香港虛擬銀行優惠追蹤</div>
      <div style="margin-top:16px;display:inline-flex;gap:12px;">
        <div style="background:rgba(255,255,255,0.1);border-radius:8px;
                    padding:8px 18px;text-align:center;display:inline-block;">
          <div style="color:#fff;font-size:22px;font-weight:900;">{total_n}</div>
          <div style="color:#9ca3af;font-size:11px;text-transform:uppercase;">Total</div>
        </div>
        <div style="background:rgba(16,185,129,0.2);border-radius:8px;
                    padding:8px 18px;text-align:center;display:inline-block;">
          <div style="color:#6ee7b7;font-size:22px;font-weight:900;">{new_total}</div>
          <div style="color:#9ca3af;font-size:11px;text-transform:uppercase;">New Today</div>
        </div>
      </div>
    </div>

    <div style="padding:24px 28px;">
      {_new_today_section(new_promotions)}
      {_insights_section(insights)}
      {_bank_overview_section(all_promotions)}

      <div style="margin-top:20px;padding-top:16px;border-top:1px solid #e5e7eb;
                  text-align:center;color:#9ca3af;font-size:11px;">
        <a href="https://vbank-tracker.github.io"
           style="color:#6366f1;font-weight:600;text-decoration:none;">
          🌐 View Live Dashboard ↗
        </a>
        &nbsp;·&nbsp;
        VBank Tracker Bot 🤖 · Auto-generated {run_date} HKT
      </div>
    </div>
  </div>
</body>
</html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender_address
    msg["To"]      = recipient
    msg.attach(MIMEText(body, "html", "utf-8"))

    print(f"\n📧  Sending to {recipient}…")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender_address, app_password)
        smtp.sendmail(sender_address, recipient, msg.as_string())
    print(f"    ✅ Sent: {subject}")