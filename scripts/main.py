# scripts/main.py
import os
import sys
import json
import asyncio
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── Helper ────────────────────────────────────────────────────────────────────
def build_fallback_digest(data):
    lines = [f"HK Bank Promotions — {datetime.now().strftime('%Y-%m-%d')}\n"]
    for bank, info in data.items():
        lines.append(f"## {bank}")
        lines.append(str(info.get('analysis', 'No data'))[:400])
        lines.append("")
    return "\n".join(lines)

# ── Banner ────────────────────────────────────────────────────────────────────
print("🚀" * 25)
print("HK BANK PROMOTIONS BOT — STARTING")
print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("🚀" * 25)
print()

# ── PHASE 1: AI ───────────────────────────────────────────────────────────────
print("📡 PHASE 1: Testing AI Connection")
print("-" * 40)

AI_AVAILABLE       = False
analyze_promotions = None
create_digest      = None

try:
    import ai_helper as _ai
    _ai.init_ai()
    analyze_promotions = _ai.analyze_promotions
    create_digest      = _ai.create_digest
    AI_AVAILABLE       = True
    print("✅ AI connection successful")
except Exception as e:
    print(f"❌ AI import error: {e}")
    print("   → AI analysis will be skipped")

print()

# ── PHASE 2: DATABASE ─────────────────────────────────────────────────────────
print("📦 PHASE 2: Setting Up Database")
print("-" * 40)

DB_AVAILABLE = False
try:
    from database import (init_db, save_promotions,
                          get_total_records, log_email,
                          get_total_emails_sent)
    init_db()
    DB_AVAILABLE = True
    print("✅ Database initialized")
except Exception as e:
    print(f"❌ Database error: {e}")

print()

# ── PHASE 3: SCRAPING ─────────────────────────────────────────────────────────
print("🌐 PHASE 3: Scraping Bank Websites")
print("-" * 40)

scraped_data = {}
try:
    from scraper import scrape_all_banks
    scraped_data = asyncio.run(scrape_all_banks())
    print(f"✅ Scraped {len(scraped_data)} banks")
except Exception as e:
    print(f"❌ Scraping error: {e}")

print()

# ── PHASE 4: AI ANALYSIS ──────────────────────────────────────────────────────
print("🤖 PHASE 4: AI Analyzing Promotions")
print("-" * 40)

analyzed_data   = {}
banks_processed = 0

for bank_name, data in scraped_data.items():
    print(f"\n  Processing: {bank_name}")

    if not data.get('success'):
        print(f"  ⚠️  Skipping {bank_name} (scraping failed)")
        continue

    raw_text = data.get('text', '')
    analysis = None

    if AI_AVAILABLE and analyze_promotions is not None:
        print(f"    🤖 AI analyzing {bank_name}...")
        try:
            analysis = analyze_promotions(bank_name, raw_text)
            print(f"    ✅ Done")
            banks_processed += 1
        except Exception as e:
            print(f"    ❌ AI failed for {bank_name}: {e}")
            analysis = raw_text[:500]
    else:
        print(f"    ℹ️  No AI — storing raw text")
        analysis = raw_text[:500]

    analyzed_data[bank_name] = {
        'analysis':   analysis,
        'raw_text':   raw_text,
        'url':        data.get('url', ''),
        'color':      data.get('color', '#333333'),
        'scraped_at': datetime.now().isoformat()
    }

    if DB_AVAILABLE:
        try:
            save_promotions(bank_name, analysis, raw_text)
        except Exception as e:
            print(f"    ⚠️  DB save error: {e}")

print(f"\n✅ AI analysis complete: {banks_processed} banks processed")

# ── SAVE JSON ─────────────────────────────────────────────────────────────────
try:
    docs_dir  = os.path.join(os.path.dirname(__file__), '..', 'docs')
    os.makedirs(docs_dir, exist_ok=True)
    data_file = os.path.join(docs_dir, 'data.json')
    with open(data_file, 'w', encoding='utf-8') as f:
        json.dump({
            'updated_at': datetime.now().isoformat(),
            'banks':      analyzed_data
        }, f, ensure_ascii=False, indent=2)
    print(f"✅ Website data saved: docs/data.json")
except Exception as e:
    print(f"❌ JSON save error: {e}")

print()

# ── PHASE 5: EMAIL DIGEST ─────────────────────────────────────────────────────
print("📝 PHASE 5: Creating Email Digest")
print("-" * 40)

digest_text = None

if AI_AVAILABLE and create_digest is not None and analyzed_data:
    print("🤖 AI creating digest...")
    try:
        digest_text = create_digest(analyzed_data)
        print("✅ Digest created")
    except Exception as e:
        print(f"❌ Digest error: {e}")
        digest_text = build_fallback_digest(analyzed_data)
elif analyzed_data:
    print("ℹ️  Using plain-text digest (no AI)")
    digest_text = build_fallback_digest(analyzed_data)
else:
    print("⚠️  No data to digest")

print()

# ── PHASE 6: SEND EMAIL ───────────────────────────────────────────────────────
print("📧 PHASE 6: Sending Email")
print("-" * 40)

email_sent = False
recipient  = os.environ.get('RECIPIENT_EMAIL')

if not recipient:
    print("❌ RECIPIENT_EMAIL not set in GitHub Secrets!")
elif not digest_text:
    print("❌ No digest content to send")
else:
    try:
        from emailer import send_email

        # Build promotions list for emailer
        promotions_list = [
            {
                "bank":       bank_name,
                "color":      info.get("color", "#0080A1"),
                "promotions": info.get("analysis", "No data")
            }
            for bank_name, info in analyzed_data.items()
        ]

        subject = f"🏦 HK Bank Promotions — {datetime.now().strftime('%Y-%m-%d')}"

        send_email(
            subject=subject,
            text_content=digest_text,
            promotions_data=promotions_list
        )
        email_sent = True
        print(f"✅ Email sent to {recipient}")

        if DB_AVAILABLE:
            log_email(recipient, subject, 'sent')

    except Exception as e:
        print(f"❌ Email failed: {e}")
        if DB_AVAILABLE and recipient:
            log_email(recipient, 'digest', f'failed: {e}')

# ── SUMMARY ───────────────────────────────────────────────────────────────────
total_records = get_total_records()     if DB_AVAILABLE else 0
total_sent    = get_total_emails_sent() if DB_AVAILABLE else 0

print()
print("=" * 50)
print("🏁 PIPELINE COMPLETE")
print("=" * 50)
print(f"🏦 Banks processed : {banks_processed}")
print(f"📧 Email           : {'✅ Sent' if email_sent else '❌ Failed'}")
print(f"📦 DB records      : {total_records}")
print(f"📨 Emails sent     : {total_sent}")
print("=" * 50)