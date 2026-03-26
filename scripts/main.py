# scripts/main.py
import asyncio
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

async def main():
    print("🚀" * 25)
    print("HK BANK PROMOTIONS BOT — STARTING")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🚀" * 25)
    print()

    start_time = datetime.now()
    banks_processed = 0
    email_sent = False

    # ─────────────────────────────────────────
    # PHASE 1: Test AI Connection
    # ─────────────────────────────────────────
    print("📡 PHASE 1: Testing AI Connection")
    print("-" * 40)
    try:
        from ai_helper import test_connection, analyze_promotions, create_digest
        ok, model = test_connection()
        if ok:
            print(f"✅ OpenRouter AI connection successful! Model: {model}")
        else:
            print(f"❌ AI connection failed: {model}")
    except Exception as e:
        print(f"❌ AI import error: {e}")
    print()

    # ─────────────────────────────────────────
    # PHASE 2: Setup Database
    # ─────────────────────────────────────────
    print("📦 PHASE 2: Setting Up Database")
    print("-" * 40)
    try:
        from database import init_db, save_promotions, get_all_promotions
        print("📦 Initializing database...")
        init_db()
        print("✅ Database ready!")
    except Exception as e:
        print(f"❌ Database error: {e}")
    print()

    # ─────────────────────────────────────────
    # PHASE 3: Scrape Bank Websites
    # ─────────────────────────────────────────
    print("🌐 PHASE 3: Scraping Bank Websites")
    print("-" * 40)
    try:
        from scraper import scrape_all_banks
        scrape_results = await scrape_all_banks()
    except Exception as e:
        print(f"❌ Scraping error: {e}")
        scrape_results = []
    print()

    # ─────────────────────────────────────────
    # PHASE 4: AI Analyzing Promotions
    # ─────────────────────────────────────────
    print("🤖 PHASE 4: AI Analyzing Promotions")
    print("-" * 40)
    print()

    all_promotions = []

    for result in scrape_results:
        bank = result["bank"]
        bank_name = bank["name"]

        if not result["success"]:
            print(f"  ⚠️  Skipping {bank_name} (scraping failed)")
            print()
            continue

        print(f"  Processing: {bank_name}")

        try:
            print(f"    🤖 AI analyzing {bank_name} promotions...")
            promotions = analyze_promotions(
                bank_name=bank_name,
                content=result["content"],
                bank_color=bank.get("color", "#ffffff")
            )
            print(f"    ✅ AI extraction complete for {bank_name}")

            # Save to database
            save_promotions(bank_name, promotions)
            print(f"  💾 Saved {bank_name} promotions to database")
            print()

            all_promotions.extend(promotions)
            banks_processed += 1

        except Exception as e:
            print(f"    ❌ AI analysis failed for {bank_name}: {e}")
            print()

    print(f"✅ AI analysis complete: {banks_processed} banks processed")
    print()

    # ─────────────────────────────────────────
    # PHASE 4.5: Save JSON for Website
    # ─────────────────────────────────────────
    try:
        save_website_data(all_promotions)
    except Exception as e:
        print(f"⚠️  Could not save website data: {e}")

    # ─────────────────────────────────────────
    # PHASE 5: Create Email Digest
    # ─────────────────────────────────────────
    print("📝 PHASE 5: Creating Email Digest")
    print("-" * 40)
    print()

    email_subject = ""
    email_body = ""

    try:
        print("🤖 AI creating email digest...")
        email_subject, email_body = create_digest(all_promotions)
        print("✅ Email digest created by AI")
    except Exception as e:
        print(f"❌ Email digest error: {e}")
        email_subject = f"🏦 HK VBank Promotions — {datetime.now().strftime('%Y-%m-%d')}"
        email_body = format_fallback_email(all_promotions)

    print()

    # ─────────────────────────────────────────
    # PHASE 6: Send Email
    # ─────────────────────────────────────────
    print("📧 PHASE 6: Sending Email")
    print("-" * 40)
    print()
    print("=" * 50)
    print("📧 SENDING EMAIL")
    print("=" * 50)

    recipient = os.getenv("RECIPIENT_EMAIL", "")
    gmail_user = os.getenv("GMAIL_ADDRESS", "")
    gmail_pass = os.getenv("GMAIL_APP_PASSWORD", "")

    if not recipient:
        print("❌ Recipient email not configured!")
        print("   → Set RECIPIENT_EMAIL in GitHub Secrets")
    elif not gmail_user or not gmail_pass:
        print("❌ Gmail credentials not configured!")
        print("   → Set GMAIL_ADDRESS and GMAIL_APP_PASSWORD in GitHub Secrets")
    else:
        try:
            from emailer import send_email
            send_email(
                to=recipient,
                subject=email_subject,
                body=email_body,
                gmail_user=gmail_user,
                gmail_pass=gmail_pass
            )
            print(f"✅ Email sent successfully to {recipient}")
            email_sent = True
        except Exception as e:
            print(f"❌ Email send failed: {e}")

    # ─────────────────────────────────────────
    # PIPELINE COMPLETE
    # ─────────────────────────────────────────
    duration = int((datetime.now() - start_time).total_seconds())
    total_records = len(all_promotions)
    emails_sent = 1 if email_sent else 0

    print()
    print("=" * 50)
    print("🏁 PIPELINE COMPLETE")
    print("=" * 50)
    print(f"⏱️  Duration: {duration} seconds")
    print(f"🏦 Banks processed: {banks_processed}")
    print(f"📧 Email: {'✅ Sent' if email_sent else '❌ Failed'}")
    print(f"📦 Total records in database: {total_records}")
    print(f"📨 Total emails sent: {emails_sent}")
    print("=" * 50)


def save_website_data(promotions: list):
    """Save promotions to docs/data.json for GitHub Pages website."""
    os.makedirs("docs", exist_ok=True)

    today = datetime.now().strftime('%Y-%m-%d')
    today_full = datetime.now().strftime('%Y-%m-%d %H:%M')

    output = {
        "lastUpdated": today,
        "lastUpdatedFull": today_full,
        "totalCount": len(promotions),
        "promotions": promotions
    }

    with open("docs/data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"✅ Website data saved: docs/data.json ({len(promotions)} promotions)")


def format_fallback_email(promotions: list) -> str:
    """Fallback plain-text email if AI digest fails."""
    today = datetime.now().strftime('%Y-%m-%d')
    lines = [
        f"HK Virtual Bank Promotions Report",
        f"Date: {today}",
        f"Total Promotions Found: {len(promotions)}",
        "",
        "=" * 40,
    ]

    # Group by bank
    banks = {}
    for p in promotions:
        bank = p.get("bank_name", "Unknown")
        if bank not in banks:
            banks[bank] = []
        banks[bank].append(p)

    for bank_name, promos in banks.items():
        lines.append(f"\n🏦 {bank_name} ({len(promos)} promotions)")
        lines.append("-" * 30)
        for promo in promos[:3]:  # Show max 3 per bank
            lines.append(f"• {promo.get('name', 'N/A')}")
            if promo.get('highlight'):
                lines.append(f"  {promo['highlight']}")
            if promo.get('period'):
                lines.append(f"  📅 {promo['period']}")
        lines.append("")

    lines.append("=" * 40)
    lines.append("HK Virtual Bank Promotions Tracker")
    lines.append("For reference only. Subject to T&C.")

    return "\n".join(lines)


if __name__ == "__main__":
    asyncio.run(main())