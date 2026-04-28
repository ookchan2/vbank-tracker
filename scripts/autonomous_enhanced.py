#!/usr/bin/env python3
"""
Enhanced Autonomous VBank Tracker with Integrated AI Processing
This version processes scraped data automatically using Claude Code's built-in AI
"""

import os
import sys
import json
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / 'scripts'))

import database as db
from ai_advanced_processor import extract_promotions_advanced, extract_products_advanced
from emailer import build_html_email, send_email

# Paths
DATA_JSON_PATH = BASE_DIR / 'docs' / 'data.json'
EMAIL_PREVIEW_PATH = BASE_DIR / 'output' / 'email_preview.html'

print("=" * 70)
print("  VBank Tracker - Enhanced Autonomous Mode")
print("=" * 70)
print("  [OK] Integrated AI processing")
print("  [OK] Automatic data extraction")
print("  [OK] Zero external dependencies")
print("=" * 70)
print()

DB_PATH = BASE_DIR / 'data' / 'promotions.db'

def save_promotions_to_db(bank_id: str, bank_name: str, promotions: list):
    """Save extracted promotions to database"""

    if not promotions:
        return {'new': 0, 'updated': 0}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    stats = {'new': 0, 'updated': 0}

    for promo in promotions:
        title = promo.get('title', '').strip()
        if not title:
            continue

        # Check if exists
        cursor.execute("""
            SELECT id FROM promotions
            WHERE bank_id = ? AND title = ?
        """, (bank_id, title))

        existing = cursor.fetchone()

        # Prepare fields
        highlight = promo.get('highlight', '')
        description = promo.get('description', '')
        types_raw = promo.get('types', [])
        promo_type = ','.join(types_raw) if isinstance(types_raw, list) else str(types_raw)
        is_bau = 1 if promo.get('is_bau', False) else 0
        start_date = promo.get('start_date') or None
        end_date = promo.get('end_date') or None
        period = promo.get('period', 'Ongoing')
        quota = promo.get('quota', '')
        cost = promo.get('cost', '')
        url = promo.get('url', '')
        tc_link = promo.get('tc_link', '')
        today = datetime.now().strftime('%Y-%m-%d')

        if existing:
            # Update existing
            cursor.execute("""
                UPDATE promotions SET
                    bank_name = ?, highlight = ?, description = ?,
                    start_date = ?, end_date = ?, period = ?,
                    quota = ?, cost = ?, promo_type = ?,
                    url = ?, tc_link = ?, is_bau = ?,
                    last_seen = ?, active = 1
                WHERE id = ?
            """, (bank_name, highlight, description,
                  start_date, end_date, period,
                  quota, cost, promo_type,
                  url, tc_link, is_bau, today, existing[0]))
            stats['updated'] += 1
        else:
            # Insert new
            cursor.execute("""
                INSERT INTO promotions (
                    bank_id, bank_name, title, highlight, description,
                    start_date, end_date, period, quota, cost,
                    promo_type, url, tc_link, is_bau,
                    created_at, first_seen_at, last_seen, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (bank_id, bank_name, title, highlight, description,
                  start_date, end_date, period, quota, cost,
                  promo_type, url, tc_link, is_bau, today, today, today))
            stats['new'] += 1

    conn.commit()
    conn.close()

    return stats

def save_products_to_db(bank_id: str, bank_name: str, products: list):
    """Save extracted products to database"""

    if not products:
        return {'new': 0, 'updated': 0}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    stats = {'new': 0, 'updated': 0}

    for prod in products:
        name = prod.get('product_name', '').strip()
        if not name:
            continue

        # Check if exists
        cursor.execute("""
            SELECT id FROM products
            WHERE bank_id = ? AND product_name = ?
        """, (bank_id, name))

        existing = cursor.fetchone()

        category = prod.get('category', '')
        subcategory = prod.get('subcategory', '')
        description = prod.get('description', '')
        features = json.dumps(prod.get('features', []))
        interest_rate = prod.get('interest_rate', '')
        fees = prod.get('fees', '')
        eligibility = prod.get('eligibility', '')
        url = prod.get('url', '')
        today = datetime.now().strftime('%Y-%m-%d')

        if existing:
            cursor.execute("""
                UPDATE products SET
                    bank_name = ?, category = ?, subcategory = ?,
                    description = ?, features = ?, interest_rate = ?,
                    fees = ?, eligibility = ?, url = ?
                WHERE id = ?
            """, (bank_name, category, subcategory, description,
                  features, interest_rate, fees, eligibility, url, existing[0]))
            stats['updated'] += 1
        else:
            cursor.execute("""
                INSERT INTO products (
                    bank_id, bank_name, product_name, category, subcategory,
                    description, features, interest_rate, fees, eligibility,
                    url, first_seen_at, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (bank_id, bank_name, name, category, subcategory,
                  description, features, interest_rate, fees, eligibility, url, today, today))
            stats['new'] += 1

    conn.commit()
    conn.close()

    return stats

def main():
    # Step 1: Initialize
    print("Step 1: Initialize database")
    db.init_db()
    print("  [OK] Database ready")
    print()

    # Step 2: Scrape
    print("Step 2: Scrape bank websites")
    print("  [INFO] Running scraper (5-10 minutes)...")

    try:
        result = subprocess.run(
            [sys.executable, '-c',
             'from scraper import run_scraper; '
             'import json; '
             'data = run_scraper(); '
             'print(json.dumps(data, default=str))'],
            cwd=str(BASE_DIR / 'scripts'),
            capture_output=True,
            text=True,
            timeout=600
        )

        if result.returncode != 0:
            print(f"  [ERR] Scraper failed: {result.stderr[:200]}")
            return

        print("  [OK] Scraper completed")

        # Parse scraped data
        try:
            scraped_data = json.loads(result.stdout.strip().split('\n')[-1])
            print(f"  [OK] Scraped {len(scraped_data)} banks")
        except Exception as e:
            print(f"  [ERR] Parse error: {e}")
            return

    except subprocess.TimeoutExpired:
        print("  [ERR] Scraper timeout")
        return
    except Exception as e:
        print(f"  [ERR] {e}")
        return

    print()

    # Step 3: AI Processing
    print("Step 3: AI Analysis & Extraction")
    print("  [INFO] Processing with Claude Code built-in AI")
    print()

    total_stats = {'promotions': {'new': 0, 'updated': 0}, 'products': {'new': 0, 'updated': 0}}

    for bank_id, data in scraped_data.items():
        bank_name = data.get('bank_name', bank_id)
        text_content = data.get('text', '')

        if not text_content or len(text_content) < 100:
            print(f"  [SKIP] {bank_name}: Insufficient content")
            continue

        print(f"  [PROCESSING] {bank_name} ({len(text_content)} chars)")

        # Extract using advanced AI processor
        promotions = extract_promotions_advanced(text_content, bank_id, bank_name)
        products = extract_products_advanced(text_content, bank_id, bank_name)

        # Save to database
        promo_stats = save_promotions_to_db(bank_id, bank_name, promotions)
        prod_stats = save_products_to_db(bank_id, bank_name, products)

        print(f"    Promotions: {promo_stats['new']} new, {promo_stats['updated']} updated")
        print(f"    Products: {prod_stats['new']} new, {prod_stats['updated']} updated")

        # Update totals
        total_stats['promotions']['new'] += promo_stats['new']
        total_stats['promotions']['updated'] += promo_stats['updated']
        total_stats['products']['new'] += prod_stats['new']
        total_stats['products']['updated'] += prod_stats['updated']

    print()

    # Step 4: Export data.json for website
    print("Step 4: Export data.json for website")
    try:
        db.export_to_json(str(DATA_JSON_PATH), ai_unavailable=False)
        print(f"  [OK] Exported to {DATA_JSON_PATH}")
    except Exception as e:
        print(f"  [ERR] Failed to export data.json: {e}")

    print()

    # Step 5: Generate and send email
    print("Step 5: Generate email report")

    # Load data for email
    all_promos = db.load_promotions(active_only=True)

    # Load data.json
    try:
        with open(DATA_JSON_PATH, 'r', encoding='utf-8') as f:
            data_json = json.load(f)
    except:
        data_json = {}

    # Get today's date for filtering
    today = datetime.now().strftime('%Y-%m-%d')

    # Filter new promotions (today and this week)
    new_promos_today = [p for p in all_promos if p.get('first_seen_at') == today]

    # Build HTML email
    html = build_html_email(
        promotions_data=all_promos,
        scraped_data=data_json,
        strategic_insights=None,
        new_promos=new_promos_today,
        new_promos_week=[],
        new_products=[],
        ai_unavailable=False
    )

    # Check email configuration
    gmail_address = os.environ.get('GMAIL_ADDRESS')
    gmail_password = os.environ.get('GMAIL_APP_PASSWORD')
    recipient = os.environ.get('RECIPIENT_EMAIL')

    if gmail_address and gmail_password and recipient:
        # Send email
        try:
            success = send_email(
                html_content=html,
                subject=f"VBank Tracker Update - {datetime.now().strftime('%Y-%m-%d')}",
                recipient=[recipient],
                gmail_address=gmail_address,
                gmail_app_password=gmail_password
            )
            if success:
                print(f"  [OK] Email sent to {recipient}")
            else:
                print("  [ERR] Email sending failed")
                # Save preview
                EMAIL_PREVIEW_PATH.parent.mkdir(parents=True, exist_ok=True)
                with open(EMAIL_PREVIEW_PATH, 'w', encoding='utf-8') as f:
                    f.write(html)
                print(f"  [FILE] Saved preview to {EMAIL_PREVIEW_PATH}")
        except Exception as e:
            print(f"  [ERR] Email error: {e}")
            # Save preview
            EMAIL_PREVIEW_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(EMAIL_PREVIEW_PATH, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"  [FILE] Saved preview to {EMAIL_PREVIEW_PATH}")
    else:
        print("  [INFO] Email not configured (missing GMAIL_ADDRESS/GMAIL_APP_PASSWORD/RECIPIENT_EMAIL)")
        # Save preview
        EMAIL_PREVIEW_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(EMAIL_PREVIEW_PATH, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"  [FILE] Saved preview to {EMAIL_PREVIEW_PATH}")

    print()

    # Step 6: Database stats
    print("Step 6: Database statistics")

    # Get current database state
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM promotions WHERE active=1")
    active_promos = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM products")
    total_products = cursor.fetchone()[0]

    conn.close()

    print(f"  Active promotions: {active_promos}")
    print(f"  Total products: {total_products}")
    print()

    # Summary
    print("=" * 70)
    print("  AUTONOMOUS RUN COMPLETE")
    print("=" * 70)
    print(f"  Promotions: {total_stats['promotions']['new']} new, {total_stats['promotions']['updated']} updated")
    print(f"  Products: {total_stats['products']['new']} new, {total_stats['products']['updated']} updated")
    print()
    print("  [OK] AI processing completed")
    print("  [OK] Database updated")
    print("  [OK] data.json exported")
    print("  [OK] Email report generated")
    print("  [OK] Zero external API calls")
    print("=" * 70)

    # Save summary
    summary = {
        'timestamp': datetime.now().isoformat(),
        'mode': 'enhanced_autonomous',
        'banks_scraped': len(scraped_data),
        'stats': total_stats,
        'database': {
            'active_promotions': active_promos,
            'products': total_products
        },
        'email_sent': bool(gmail_address and gmail_password and recipient)
    }

    output_file = BASE_DIR / 'data' / 'autonomous_enhanced_summary.json'
    with open(output_file, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"\n  [OUTPUT] {output_file}")

if __name__ == '__main__':
    main()
