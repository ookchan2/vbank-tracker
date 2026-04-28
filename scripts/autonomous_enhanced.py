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
from ai_processor import process_bank_content

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
                    first_seen_at, last_seen, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (bank_id, bank_name, title, highlight, description,
                  start_date, end_date, period, quota, cost,
                  promo_type, url, tc_link, is_bau, today, today))
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
                    url, first_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (bank_id, bank_name, name, category, subcategory,
                  description, features, interest_rate, fees, eligibility, url, today))
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
             'import asyncio; from scraper import run_scraper; '
             'data = asyncio.run(run_scraper()); '
             'import json; print(json.dumps(data, default=str))'],
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

        # Process with AI
        processed = process_bank_content(bank_id, bank_name, text_content)

        # Save to database
        promo_stats = save_promotions_to_db(bank_id, bank_name, processed['promotions'])
        prod_stats = save_products_to_db(bank_id, bank_name, processed['products'])

        # Update totals
        total_stats['promotions']['new'] += promo_stats['new']
        total_stats['promotions']['updated'] += promo_stats['updated']
        total_stats['products']['new'] += prod_stats['new']
        total_stats['products']['updated'] += prod_stats['updated']

    print()

    # Step 4: Generate outputs
    print("Step 4: Generate outputs")

    # Get current database state
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM promotions WHERE active=1")
    active_promos = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM products")
    total_products = cursor.fetchone()[0]

    conn.close()

    print(f"  Database: {active_promos} active promotions, {total_products} products")
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
        }
    }

    output_file = BASE_DIR / 'data' / 'autonomous_enhanced_summary.json'
    with open(output_file, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"\n  [OUTPUT] {output_file}")

if __name__ == '__main__':
    main()
