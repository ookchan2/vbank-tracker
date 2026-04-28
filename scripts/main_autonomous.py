#!/usr/bin/env python3
"""
Autonomous VBank Tracker - With Claude Code Integration
This version runs the tracker and intercepts AI requests for Claude Code to process
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime

# Set autonomous mode
os.environ['ANTHROPIC_API_KEY'] = ''
os.environ['AUTONOMOUS_MODE'] = '1'

sys.path.insert(0, str(Path(__file__).parent))

# Import after setting autonomous mode
from scraper import run_scraper, BANK_CONFIGS
import database as db
from claude_bridge import request_ai_analysis

print("=" * 70)
print("  VBank Tracker - Autonomous Mode with Claude Code")
print("=" * 70)
print("  Mode: Fully Autonomous (No External API)")
print("  AI: Claude Code Built-in")
print("  Scraper: Playwright")
print("  Database: SQLite")
print("=" * 70)
print()

def main():
    # Step 1: Initialize database
    print("Step 1: Initialize database")
    db.init_db()
    run_id = db.start_new_run()
    print(f"  [OK] Database ready (run #{run_id})")
    print()

    # Step 2: Scrape banks
    print("Step 2: Scrape bank websites")
    print("  [INFO] This will take several minutes...")
    print()

    # Run async scraper
    import asyncio
    try:
        scraped_data = asyncio.run(run_scraper())
        print(f"  [OK] Scraped {len(scraped_data)} banks")
    except Exception as e:
        print(f"  [ERR] Scraping failed: {e}")
        return
    print()

    # Step 3: AI Analysis (Claude Code processes each bank)
    print("Step 3: AI Analysis")
    print("  [INFO] Using Claude Code's built-in AI")
    print()

    all_promotions = []
    all_products = []

    for bank_id, data in scraped_data.items():
        bank_name = data.get('bank_name', bank_id)
        text_content = data.get('text', '')

        if not text_content:
            print(f"  [SKIP] {bank_name}: No content")
            continue

        print(f"  [BANK] {bank_name}: {len(text_content)} chars")

        # Request AI analysis through bridge
        # In real autonomous mode, Claude Code would:
        # 1. Read this request
        # 2. Analyze the text
        # 3. Write back structured JSON

        result = request_ai_analysis(
            task_type='extract_promotions',
            content=text_content,
            metadata={
                'bank_id': bank_id,
                'bank_name': bank_name
            }
        )

        print(f"    [RESULT] {result['status']}")

    print()
    print("=" * 70)
    print("  [STATUS] Autonomous framework operational")
    print("  [NEXT] Claude Code needs to process the AI requests")
    print("=" * 70)

if __name__ == '__main__':
    main()
