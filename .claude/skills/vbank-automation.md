---
name: vbank-automation
description: Automated HK Virtual Bank promotions tracker - scrapes, extracts, updates database, sends emails with full details
category: automation
requires: playwright-cli, python3
---

# HK Virtual Bank Promotions Tracker - Full Automation Skill

## Overview

This skill automates the entire HK Virtual Bank promotions tracking system:
1. Scrapes 8 virtual bank websites using Playwright CLI
2. Extracts promotions and products using AI (Claude via Poe API)
3. Updates SQLite database with new/updated records
4. Exports data.json for website display
5. Sends detailed HTML email with new promotions/products
6. Generates strategic insights comparing banks

## Prerequisites

```bash
# Install dependencies (one-time setup)
pip install -r requirements.txt
playwright install chromium
playwright install-deps chromium

# Set environment variables (in .env file)
POE_API_KEY=your-key-here
GMAIL_ADDRESS=your-email@gmail.com
GMAIL_APP_PASSWORD=your-app-password
RECIPIENT_EMAIL=recipient@example.com
```

## Usage

Trigger this skill automatically or manually:

```bash
cd scripts && python main.py
```

For no-email mode (dry run):
```bash
cd scripts && python main.py --no-email
```

To skip scraping and reprocess DB:
```bash
cd scripts && python main.py --skip-scrape
```

## What This Skill Does

### Step 1: Database Initialization
- Creates/opens `data/promotions.db` SQLite database
- Starts new scrape run record
- Migrates legacy bank names if needed

### Step 2: AI System Setup
- Initializes Poe API connection (Claude 3.7 Sonnet)
- Falls back gracefully if AI unavailable

### Step 3: Web Scraping (8 Banks)
Scrapes all virtual banks:
- **ZA Bank**: Promotions, funds, stocks, loans
- **Mox Bank**: Credit cards, referrals, travel deals
- **livi Bank**: Savings accounts, deposits
- **WeLab Bank**: Funds, insurance, loans
- **EleBank** (formerly Airstar): Stock trading, deposits
- **PADB** (formerly PAObank): Investments, money market
- **Fusion Bank**: FX transfers, savings
- **Ant Bank**: EM Plus offers, funds

Each bank scraped with:
- Primary URL fetching (Playwright)
- Retry logic for failed requests
- Content scrubbing (removes non-bank content)
- Screenshot fallback for thin content
- Character limit trimming (50k chars)

### Step 4: AI Extraction & Deduplication

#### 4a: Promotion Extraction
Extracts time-limited promotions with:
- Name, description, highlight (in English)
- Category tags (迎新/消費/投資/旅遊/etc.)
- Start/end dates, validity period
- Quota/eligibility requirements
- Cost/minimum spend
- T&C links

Within-batch deduplication removes duplicates.

#### 4b: Product Extraction
Extracts core banking products:
- **Deposit Products**: Savings, time deposits, multi-currency
- **Card Products**: Debit cards, credit cards
- **Investment Products**: Stock trading, funds, crypto
- **Loan Products**: Personal loans, mortgages

With enhanced details:
- Interest rates (e.g., "Up to 3.88% p.a.")
- Fee structures (e.g., "HKD 15/order")
- Eligibility (e.g., "HK residents 18+")
- Minimum requirements (e.g., "Min deposit HKD 10,000")
- 3-7 specific features per product

#### 4c: Database Matching
- Matches against existing DB records
- Prevents duplicate insertions
- Updates changed promotions

### Step 5: Stale Data Cleanup
- Marks inactive promotions (not seen today)
- Removes old records (>90 days)
- Repairs re-inserted promotions
- Migrates legacy bank names

### Step 6: Website Data Export
Exports `docs/data.json` with:
- All active promotions (non-BAU + BAU)
- All active products
- Strategic insights (bank comparisons)
- Timestamp metadata

### Step 7: Daily Report Generation
Generates summary:
- New promotions today
- Active promotions by bank
- Expired promotions

### Step 8: Strategic Insights
AI analyzes all promotions to identify:
- Best-in-class for each category (HK stocks, US stocks, crypto, funds, etc.)
- Bank strengths and weaknesses
- Competitive advantages vs ZA Bank
- Expiring alerts
- BAU (permanent) vs time-limited features

### Step 9: Email Generation & Sending

Builds comprehensive HTML email with sections for:

1. **New Promotions Today** (if any)
   - Bank name, title, category tags
   - Detailed description (WHAT/WHO/HOW MUCH/WHEN)
   - Quota, cost, validity period
   - Link to official source

2. **New Promotions This Week** (past 6 days)
   - Same detailed format as above

3. **All Active Promotions** (non-BAU)
   - Displayed in card format
   - Shows highlight/description
   - Bank color coding

4. **New Products Today** (if any)
   - Product name, bank, category badge
   - Description with rates/fees
   - Eligibility criteria
   - Link to view details

5. **Strategic Insights**
   - Best-for category winners
   - Bank analysis table
   - Pros/cons vs competitors

Email sent via Gmail SMTP to configured recipients.

## Output Files

- `data/promotions.db`: SQLite database with all records
- `docs/data.json`: Website data file (promotions + products + insights)
- `output/email_preview.html`: Local copy of sent email
- Console logs: Detailed execution output

## Data Consistency

**Database ↔ Website ↔ Email synchronization:**
- All sources use `data.json` as canonical count source
- Non-BAU promotions filtered consistently across email/website
- Products exported from same database query
- Strategic insights patched into `data.json` after generation

**Stats shown in final summary:**
- `[OK] X active (DB)`: Total active promotions from database
- `[FILE] Y active (data.json)`: Count from exported file
- These should match - if not, check export filters

## Troubleshooting

### Email Not Sending
- Check `.env` file has correct credentials
- Verify Gmail App Password is valid
- Check firewall doesn't block SMTP (port 587)
- Run with `--no-email` to test without SMTP

### Scraping Failures
- Ant Bank often fails (JavaScript blocking) - known limitation
- Check network connectivity
- Increase timeout in scraper settings
- Use `--skip-scrape` to reprocess existing data

### AI Extraction Issues
- Ensure `POE_API_KEY` is set and valid
- Check text length (>500 chars required)
- Review AI response format (must be valid JSON)
- Fallback to retry logic on parse failures

### Data Inconsistency
- Run `python main.py --skip-scrape` to re-export
- Check database queries match expected results
- Verify `export_to_json()` includes all active records
- Clear Python cache: `find . -name "*.pyc" -delete`

## Enhancement Opportunities

### Future Improvements
1. **Playwright CLI Integration**: Direct browser automation instead of requests
2. **Multi-language Support**: Extract Chinese descriptions alongside English
3. **Real-time Monitoring**: Alert when new promotions detected
4. **Historical Analytics**: Track promotion trends over time
5. **Price Comparison**: Auto-compare interest rates/fees across banks
6. **Mobile App**: React Native app displaying promotions

## Key Metrics Tracked

Per run:
- ~60-80 promotions extracted
- ~20-30 products tracked
- ~15-25% deduplication rate
- ~70-80% DB match rate (existing records)
- 0-5 new promotions per day
- 5-10 new promotions per week

Banks monitored: 8 (ZA, Mox, livi, WeLab, EleBank, PADB, Fusion, Ant)

## Automation Schedule

**GitHub Actions**: Runs daily at 9:00 AM HKT (UTC+8)
- Cron: `0 1 * * *` (01:00 UTC = 09:00 HKT)
- Manual trigger: `workflow_dispatch`
- Auto-commit: Pushes `data.json` changes to repo

## Architecture

```
scripts/
├── scraper.py       → Playwright web scraping
├── ai_helper.py     → AI extraction & analysis
├── database.py      → SQLite CRUD operations
├── emailer.py       → HTML email builder/send
└── main.py          → Pipeline orchestration

data/
└── promotions.db    → SQLite database

docs/
└── data.json        → Website data export

output/
└── email_preview.html → Email backup file
```

## Quick Start

```bash
# One-time setup
git clone https://github.com/ookchan2/vbank-tracker.git
cd vbank-tracker
pip install -r requirements.txt
playwright install chromium
cp .env.example .env
# Edit .env with your credentials

# Run manually
cd scripts && python main.py

# Or wait for daily GitHub Actions run
```

---

**Note**: This skill is designed for autonomous operation. When triggered, it executes the full pipeline end-to-end with minimal human intervention required.
