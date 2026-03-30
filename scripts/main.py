# scripts/main.py

from dotenv import load_dotenv
import os

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

print("USER:", os.getenv("GMAIL_ADDRESS"))
print("PASS:", ("SET=" + os.getenv("GMAIL_APP_PASSWORD", "")[:4] + "...")
      if os.getenv("GMAIL_APP_PASSWORD") else "MISSING")
print("TO:  ", os.getenv("RECIPIENT_EMAIL"))

import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper   import run_scraper, BANK_CONFIGS
from ai_helper import init_ai, analyze_promotions, generate_strategic_insights
from database  import (
    init_db, save_promotions,
    mark_stale_as_inactive, mark_inactive_old,
    generate_daily_report,
    export_to_json,
)
from emailer   import build_html_email, send_email

DATA_JSON_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', 'docs', 'data.json'
)


def main():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f'\n{"═"*60}')
    print(f'  HK Virtual Bank Promotions Tracker  |  {today}')
    print(f'{"═"*60}\n')

    # ── Env check ────────────────────────────────────────────────
    _gmail_addr = os.environ.get('GMAIL_ADDRESS', '')
    _gmail_pass = os.environ.get('GMAIL_APP_PASSWORD', '')
    _recipient  = (
        os.environ.get('RECIPIENT_EMAIL') or
        os.environ.get('EMAIL_RECIPIENT') or
        os.environ.get('EMAIL_TO') or ''
    )

    print('  Env check:')
    print(f'    GMAIL_ADDRESS     : {"✅ set" if _gmail_addr else "❌ MISSING"}')
    print(f'    GMAIL_APP_PASSWORD: {"✅ set" if _gmail_pass else "❌ MISSING"}')
    print(f'    RECIPIENT_EMAIL   : {"✅ " + _recipient if _recipient else "❌ MISSING"}')

    # ── Step 1: Database ──────────────────────────────────────────
    print('\nStep 1 ── Init database')
    init_db()

    # ── Step 2: AI ────────────────────────────────────────────────
    print('\nStep 2 ── Init AI')
    ai_ok = init_ai()

    # ── Step 3: Scrape all 8 banks ────────────────────────────────
    print('\nStep 3 ── Scrape all 8 banks')
    scraped = run_scraper()

    if not scraped:
        print('  ❌ No data scraped — abort')
        return

    # Banks that returned successful scrape results (used later for mark_stale)
    bank_ids_ok: list = [
        bank_id for bank_id, result in scraped.items()
        if result.get('success')
    ]

    # Build dict keyed by bank_name for email scrape-status table
    scraped_by_name: dict = {
        result.get('bank_name', bank_id): result
        for bank_id, result in scraped.items()
    }

    # ── Step 4: AI extraction + save to DB ───────────────────────
    print('\nStep 4 ── AI extraction')
    total_extracted = 0

    for bank_id, result in scraped.items():
        bank_name   = result.get('bank_name', bank_id)
        default_url = BANK_CONFIGS.get(bank_id, {}).get('link', '')
        chars       = len(result.get('text', ''))
        mark        = '✅' if result.get('success') else '❌'
        print(f'\n  [{bank_id.upper()}] {bank_name}  {mark}  ({chars:,} chars scraped)')

        if not ai_ok:
            print('    ⚠️  AI unavailable — skip')
            continue
        if not result.get('success'):
            print(f'    ⚠️  Scrape failed — skip AI for {bank_name}')
            continue

        try:
            promos = analyze_promotions(
                bank_id     = bank_id,
                bank_name   = bank_name,
                text        = result.get('text', ''),
                screenshot  = result.get('screenshot'),
                default_url = default_url,
            )
        except Exception as e:
            print(f'    ❌ AI error for {bank_name}: {e}')
            continue

        if promos:
            ins, upd, skip = save_promotions(promos)
            total_extracted += len(promos)
            print(f'    ✅ {len(promos)} extracted — {ins} inserted, {upd} updated, {skip} skipped')
        else:
            print(f'    ⚠️  0 promotions extracted for {bank_name}')

    print(f'\n  Total extracted this run: {total_extracted}')

    # ── Step 5: Mark stale + old inactive ────────────────────────
    print('\nStep 5 ── Mark stale / old promos inactive')
    # Only mark stale for banks we successfully scraped today
    mark_stale_as_inactive(bank_ids_ok)
    # Safety net: anything not seen in 90 days regardless of bank
    mark_inactive_old(days_threshold=90)

    # ── Step 6: Export data.json for website ─────────────────────
    print('\nStep 6 ── Export data.json for website')
    export_to_json(DATA_JSON_PATH)

    # ── Step 7: Generate daily report ────────────────────────────
    print('\nStep 7 ── Generate daily report')
    report         = generate_daily_report()
    new_promos     = report['new']
    active_promos  = report['active']
    expired_promos = report['expired']
    summary        = report['summary']

    print(f'  🆕 New:     {summary["new_count"]}')
    print(f'  ✅ Active:  {len(active_promos)}')
    print(f'  ❌ Expired: {summary["expired_count"]}')
    for bank_id, count in summary['by_bank'].items():
        print(f'    {bank_id.upper()}: {count} active')

    # Combine new + active for email promo list + insights
    all_promos = new_promos + active_promos

    # ── Step 8: Strategic insights ────────────────────────────────
    print('\nStep 8 ── Generate AI strategic insights')
    promos_by_name: dict = {}
    for p in all_promos:
        bname = p.get('bName') or p.get('bank_name') or p.get('bank') or 'Unknown'
        promos_by_name.setdefault(bname, []).append(p)

    strategic_insights = None
    if ai_ok and promos_by_name:
        try:
            strategic_insights = generate_strategic_insights(promos_by_name)
        except Exception as e:
            print(f'  ⚠️  Insights error: {e}')
    if not strategic_insights:
        print('  ⚠️  Insights unavailable — continuing without it')

    # ── Step 9: Build & send email ────────────────────────────────
    print('\nStep 9 ── Build & send email')
    html = build_html_email(
        promotions_data    = all_promos,
        scraped_data       = scraped_by_name,
        strategic_insights = strategic_insights,
    )
    print('  ✅ HTML email built')

    output_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..', 'output', 'email_preview.html'
    )
    _save_html_fallback(html, output_path)

    smtp_ready = all([_gmail_addr, _gmail_pass, _recipient])
    if not smtp_ready:
        missing = [
            name for name, val in [
                ('GMAIL_ADDRESS',      _gmail_addr),
                ('GMAIL_APP_PASSWORD', _gmail_pass),
                ('RECIPIENT_EMAIL',    _recipient),
            ] if not val
        ]
        print(f'  ❌ Missing {" / ".join(missing)} — email skipped')
        print(f'  📄 HTML preview saved → {output_path}')
    else:
        try:
            success = send_email(html_content=html, recipient=_recipient)
            if success:
                print(f'  ✅ Email sent → {_recipient}')
            else:
                print('  ❌ send_email() returned False')
                print(f'  📄 HTML preview → {output_path}')
        except Exception as e:
            print(f'  ❌ Email failed: {e}')
            print(f'  📄 HTML preview → {output_path}')

    print(f'\n{"═"*60}')
    print(
        f'  Done  |  '
        f'🆕 {summary["new_count"]} new  |  '
        f'✅ {len(active_promos)} active  |  '
        f'❌ {summary["expired_count"]} expired'
    )
    print(f'{"═"*60}\n')


def _save_html_fallback(html: str, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  📄 HTML saved → {path}')


if __name__ == '__main__':
    main()