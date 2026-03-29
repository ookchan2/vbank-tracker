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
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper   import run_scraper, BANK_CONFIGS
from ai_helper import init_ai, analyze_promotions, generate_strategic_insights
from database  import init_db, save_promotions, load_promotions, mark_inactive_old, export_to_json
from emailer   import build_html_email, send_email

# Path for website data (committed to GitHub Pages)
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
    if _recipient:
        os.environ['EMAIL_TO'] = _recipient

    print('  Env check:')
    print(f'    GMAIL_ADDRESS     : {"✅ set" if _gmail_addr else "❌ MISSING"}')
    print(f'    GMAIL_APP_PASSWORD: {"✅ set" if _gmail_pass else "❌ MISSING"}')
    print(f'    RECIPIENT_EMAIL   : {"✅ " + _recipient if _recipient else "❌ MISSING"}')

    # ── Step 1: Database ─────────────────────────────────────────
    print('\nStep 1 ── Init database')
    init_db()

    # ── Step 2: AI ───────────────────────────────────────────────
    print('\nStep 2 ── Init AI')
    ai_ok = init_ai()

    # ── Step 3: Scrape all 8 banks ───────────────────────────────
    print('\nStep 3 ── Scrape all 8 banks')
    scraped = run_scraper()

    if not scraped:
        print('  ❌ No data scraped — abort')
        return

    # Build scraped_data dict keyed by bank_name for email
    scraped_by_name: dict = {}
    for bank_id, result in scraped.items():
        bname = result.get('bank_name', bank_id)
        scraped_by_name[bname] = result

    # ── Step 4: AI extraction ────────────────────────────────────
    print('\nStep 4 ── AI extraction')
    total_new = 0

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
            save_promotions(promos)
            total_new += len(promos)
            print(f'    ✅ {len(promos)} promotions saved')
        else:
            print(f'    ⚠️  0 promotions extracted for {bank_name}')

    print(f'\n  Total new/updated this run: {total_new}')

    # ── Step 5: Mark old promos inactive ─────────────────────────
    print('\nStep 5 ── Mark old promos inactive (>90 days unseen)')
    mark_inactive_old(days_threshold=90)

    # ── Step 6: Export to data.json for website ──────────────────
    print('\nStep 6 ── Export data.json for website')
    export_to_json(DATA_JSON_PATH)

    # ── Step 7: Load DB + insights + build email ─────────────────
    print('\nStep 7 ── Load DB + strategic insights + build email')
    all_promos = load_promotions(active_only=True)
    print(f'  DB total active: {len(all_promos)}')

    promos_by_id: defaultdict = defaultdict(list)
    for p in all_promos:
        bid = p.get('bank_id') or p.get('bank') or 'unknown'
        promos_by_id[bid].append(p)
    for bid, promos in promos_by_id.items():
        print(f'  {bid.upper()}: {len(promos)} active promos')

    promos_by_name: dict = {}
    for p in all_promos:
        bname = p.get('bName') or p.get('bank_name') or p.get('bank') or 'Unknown'
        promos_by_name.setdefault(bname, []).append(p)

    print('\n  🧠 Generating AI strategic insights...')
    strategic_insights = None
    if ai_ok:
        try:
            strategic_insights = generate_strategic_insights(promos_by_name)
        except Exception as e:
            print(f'  ⚠️  Insights error: {e}')
    if not strategic_insights:
        print('  ⚠️  Insights unavailable — continuing without it')

    html = build_html_email(
        promotions_data    = all_promos,
        scraped_data       = scraped_by_name,
        strategic_insights = strategic_insights,
    )
    print('  ✅ HTML email built')

    # ── Step 8: Send email ───────────────────────────────────────
    print('\nStep 8 ── Send email')
    output_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..', 'output', 'email_preview.html'
    )
    _save_html_fallback(html, output_path)

    smtp_ready = all([_gmail_addr, _gmail_pass, _recipient])
    if not smtp_ready:
        missing = []
        if not _gmail_addr: missing.append('GMAIL_ADDRESS')
        if not _gmail_pass: missing.append('GMAIL_APP_PASSWORD')
        if not _recipient:  missing.append('RECIPIENT_EMAIL')
        print(f'  ❌ Missing {" / ".join(missing)} — email skipped')
        print(f'  📄 HTML preview saved → {output_path}')
    else:
        try:
            success = send_email(html_content=html)
            if success:
                print(f'  ✅ Email sent → {_recipient}')
            else:
                print('  ❌ send_email() returned False')
                print(f'  📄 HTML preview → {output_path}')
        except Exception as e:
            print(f'  ❌ Email failed: {e}')
            print(f'  📄 HTML preview → {output_path}')

    print(f'\n{"═"*60}')
    print(f'  Done  |  {total_new} new/updated  |  {len(all_promos)} active in DB')
    print(f'{"═"*60}\n')


def _save_html_fallback(html: str, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  📄 HTML saved → {path}')


if __name__ == '__main__':
    main()