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
from ai_helper import (
    init_ai,
    analyze_promotions,
    ai_dedup_titles,
    ai_match_against_existing,
    generate_strategic_insights,
)
from database  import (
    init_db,
    start_new_run,
    save_promotions,
    mark_stale_as_inactive,
    mark_inactive_old,
    generate_daily_report,
    export_to_json,
    get_active_promos_for_bank,
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
    current_run_id = start_new_run(banks=list(BANK_CONFIGS.keys()))

    # ── Step 2: AI ────────────────────────────────────────────────
    print('\nStep 2 ── Init AI')
    ai_ok = init_ai()

    # ── Step 3: Scrape all banks ──────────────────────────────────
    print('\nStep 3 ── Scrape all 8 banks')
    scraped = run_scraper()

    if not scraped:
        print('  ❌ No data scraped — abort')
        return

    bank_ids_ok: list = [
        bank_id for bank_id, result in scraped.items()
        if result.get('success')
    ]
    scraped_by_name: dict = {
        result.get('bank_name', bank_id): result
        for bank_id, result in scraped.items()
    }

    # ── Step 4: AI extraction + dedup + save ─────────────────────
    print('\nStep 4 ── AI extraction')
    total_extracted  = 0
    total_new        = 0
    total_updated    = 0
    total_deduped    = 0
    total_db_matched = 0

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

        # ── 4a: Extract promotions ────────────────────────────────
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

        if not promos:
            print(f'    ⚠️  0 promotions extracted for {bank_name}')
            continue

        # ── 4b: Within-batch dedup ────────────────────────────────
        try:
            titles  = [p.get('name') or p.get('title', '') for p in promos]
            dup_map = ai_dedup_titles(titles, bank_name)
            if dup_map:
                before  = len(promos)
                promos  = [p for i, p in enumerate(promos) if i not in dup_map]
                removed = before - len(promos)
                total_deduped += removed
                print(f'    🤖 Within-batch dedup: removed {removed} '
                      f'({before} → {len(promos)}) for {bank_name}')
        except Exception as e:
            print(f'    ⚠️  Within-batch dedup error for {bank_name}: {e}')

        if not promos:
            print(f'    ⚠️  0 promotions after within-batch dedup for {bank_name}')
            continue

        # ── 4c: Match against existing DB records ─────────────────
        try:
            existing_db = get_active_promos_for_bank(bank_id)
            if existing_db:
                match_map = ai_match_against_existing(promos, existing_db, bank_name)
                for idx, db_id in match_map.items():
                    if 0 <= idx < len(promos):
                        promos[idx]['_matched_id'] = db_id
                total_db_matched += len(match_map)
            else:
                print(f'    ℹ️  No existing DB records for {bank_name} — all will be new')
        except Exception as e:
            print(f'    ⚠️  DB-match error for {bank_name}: {e} — formula pass only')

        # ── 4d: Save to DB ────────────────────────────────────────
        total_extracted += len(promos)
        db_result = save_promotions(
            bank_id,
            bank_name,
            promos,
            current_run_id = current_run_id,
        )
        total_new     += db_result['new']
        total_updated += db_result['updated']
        print(f"    ✅ {db_result['new']} new, {db_result['updated']} updated, "
              f"{db_result['skipped']} skipped — {bank_name}")

    print(f"\n📊 Extracted: {total_extracted} | "
          f"New: {total_new} | Updated: {total_updated} | "
          f"Within-batch deduped: {total_deduped} | "
          f"DB-matched: {total_db_matched}")

    # ── Step 5: Mark stale + old inactive ────────────────────────
    print('\nStep 5 ── Mark stale / old promos inactive')
    mark_stale_as_inactive(bank_ids_ok)
    mark_inactive_old(days_threshold=90)

    # ── Step 6: Export data.json for website ─────────────────────
    print('\nStep 6 ── Export data.json for website')
    export_to_json(DATA_JSON_PATH)

    # ── Step 7: Generate daily report ────────────────────────────
    print('\nStep 7 ── Generate daily report')
    report         = generate_daily_report(current_run_id)
    new_promos     = report['new']
    active_promos  = report['active']
    expired_promos = report['expired']
    summary        = report['summary']

    print(f'  🆕 New:     {summary["new_count"]}')
    print(f'  ✅ Active:  {len(active_promos)}')
    print(f'  ❌ Expired: {summary["expired_count"]}')
    for bid, count in summary['by_bank'].items():
        print(f'    {bid.upper()}: {count} active')

    # ── BAU separation ────────────────────────────────────────────
    # generate_daily_report excludes BAU (is_bau=0 filter in DB).
    # The explicit filters below are a safety layer in case BAU slips through.
    #
    # ┌─ all_promos_full  ─── for strategic insights
    # │    BAU features (e.g. "$0 crypto fee") are CRITICAL competitive signals.
    # │    Use the full list so insights can see them.
    # │
    # └─ all_promos_email ─── for email display sections
    #      Email shows ONLY non-BAU promotions.
    #      Both "All Active" and "Newly Launched" sections exclude BAU.
    # ← CHANGED: was a single all_promos = new_promos + active_promos
    all_promos_full  = new_promos + active_promos
    all_promos_email = [p for p in all_promos_full if not p.get('is_bau', False)]
    new_promos_email = [p for p in new_promos      if not p.get('is_bau', False)]

    # ── Step 8: Strategic insights ────────────────────────────────
    print('\nStep 8 ── Generate AI strategic insights')
    promos_by_name: dict = {}
    for p in all_promos_full:   # ← uses FULL list (BAU included if present)
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
    # ← CHANGED: pass email-specific (non-BAU) lists to the emailer
    html = build_html_email(
        promotions_data    = all_promos_email,
        scraped_data       = scraped_by_name,
        strategic_insights = strategic_insights,
        new_promos         = new_promos_email,
    )
    print('  ✅ HTML email built')
    print(f'  [INFO] Non-BAU new promos this run : {len(new_promos_email)}')
    print(f'  [INFO] Non-BAU active promos total : {len(all_promos_email)}')

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
        f'🆕 {len(new_promos_email)} new (non-BAU)  |  '
        f'✅ {len(all_promos_email)} active (non-BAU)  |  '
        f'❌ {summary["expired_count"]} expired  |  '
        f'🤖 deduped:{total_deduped} db-matched:{total_db_matched}'
    )
    print(f'{"═"*60}\n')


def _save_html_fallback(html: str, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  📄 HTML saved → {path}')


if __name__ == '__main__':
    main()