# scripts/main.py

from dotenv import load_dotenv
import os
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

import sys
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper    import run_scraper, BANK_CONFIGS
from ai_helper  import init_ai, analyze_promotions, generate_strategic_insights  # ✅ FIX 2
from database   import init_db, save_promotions, load_promotions, mark_inactive_old
from emailer    import build_html_email, send_email


def main():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f'\n{"═"*60}')
    print(f'  HK Virtual Bank Promotions Tracker  |  {today}')
    print(f'{"═"*60}\n')

    # ── Step 1: 初始化資料庫 ─────────────────────────────────────
    print('Step 1 ── Init database')
    init_db()

    # ── Step 2: 初始化 AI ────────────────────────────────────────
    print('\nStep 2 ── Init AI')
    ai_ok = init_ai()

    # ── Step 3: 爬取全部 8 家銀行 ────────────────────────────────
    print('\nStep 3 ── Scrape all 8 banks')
    scraped = run_scraper()

    if not scraped:
        print('  ❌ No data scraped — abort')
        return

    # ── Step 4: AI 提取促銷 ──────────────────────────────────────
    print('\nStep 4 ── AI extraction')
    total_new = 0

    for bank_id, result in scraped.items():
        bank_name   = result.get('bank_name', bank_id)
        default_url = BANK_CONFIGS.get(bank_id, {}).get('url', '')
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

    # ── Step 5: 清理舊記錄 ───────────────────────────────────────
    print('\nStep 5 ── Mark old promos inactive (>90 days unseen)')
    mark_inactive_old(days_threshold=90)

    # ── Step 6: 從 DB 讀取、生成 Insights、建立 Email ────────────
    print('\nStep 6 ── Load DB + strategic insights + build email')
    all_promos = load_promotions(active_only=True)
    print(f'  DB total active: {len(all_promos)}')

    # ── 6a: 按 bank_id 統計（用於 log）───────────────────────────
    promos_by_id: defaultdict = defaultdict(list)
    for p in all_promos:
        promos_by_id[p.get('bank', 'unknown')].append(p)

    for bank_id, promos in promos_by_id.items():
        print(f'  {bank_id.upper()}: {len(promos)} active promos')

    # ── 6b: 按 bank display name（用於 AI insights）──────────────
    # ✅ FIX 6: field is 'bName' from _stamp(), not 'bank_name'
    promos_by_name: dict = {}
    for p in all_promos:
        bname = p.get('bName') or p.get('bank') or 'Unknown'   # ✅ FIX 6
        promos_by_name.setdefault(bname, []).append(p)

    # ✅ FIX 1: Strategic Insights block moved INSIDE main()
    print('\n  🧠 Generating AI strategic insights...')
    strategic_insights = generate_strategic_insights(promos_by_name)
    if not strategic_insights:
        print('  ⚠️  Insights unavailable — email will send without that section')

    # ── 6c: Build email HTML ──────────────────────────────────────
    # ✅ FIX 1 + FIX 3: html built here, available for Step 7
    html = build_html_email(
        promotions_data   = all_promos,
        scraped_data      = scraped,
        strategic_insights= strategic_insights,
    )

    # ── Step 7: 發送 email ───────────────────────────────────────
    print('\nStep 7 ── Send email')

    # ✅ FIX 5: send_email reads EMAIL_TO from env — no recipient param
    # Set override here if RECIPIENT_EMAIL env var is provided
    recipient = (
        os.environ.get('RECIPIENT_EMAIL') or
        os.environ.get('EMAIL_RECIPIENT') or
        ''
    )
    if recipient:
        # Temporarily override EMAIL_TO so emailer picks it up
        os.environ['EMAIL_TO'] = recipient

    output_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..', 'output', 'email_preview.html'
    )

    if recipient or os.environ.get('EMAIL_TO'):
        try:
            success = send_email(html_content=html)   # ✅ FIX 4+5: called ONCE, correct sig
            if success:
                print(f'  ✅ Email sent → {os.environ.get("EMAIL_TO")}')
            else:
                print('  ❌ Email send returned False — saving HTML fallback')
                _save_html_fallback(html, output_path)
        except Exception as e:
            print(f'  ❌ Email failed: {e}')
            _save_html_fallback(html, output_path)
    else:
        print('  ⚠️  EMAIL_TO / RECIPIENT_EMAIL not set → saving HTML preview only')
        _save_html_fallback(html, output_path)

    # ── 完成 ─────────────────────────────────────────────────────
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