# scripts/main.py
# main.py 在 scripts/ 資料夾內，直接 import 同層模組

import os
import sys
from datetime import datetime

# ── 確保 scripts/ 在 Python 路徑內 ──────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── 同層直接 import，不加 scripts. 前綴 ──────────────────────────
from scraper   import run_scraper, BANK_CONFIGS
from ai_helper import init_ai, analyze_promotions
from database  import init_db, save_promotions, load_promotions, mark_inactive_old
from emailer   import build_html_email, send_email


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

    # ── Step 6: 從 DB 讀取並生成 email ───────────────────────────
    print('\nStep 6 ── Load DB + build email')
    all_promos = load_promotions(active_only=True)
    print(f'  DB total active: {len(all_promos)}')

    html = build_html_email(
        promotions_data = all_promos,
        scraped_data    = scraped,
    )

    # ── Step 7: 發送 email ───────────────────────────────────────
    print('\nStep 7 ── Send email')

    # ✅ 兼容兩個環境變數名稱
    recipient = (
        os.environ.get('RECIPIENT_EMAIL') or
        os.environ.get('EMAIL_RECIPIENT') or
        ''
    )

    output_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..', 'output', 'email_preview.html'
    )

    if recipient:
        try:
            send_email(html_content=html, recipient=recipient)
            print(f'  ✅ Email sent to {recipient}')
        except Exception as e:
            print(f'  ❌ Email failed: {e}')
            _save_html_fallback(html, output_path)
    else:
        print('  ⚠️  RECIPIENT_EMAIL not set → saving HTML preview only')
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