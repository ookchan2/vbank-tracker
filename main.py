# main.py  ── 修復 scope bug，統一使用函數 import

import os
from datetime import datetime

from scripts.scraper   import run_scraper, BANK_CONFIGS
from scripts.ai_helper import init_ai, analyze_promotions   # 直接 import 函數，無 scope 問題
from scripts.db        import init_db, save_promotions, load_promotions, mark_inactive_old
from scripts.emailer   import build_html_email, send_email


def main():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f'\n{"═"*60}')
    print(f'  HK Virtual Bank Promotions Tracker  |  {today}')
    print(f'{"═"*60}\n')

    # 1 ── 初始化資料庫
    print('Step 1 ── Init database')
    init_db()

    # 2 ── 初始化 AI（先 init，之後直接用函數）
    print('\nStep 2 ── Init AI')
    ai_ok = init_ai()

    # 3 ── 爬取全部 8 家銀行
    print('\nStep 3 ── Scrape all 8 banks')
    scraped = run_scraper()

    # 4 ── AI 提取促銷（analyze_promotions 永遠可調用）
    print('\nStep 4 ── AI extraction')
    total_new = 0

    for bank_id, result in scraped.items():
        bank_name   = result['bank_name']
        default_url = BANK_CONFIGS[bank_id]['link']
        chars       = len(result.get('text', ''))
        mark        = '✅' if result['success'] else '❌'
        print(f'\n  [{bank_id.upper()}] {bank_name}  {mark}  ({chars:,} chars scraped)')

        if not ai_ok:
            print('    ⚠️  AI unavailable — skip')
            continue

        # analyze_promotions 已定義在模組頂層，永遠可調用
        promos = analyze_promotions(
            bank_id     = bank_id,
            bank_name   = bank_name,
            text        = result.get('text', ''),
            screenshot  = result.get('screenshot'),
            default_url = default_url,
        )

        if promos:
            save_promotions(promos)
            total_new += len(promos)
        else:
            print(f'    ⚠️  0 promotions extracted for {bank_name}')

    print(f'\n  Total new/updated this run: {total_new}')

    # 5 ── 清理舊記錄
    print('\nStep 5 ── Mark old promos inactive (>90 days unseen)')
    mark_inactive_old(days_threshold=90)

    # 6 ── 從 DB 讀取全部記錄並生成 email
    print('\nStep 6 ── Load DB + build email')
    all_promos = load_promotions(active_only=True)
    print(f'  DB total active: {len(all_promos)}')

    html = build_html_email(promotions_data=all_promos)

    # 7 ── 發送 email
    print('\nStep 7 ── Send email')
    recipient = os.environ.get('EMAIL_RECIPIENT', '')

    if recipient:
        try:
            send_email(html)
        except Exception as e:
            print(f'  ❌ Email failed: {e}')
            # 儲存備份
            os.makedirs('output', exist_ok=True)
            with open('output/email_preview.html', 'w', encoding='utf-8') as f:
                f.write(html)
            print('  📄 Saved fallback to output/email_preview.html')
    else:
        os.makedirs('output', exist_ok=True)
        with open('output/email_preview.html', 'w', encoding='utf-8') as f:
            f.write(html)
        print('  EMAIL_RECIPIENT not set → saved to output/email_preview.html')

    print(f'\n{"═"*60}')
    print(f'  Done  |  {total_new} promotions processed  |  {len(all_promos)} in DB')
    print(f'{"═"*60}\n')


if __name__ == '__main__':
    main()