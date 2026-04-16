# scripts/main.py

import json as _json
import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

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
    get_active_promotions,
    get_promotions_by_bank_name,
    get_new_promotions_last_n_days,
    get_db_stats,
)
from emailer   import build_html_email, send_email

DATA_JSON_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', 'docs', 'data.json',
)

# ── CLI flags ─────────────────────────────────────────────────────────────────
_NO_EMAIL    = '--no-email'    in sys.argv or '--dry-run' in sys.argv
_SKIP_SCRAPE = '--skip-scrape' in sys.argv


# ── Env helpers ───────────────────────────────────────────────────────────────

def _read_env() -> tuple[str, str, str]:
    addr = os.environ.get('GMAIL_ADDRESS',      '').strip()
    pwd  = os.environ.get('GMAIL_APP_PASSWORD', '').strip()
    to   = (
        os.environ.get('RECIPIENT_EMAIL') or
        os.environ.get('EMAIL_RECIPIENT') or
        os.environ.get('EMAIL_TO')        or ''
    ).strip()
    return addr, pwd, to


def _print_env_check(addr: str, pwd: str, to: str) -> None:
    print('  Env check:')
    print(f'    GMAIL_ADDRESS     : {"✅ set" if addr else "❌ MISSING"}')
    print(f'    GMAIL_APP_PASSWORD: {"✅ set (hidden)" if pwd else "❌ MISSING"}')
    print(f'    RECIPIENT_EMAIL   : {"✅ " + to if to else "❌ MISSING"}')
    if _NO_EMAIL:
        print('    📴 --no-email flag — SMTP step will be skipped')


def _save_html_fallback(html: str, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  📄 HTML saved → {path}')


# ── Patch data.json with an arbitrary dict of extra keys ─────────────────────

def _patch_data_json(path: str, extra: dict) -> None:
    """
    Read data.json, merge `extra` into the top-level dict, then write it back.
    Called once after strategic insights are generated so the website can read
    `data.strategic_insights` without a separate file.

    Failures are non-fatal — a warning is printed and the pipeline continues.
    """
    keys = ', '.join(extra.keys())
    try:
        with open(path, 'r', encoding='utf-8') as f:
            jdata = _json.load(f)
        jdata.update(extra)
        with open(path, 'w', encoding='utf-8') as f:
            _json.dump(jdata, f, ensure_ascii=False, indent=2)
        print(f'  ✅ data.json patched with key(s): {keys}')
    except Exception as exc:
        print(f'  ⚠️  data.json patch failed ({keys}): {exc}')


# ── Main pipeline ─────────────────────────────────────────────────────────────

def main() -> int:
    """
    Returns 0 on success, 1 on a hard failure that prevented the data pipeline
    from completing.  Soft failures (AI unavailable, email not sent) still
    return 0 — the scrape + DB write succeeded.
    """
    t_start = time.monotonic()
    today   = datetime.now().strftime('%Y-%m-%d')

    print(f'\n{"═"*60}')
    print(f'  HK Virtual Bank Promotions Tracker  |  {today}')
    if _NO_EMAIL:
        print('  MODE: --no-email  (pipeline runs; SMTP skipped)')
    if _SKIP_SCRAPE:
        print('  MODE: --skip-scrape  (re-processing DB data only)')
    print(f'{"═"*60}\n')

    addr, pwd, to = _read_env()
    _print_env_check(addr, pwd, to)

    # ── Step 1: Database ──────────────────────────────────────────
    print('\nStep 1 ── Init database')
    try:
        init_db()
        current_run_id = start_new_run(banks=list(BANK_CONFIGS.keys()))
    except Exception as exc:
        print(f'  ❌ Database init failed — cannot continue: {exc}')
        return 1

    # ── Step 2: AI ────────────────────────────────────────────────
    print('\nStep 2 ── Init AI')
    ai_ok = init_ai()

    # ── Step 3: Scrape all banks ──────────────────────────────────
    print(f'\nStep 3 ── Scrape all {len(BANK_CONFIGS)} banks')
    t3 = time.monotonic()

    if _SKIP_SCRAPE:
        print('  ⏭  --skip-scrape: using existing DB data only')
        scraped: dict = {
            bid: {
                'bank_name':      cfg['name'],
                'text':           '',
                'success':        True,
                'screenshot':     None,
                'sections_count': 0,
                'elapsed_s':      0.0,
                'errors':         [],
            }
            for bid, cfg in BANK_CONFIGS.items()
        }
    else:
        scraped = run_scraper()

    print(f'  ⏱  Scrape completed in {time.monotonic() - t3:.1f}s')

    if not scraped:
        print('  ❌ No data scraped — abort')
        return 1

    bank_ids_ok: list[str] = [bid for bid, r in scraped.items() if r.get('success')]
    scraped_by_name: dict  = {
        r.get('bank_name', bid): r
        for bid, r in scraped.items()
    }

    # ── Step 4: AI extraction + dedup + save ─────────────────────
    print('\nStep 4 ── AI extraction')
    t4 = time.monotonic()

    total_extracted  = 0
    total_new        = 0
    total_updated    = 0
    total_deduped    = 0
    total_db_matched = 0
    banks_ai_saved: list[str] = []   # tracks banks where save_promotions succeeded

    for bank_id, result in scraped.items():
        bank_name   = result.get('bank_name', bank_id)
        default_url = BANK_CONFIGS.get(bank_id, {}).get('link', '')
        chars       = len(result.get('text', ''))
        mark        = '✅' if result.get('success') else '❌'
        print(f'\n  [{bank_id.upper()}] {bank_name}  {mark}  ({chars:,} chars)')

        if not ai_ok:
            print('    ⚠️  AI unavailable — skip')
            continue
        if not result.get('success') and not _SKIP_SCRAPE:
            print(f'    ⚠️  Scrape failed — skip AI for {bank_name}')
            continue

        # 4a: Extract promotions ───────────────────────────────────
        try:
            promos = analyze_promotions(
                bank_id     = bank_id,
                bank_name   = bank_name,
                text        = result.get('text', ''),
                screenshot  = result.get('screenshot'),
                default_url = default_url,
            )
        except Exception as exc:
            print(f'    ❌ AI extraction error for {bank_name}: {exc}')
            continue

        if not promos:
            print(f'    ⚠️  0 promotions extracted for {bank_name}')
            continue

        # 4b: Within-batch dedup ───────────────────────────────────
        try:
            titles  = [p.get('name') or p.get('title', '') for p in promos]
            dup_map = ai_dedup_titles(titles, bank_name)
            if dup_map:
                before         = len(promos)
                promos         = [p for i, p in enumerate(promos) if i not in dup_map]
                removed        = before - len(promos)
                total_deduped += removed
                print(
                    f'    🤖 Within-batch dedup: {removed} removed '
                    f'({before} → {len(promos)}) for {bank_name}'
                )
        except Exception as exc:
            print(f'    ⚠️  Within-batch dedup error for {bank_name}: {exc}')

        if not promos:
            print(f'    ⚠️  0 promotions after within-batch dedup for {bank_name}')
            continue

        # 4c: Match against existing DB records ───────────────────
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
        except Exception as exc:
            print(f'    ⚠️  DB-match error for {bank_name}: {exc} — formula pass only')

        # 4d: Save to DB ───────────────────────────────────────────
        total_extracted += len(promos)
        try:
            db_result = save_promotions(
                bank_id, bank_name, promos,
                current_run_id=current_run_id,
            )
        except Exception as exc:
            print(f'    ❌ save_promotions error for {bank_name}: {exc}')
            continue

        # Only record this bank as successfully saved so Step 5 only
        # marks stale rows for banks where last_seen was actually refreshed.
        banks_ai_saved.append(bank_id)

        total_new     += db_result['new']
        total_updated += db_result['updated']
        print(
            f"    ✅ {db_result['new']} new, {db_result['updated']} updated, "
            f"{db_result['skipped']} skipped — {bank_name}"
        )

    print(f'  ⏱  AI extraction completed in {time.monotonic() - t4:.1f}s')
    print(
        f"\n📊 Extracted:{total_extracted}  New:{total_new}  Updated:{total_updated}  "
        f"Deduped:{total_deduped}  DB-matched:{total_db_matched}"
    )

    # ── Step 5: Mark stale / old inactive ────────────────────────
    print('\nStep 5 ── Mark stale / old promos inactive')

    if not ai_ok:
        # AI was unavailable — save_promotions() was never called for any
        # bank, so last_seen was never updated.  Running mark_stale_as_inactive
        # now would flip every row to active=0, wiping the entire dataset.
        # Skip both staleness passes to preserve the existing DB state.
        print(
            '  ⚠️  AI unavailable — skipping mark_stale_as_inactive and '
            'mark_inactive_old to preserve existing data'
        )
    elif not banks_ai_saved:
        # AI was available but every bank either failed extraction or
        # save_promotions raised — same risk: last_seen was never refreshed.
        print(
            '  ⚠️  No banks were successfully saved this run — '
            'skipping mark_stale_as_inactive to avoid false-expiry'
        )
    else:
        # Only mark stale for banks where save_promotions actually succeeded
        # (last_seen was refreshed today).  This prevents a partial-failure
        # run from expiring rows for banks whose save errored out.
        mark_stale_as_inactive(banks_ai_saved)
        mark_inactive_old(days_threshold=90)

    # ── Step 6: Export data.json ──────────────────────────────────
    print('\nStep 6 ── Export data.json for website')
    export_to_json(DATA_JSON_PATH)

    _run_ts = datetime.now().strftime('%Y-%m-%d %H:%M')
    try:
        with open(DATA_JSON_PATH, 'r', encoding='utf-8') as _f:
            _jdata = _json.load(_f)
        _jdata['updated']      = _run_ts
        _jdata['last_updated'] = _run_ts
        with open(DATA_JSON_PATH, 'w', encoding='utf-8') as _f:
            _json.dump(_jdata, _f, ensure_ascii=False, indent=2)
        print(f'  ✅ data.json timestamp patched → {_run_ts}')
    except Exception as exc:
        print(f'  ⚠️  data.json timestamp patch failed: {exc}')

    # ── Step 7: Daily report ──────────────────────────────────────
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

    # ── Step 8: Strategic insights + weekly new promos ────────────
    print('\nStep 8 ── Generate AI strategic insights')
    all_active_with_bau = get_active_promotions(include_bau=True)
    bau_count_insights  = sum(1 for p in all_active_with_bau if p.get('is_bau', False))

    print(
        f'  📊 Insights input: {len(all_active_with_bau)} promos '
        f'({bau_count_insights} BAU + '
        f'{len(all_active_with_bau) - bau_count_insights} time-limited)'
    )

    # Build bank-name → promos map from the same DB snapshot as data.json.
    promos_by_name: dict = {}
    for p in all_active_with_bau:
        bname = p.get('bank_name') or p.get('bName') or p.get('bank') or 'Unknown'
        promos_by_name.setdefault(bname, []).append(p)

    # Email lists: same DB snapshot, non-BAU only.
    all_promos_email = [p for p in all_active_with_bau if not p.get('is_bau', False)]
    new_promos_email = [p for p in new_promos          if not p.get('is_bau', False)]

    # Promotions first seen in the past 6 days, excluding today's new batch.
    # These populate a "new this week" section in the email so readers don't
    # miss promotions that were new on previous days this week.
    new_promos_week_raw   = get_new_promotions_last_n_days(
        days           = 6,
        include_bau    = False,
        exclude_run_id = current_run_id,
    )
    new_promos_week_email = [
        p for p in new_promos_week_raw if not p.get('is_bau', False)
    ]
    print(f'  [INFO] Non-BAU new (today):        {len(new_promos_email)}')
    print(f'  [INFO] Non-BAU new (past 6 days):  {len(new_promos_week_email)}')
    print(f'  [INFO] Non-BAU active (all):       {len(all_promos_email)}')
    print(f'  [INFO] BAU (insights input):       {bau_count_insights}')

    strategic_insights = None
    if ai_ok and promos_by_name:
        try:
            strategic_insights = generate_strategic_insights(
                promos_by_name,
                db_fetch_fn=get_promotions_by_bank_name,
            )
        except Exception as exc:
            print(f'  ⚠️  Insights error: {exc}')

    if strategic_insights:
        # Patch data.json so the website can read data.strategic_insights
        # directly from the same file it already fetches — no extra request.
        _patch_data_json(DATA_JSON_PATH, {'strategic_insights': strategic_insights})
    else:
        # Write an explicit null so the frontend can distinguish
        # "not yet generated" from a network error loading data.json.
        _patch_data_json(DATA_JSON_PATH, {'strategic_insights': None})
        print('  ⚠️  Insights unavailable — continuing without it')

    # ── Step 9: Build & send email ────────────────────────────────
    print('\nStep 9 ── Build & send email')
    html = build_html_email(
        promotions_data    = all_promos_email,
        scraped_data       = scraped_by_name,
        strategic_insights = strategic_insights,
        new_promos         = new_promos_email,
        new_promos_week    = new_promos_week_email,
    )
    print('  ✅ HTML email built')

    output_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..', 'output', 'email_preview.html',
    )
    _save_html_fallback(html, output_path)

    smtp_ready = all([addr, pwd, to])

    if _NO_EMAIL:
        print('  📴 Email skipped (--no-email)')
        print(f'  📄 HTML preview → {output_path}')
    elif not smtp_ready:
        missing = [
            name for name, val in [
                ('GMAIL_ADDRESS',      addr),
                ('GMAIL_APP_PASSWORD', pwd),
                ('RECIPIENT_EMAIL',    to),
            ] if not val
        ]
        print(f'  ❌ Missing {" / ".join(missing)} — email skipped')
        print(f'  📄 HTML preview → {output_path}')
    else:
        try:
            success = send_email(
                html_content    = html,
                recipient       = to,
                new_promos      = new_promos_email,
                promotions_data = all_promos_email,
            )
            if success:
                print(f'  ✅ Email sent → {to}')
            else:
                print('  ❌ send_email() returned False')
                print(f'  📄 HTML preview → {output_path}')
        except Exception as exc:
            print(f'  ❌ Email failed: {exc}')
            print(f'  📄 HTML preview → {output_path}')

    # ── Done ──────────────────────────────────────────────────────
    elapsed  = time.monotonic() - t_start
    db_stats = get_db_stats()

    print(f'\n{"═"*60}')
    print(
        f'  Done in {elapsed:.1f}s  |  '
        f'🆕 {len(new_promos_email)} new today  |  '
        f'📅 {len(new_promos_week_email)} new this week  |  '
        f'✅ {len(all_promos_email)} active  |  '
        f'❌ {summary["expired_count"]} expired  |  '
        f'🤖 deduped:{total_deduped} matched:{total_db_matched}  |  '
        f'⚙️  {bau_count_insights} BAU  |  '
        f'📦 DB:{db_stats.get("total_promotions", "?")} total'
    )
    print(f'{"═"*60}\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())