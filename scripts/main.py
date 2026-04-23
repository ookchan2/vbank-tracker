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
    reactivate_promotions_seen_on,
    reactivate_most_recently_seen,
    generate_daily_report,
    export_to_json,
    get_active_promos_for_bank,
    get_active_promotions,
    get_promotions_by_bank_name,
    get_new_promotions_today,
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


# ── ★ NEW: load data.json from disk as canonical email count source ───────────
# Called after Step 6 (export) so we always read the file that the website
# serves — whether freshly written this run or preserved from a prior run.
#
# ROOT CAUSE of email 52/45/7 vs website 47/41/6:
#   main.py previously passed scraped_by_name (raw scrape dict, no 'promotions'
#   key) to build_html_email(scraped_data=…).  emailer._resolve_count_source()
#   fell back to promotions_data (raw DB rows) which contained stale rows the
#   AI had already expired in data.json, inflating the email count by +5.
#
# FIX: load data.json here and pass it as scraped_data to both
#   build_html_email() and send_email() so emailer._resolve_count_source()
#   picks data.json['promotions'] — identical to what the website reads.

def _load_data_json(path: str) -> dict | None:
    """
    Read data.json from disk and return its parsed content.

    Returns None (with a warning) if the file is missing or unparseable;
    the emailer gracefully falls back to promotions_data (DB rows) in that case.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = _json.load(f)
        n = len(content.get('promotions', []))
        print(f'  ✅ data.json loaded for email stats ({n} promotions in file)')
        return content
    except FileNotFoundError:
        print(f'  ⚠️  data.json not found at {path} — email will use DB rows for stats')
        return None
    except Exception as exc:
        print(f'  ⚠️  data.json load failed: {exc} — email will use DB rows for stats')
        return None


# ── Main pipeline ─────────────────────────────────────────────────────────────

def main() -> int:
    t_start  = time.monotonic()
    today    = datetime.now().strftime('%Y-%m-%d')
    RUN_DATE = today

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

    # ── Step 2b: Pre-run DB recovery when AI is unavailable ───────
    _pre_run_recovered = 0
    if not ai_ok:
        _pre_stats  = get_db_stats()
        _pre_total  = _pre_stats.get('total_promotions', 0)
        _pre_active = _pre_stats.get('active_promotions', 0)

        if _pre_total > 0 and _pre_active == 0:
            print(
                f'\n  🚨 AI unavailable + 0 active promotions '
                f'(DB has {_pre_total} total) → attempting DB recovery'
            )
            _pre_run_recovered = reactivate_most_recently_seen(window_days=7)
            if _pre_run_recovered:
                _post = get_db_stats()
                print(
                    f'  ✅ Recovery succeeded: {_pre_run_recovered} promotions restored '
                    f'({_post.get("active_promotions", 0)} active, '
                    f'{_post.get("bau_promotions", 0)} BAU)'
                )
            else:
                print('  ⚠️  Recovery found nothing to restore — DB may be truly empty')
        elif _pre_total > 0 and _pre_active > 0:
            print(
                f'\n  ℹ️  AI unavailable — using existing '
                f'{_pre_active} active promotions from DB for email/website'
            )
        else:
            print('\n  ⚠️  AI unavailable and DB is completely empty')

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

    # NOTE: scraped_by_name is the raw scrape-result dict keyed by bank name.
    # It is NOT the data.json content and must NOT be passed as scraped_data
    # to build_html_email / send_email (that was the original bug).
    # It is retained here only in case other pipeline steps need it.
    scraped_by_name: dict = {
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
    banks_ai_saved: list[str] = []

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

        # 4a: Extract promotions
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

        # 4b: Within-batch dedup
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

        # 4c: Match against existing DB records
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

        # 4d: Save to DB
        total_extracted += len(promos)
        try:
            db_result = save_promotions(
                bank_id, bank_name, promos,
                current_run_id = current_run_id,
                today_str      = RUN_DATE,
            )
        except Exception as exc:
            print(f'    ❌ save_promotions error for {bank_name}: {exc}')
            continue

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
        print(
            '  ⚠️  AI unavailable — skipping mark_stale_as_inactive and '
            'mark_inactive_old to preserve existing data'
        )
    elif not banks_ai_saved:
        print(
            '  ⚠️  No banks were successfully saved this run — '
            'skipping mark_stale_as_inactive to avoid false-expiry'
        )
    else:
        mark_stale_as_inactive(banks_ai_saved, today_str=RUN_DATE)
        mark_inactive_old(days_threshold=90)

    # ── Step 5b: Post-staleness sanity check ─────────────────────
    _active_after_stale = get_active_promotions(include_bau=True)
    if not _active_after_stale and banks_ai_saved:
        print(
            f'  🚨 CRITICAL: 0 active promotions after mark_stale_as_inactive! '
            f'Triggering date-skew recovery for RUN_DATE={RUN_DATE}'
        )
        recovered = reactivate_promotions_seen_on(RUN_DATE)
        if not recovered:
            print(
                '  ❌ Recovery found nothing — attempting broad recovery'
            )
            reactivate_most_recently_seen(window_days=7)
    elif not _active_after_stale and not banks_ai_saved:
        print(
            '  ⚠️  Still 0 active promotions after Step 2b recovery attempt'
        )

    # ── Step 6: Export data.json for website ─────────────────────
    print('\nStep 6 ── Export data.json for website')

    _active_for_export = get_active_promotions(include_bau=True)
    if not _active_for_export:
        print(
            '  ⚠️  Skipping data.json export — 0 active promotions in DB '
            '(preserving existing file)'
        )
    else:
        export_to_json(DATA_JSON_PATH)

        _run_ts      = datetime.now().strftime('%Y-%m-%d %H:%M')
        _extra_patch = {'updated': _run_ts, 'last_updated': _run_ts}
        if not ai_ok:
            _extra_patch['ai_unavailable'] = True
            _extra_patch['cached_data']    = True
        try:
            with open(DATA_JSON_PATH, 'r', encoding='utf-8') as _f:
                _jdata = _json.load(_f)
            _jdata.update(_extra_patch)
            with open(DATA_JSON_PATH, 'w', encoding='utf-8') as _f:
                _json.dump(_jdata, _f, ensure_ascii=False, indent=2)
            print(f'  ✅ data.json timestamp patched → {_run_ts}')
            if not ai_ok:
                print('  ℹ️  data.json flagged as cached (ai_unavailable=true)')
        except Exception as exc:
            print(f'  ⚠️  data.json timestamp patch failed: {exc}')

    # ── Step 6b: Load data.json as canonical count source for email ───────────
    # ★ FIX: this is the key change that aligns email stats with the website.
    #
    # The website reads data.json['promotions'] directly.
    # The emailer must use the same list — not the raw DB rows — so that the
    # header numbers (total / active / expiring) are identical in both places.
    #
    # We load data.json here (after it has been written / patched above) and
    # pass it as scraped_data to build_html_email() and send_email().
    # emailer._resolve_count_source() will detect the 'promotions' key and
    # use it instead of falling back to the DB-derived promotions_data list.
    #
    # This works correctly in both cases:
    #   • AI ran     → data.json was just exported and patched above
    #   • AI skipped → data.json is the preserved file from the last good run
    #     (same file the website is serving right now)
    print('\nStep 6b ── Load data.json for email count source')
    data_json_content = _load_data_json(DATA_JSON_PATH)

    # ── Step 7: Daily report ──────────────────────────────────────
    print('\nStep 7 ── Generate daily report')
    report         = generate_daily_report(current_run_id)
    active_promos  = report['active']
    expired_promos = report['expired']
    summary        = report['summary']

    print(f'  🆕 New:     {summary["new_count"]}')
    print(f'  ✅ Active:  {len(active_promos)}')
    print(f'  ❌ Expired: {summary["expired_count"]}')
    for bid, count in summary['by_bank'].items():
        print(f'    {bid.upper()}: {count} active')

    # ── Step 8: Strategic insights ────────────────────────────────
    print('\nStep 8 ── Generate AI strategic insights')
    all_active_with_bau = get_active_promotions(include_bau=True)
    bau_count_insights  = sum(1 for p in all_active_with_bau if p.get('is_bau', False))

    print(
        f'  📊 Insights input: {len(all_active_with_bau)} promos '
        f'({bau_count_insights} BAU + '
        f'{len(all_active_with_bau) - bau_count_insights} time-limited)'
    )

    promos_by_name: dict = {}
    for p in all_active_with_bau:
        bname = p.get('bank_name') or p.get('bName') or p.get('bank') or 'Unknown'
        promos_by_name.setdefault(bname, []).append(p)

    # Non-BAU active list used for the email body (promo cards, bank breakdown).
    # This comes from the DB and is used as promotions_data (card content only).
    # Stats (total / active / expiring counters) are derived from data_json_content
    # inside the emailer via _resolve_count_source() — NOT from this list.
    all_promos_email = [p for p in all_active_with_bau if not p.get('is_bau', False)]

    # New today (HKT date-based — matches website isNewToday())
    new_promos_email = get_new_promotions_today(include_bau=False)

    # New in the past 6 days excluding today (matches website isNewThisWeek())
    new_promos_week_raw   = get_new_promotions_last_n_days(
        days        = 6,
        include_bau = False,
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
        _patch_data_json(DATA_JSON_PATH, {'strategic_insights': strategic_insights})
    else:
        _patch_data_json(DATA_JSON_PATH, {'strategic_insights': None})
        print('  ⚠️  Insights unavailable — continuing without it')

    # ── Step 8b: Reload data.json after insights patch ────────────────────────
    # The insights patch may have changed data.json.  Reload so that the
    # data_json_content passed to the emailer includes the latest strategic_insights
    # field (used only for display; does not affect the stats count).
    if strategic_insights:
        print('\nStep 8b ── Reload data.json after insights patch')
        _reloaded = _load_data_json(DATA_JSON_PATH)
        if _reloaded is not None:
            data_json_content = _reloaded

    # ── Step 9: Build & send email ────────────────────────────────
    print('\nStep 9 ── Build & send email')

    # ★ FIX: pass data_json_content (the actual data.json dict) as scraped_data.
    #
    # Previously scraped_by_name was passed here.  scraped_by_name is keyed by
    # bank name and contains raw scrape results — it has no 'promotions' key, so
    # emailer._resolve_count_source() fell back to promotions_data (DB rows) and
    # produced inflated counts (email showed 52/45/7 instead of website's 47/41/6).
    #
    # With data_json_content, _resolve_count_source() finds 'promotions' and uses
    # data.json as the single source of truth for all stats, identical to the website.
    html = build_html_email(
        promotions_data    = all_promos_email,
        scraped_data       = data_json_content,    # ★ CHANGED: was scraped_by_name
        strategic_insights = strategic_insights,
        new_promos         = new_promos_email,
        new_promos_week    = new_promos_week_email,
        ai_unavailable     = not ai_ok,
    )
    print('  ✅ HTML email built')

    output_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..', 'output', 'email_preview.html',
    )
    _save_html_fallback(html, output_path)

    smtp_ready = all([addr, pwd, to])

    email_subject = (
        f'🏦 VBank Daily Report — {datetime.now().strftime("%d %b %Y")} '
        f'{"[Cached Data — AI Unavailable]" if not ai_ok else ""}'
    ).strip()

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
            # ★ FIX: pass scraped_data=data_json_content so the plain-text
            # part of the email (MIMEText 'plain') also uses data.json for its
            # stats, not the DB rows.  Without this the plain-text body would
            # still show the old inflated numbers.
            success = send_email(
                html_content    = html,
                subject         = email_subject,
                recipient       = to,
                new_promos      = new_promos_email,
                new_promos_week = new_promos_week_email,
                promotions_data = all_promos_email,
                ai_unavailable  = not ai_ok,
                scraped_data    = data_json_content,   # ★ NEW parameter
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

    # ── Done summary: report what the email actually showed ───────
    # Use data_json_content for the summary counts so the console output
    # matches the email (47/41/6) rather than the raw DB counts (52/45/7).
    _summary_promos  = (
        data_json_content.get('promotions', [])
        if data_json_content else all_promos_email
    )
    _summary_non_bau = [p for p in _summary_promos if not p.get('is_bau', False)]
    _summary_total   = sum(
        1 for p in _summary_non_bau
        if p.get('active') is not False
        and (not p.get('end_date') or str(p['end_date'])[:10] >= today)
    )

    print(f'\n{"═"*60}')
    print(
        f'  Done in {elapsed:.1f}s  |  '
        f'🆕 {len(new_promos_email)} new today  |  '
        f'📅 {len(new_promos_week_email)} new this week  |  '
        f'✅ {len(all_promos_email)} active (DB)  |  '
        f'📄 {_summary_total} active (data.json)  |  '
        f'❌ {summary["expired_count"]} expired  |  '
        f'🤖 deduped:{total_deduped} matched:{total_db_matched}  |  '
        f'⚙️  {bau_count_insights} BAU  |  '
        f'📦 DB:{db_stats.get("total_promotions", "?")} total'
        + (f'  |  ⚠️  AI UNAVAILABLE (cached)' if not ai_ok else '')
    )
    print(f'{"═"*60}\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())