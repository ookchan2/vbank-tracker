# scripts/main.py
# Force cache rebuild - 2026-04-28 email fix verification

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
    extract_products,
)
from database  import (
    init_db,
    start_new_run,
    save_promotions,
    save_products,
    mark_stale_as_inactive,
    mark_inactive_old,
    mark_stale_products_as_inactive,
    reactivate_promotions_seen_on,
    reactivate_most_recently_seen,
    migrate_legacy_bank_names,
    generate_daily_report,
    export_to_json,
    get_active_promos_for_bank,
    get_active_promotions,
    get_promotions_by_bank_name,
    get_new_promotions_today,
    get_new_promotions_last_n_days,
    get_new_products_today,
    get_all_active_products,
    get_db_stats,
    repair_reinserted_promotions,
)
from emailer   import build_html_email, send_email

DATA_JSON_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', 'docs', 'data.json',
)

# -- CLI flags ----------------------------------------------------------------─
_NO_EMAIL    = '--no-email'    in sys.argv or '--dry-run' in sys.argv
_SKIP_SCRAPE = '--skip-scrape' in sys.argv


# -- Bank-name canonical map --------------------------------------------------─
# Guards against stale bank_name values still in the DB from before the rename.
# Keys are lowercase; values are the current canonical display names.
# ★ Keep legacy aliases here whenever a bank is renamed in future.
_BANK_NAME_CANONICAL: dict[str, str] = {
    'airstar bank': 'EleBank',
    'paobank':      'PADB',
    'pao bank':     'PADB',
    'paob':         'PADB',
}

# Maps canonical name -> all legacy aliases stored in DB for the same bank.
# Used by the db_fetch_fn fallback wrapper below.
_BANK_NAME_LEGACY_ALIASES: dict[str, list[str]] = {
    'EleBank': ['Airstar Bank'],
    'PADB':    ['PAObank', 'PAO Bank', 'PAOB'],
}


def _canonical_bank_name(raw: str) -> str:
    """Return canonical display name; pass unknowns through unchanged."""
    if not raw:
        return raw
    return _BANK_NAME_CANONICAL.get(raw.strip().lower(), raw.strip())


def _make_db_fetch_fn():
    """
    Wrap get_promotions_by_bank_name with a legacy-alias fallback.

    If the canonical name returns no rows (because DB still has the old name),
    try each legacy alias in order and return the first non-empty result.
    This matters for generate_strategic_insights supplement queries.
    """
    def _fetch(bank_name: str):
        rows = get_promotions_by_bank_name(bank_name)
        if rows:
            return rows
        for alias in _BANK_NAME_LEGACY_ALIASES.get(bank_name, []):
            rows = get_promotions_by_bank_name(alias)
            if rows:
                print(
                    f'  [INFO]  db_fetch_fn: "{bank_name}" returned 0 rows; '
                    f'fell back to legacy alias "{alias}" ({len(rows)} rows)'
                )
                return rows
        return []
    return _fetch


# -- Env helpers --------------------------------------------------------------─

def _read_env() -> tuple[str, str, list[str]]:
    addr = os.environ.get('GMAIL_ADDRESS',      '').strip()
    pwd  = os.environ.get('GMAIL_APP_PASSWORD', '').strip()
    raw  = (
        os.environ.get('RECIPIENT_EMAIL') or
        os.environ.get('EMAIL_RECIPIENT') or
        os.environ.get('EMAIL_TO')        or ''
    ).strip()
    to = [e.strip() for e in raw.split(',') if e.strip()]
    return addr, pwd, to


def _print_env_check(addr: str, pwd: str, to: list[str]) -> None:
    to_display = ', '.join(to) if to else ''
    print('  Env check:')
    print(f'    GMAIL_ADDRESS     : {"[OK] set" if addr else "[ERR] MISSING"}')
    print(f'    GMAIL_APP_PASSWORD: {"[OK] set (hidden)" if pwd else "[ERR] MISSING"}')
    print(f'    RECIPIENT_EMAIL   : {"[OK] " + to_display if to else "[ERR] MISSING"}')
    if _NO_EMAIL:
        print('    [SKIP] --no-email flag - SMTP step will be skipped')


def _save_html_fallback(html: str, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  [FILE] HTML saved -> {path}')


# -- Patch data.json with an arbitrary dict of extra keys --------------------─

def _patch_data_json(path: str, extra: dict) -> None:
    keys = ', '.join(extra.keys())
    try:
        with open(path, 'r', encoding='utf-8') as f:
            jdata = _json.load(f)
        jdata.update(extra)
        with open(path, 'w', encoding='utf-8') as f:
            _json.dump(jdata, f, ensure_ascii=False, indent=2)
        print(f'  [OK] data.json patched with key(s): {keys}')
    except Exception as exc:
        print(f'  [WARN]  data.json patch failed ({keys}): {exc}')


# -- Load data.json from disk as canonical email count source ------------------

def _load_data_json(path: str) -> dict | None:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = _json.load(f)
        n = len(content.get('promotions', []))
        print(f'  [OK] data.json loaded for email stats ({n} promotions in file)')
        return content
    except FileNotFoundError:
        print(f'  [WARN]  data.json not found at {path} - email will use DB rows for stats')
        return None
    except Exception as exc:
        print(f'  [WARN]  data.json load failed: {exc} - email will use DB rows for stats')
        return None


# -- Main pipeline ------------------------------------------------------------─

def main() -> int:
    t_start  = time.monotonic()
    today    = datetime.now().strftime('%Y-%m-%d')
    RUN_DATE = today

    print(f'\n{"="*60}')
    print(f'  HK Virtual Bank Promotions Tracker  |  {today}')
    if _NO_EMAIL:
        print('  MODE: --no-email  (pipeline runs; SMTP skipped)')
    if _SKIP_SCRAPE:
        print('  MODE: --skip-scrape  (re-processing DB data only)')
    print(f'{"="*60}\n')

    addr, pwd, to = _read_env()
    _print_env_check(addr, pwd, to)

    # -- Step 1: Database ------------------------------------------
    print('\nStep 1 -- Init database')
    try:
        init_db()
        current_run_id = start_new_run(banks=list(BANK_CONFIGS.keys()))
    except Exception as exc:
        print(f'  [ERR] Database init failed - cannot continue: {exc}')
        return 1

    # -- Step 2: AI ------------------------------------------------
    print('\nStep 2 -- Init AI')
    ai_ok = init_ai()

    # -- Step 2b: Pre-run DB recovery when AI is unavailable ------─
    _pre_run_recovered = 0
    if not ai_ok:
        _pre_stats  = get_db_stats()
        _pre_total  = _pre_stats.get('total_promotions', 0)
        _pre_active = _pre_stats.get('active_promotions', 0)

        if _pre_total > 0 and _pre_active == 0:
            print(
                f'\n  [ALERT] AI unavailable + 0 active promotions '
                f'(DB has {_pre_total} total) -> attempting DB recovery'
            )
            _pre_run_recovered = reactivate_most_recently_seen(window_days=7)
            if _pre_run_recovered:
                _post = get_db_stats()
                print(
                    f'  [OK] Recovery succeeded: {_pre_run_recovered} promotions restored '
                    f'({_post.get("active_promotions", 0)} active, '
                    f'{_post.get("bau_promotions", 0)} BAU)'
                )
            else:
                print('  [WARN]  Recovery found nothing to restore - DB may be truly empty')
        elif _pre_total > 0 and _pre_active > 0:
            print(
                f'\n  [INFO]  AI unavailable - using existing '
                f'{_pre_active} active promotions from DB for email/website'
            )
        else:
            print('\n  [WARN]  AI unavailable and DB is completely empty')

    # -- Step 3: Scrape all banks ----------------------------------
    print(f'\nStep 3 -- Scrape all {len(BANK_CONFIGS)} banks')
    t3 = time.monotonic()

    if _SKIP_SCRAPE:
        print('  [NEXT]  --skip-scrape: using existing DB data only')
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

    print(f'  [TIME]  Scrape completed in {time.monotonic() - t3:.1f}s')

    if not scraped:
        print('  [ERR] No data scraped - abort')
        return 1

    bank_ids_ok: list[str] = [bid for bid, r in scraped.items() if r.get('success')]

    scraped_by_name: dict = {
        r.get('bank_name', bid): r
        for bid, r in scraped.items()
    }

    # -- Step 4: AI extraction + dedup + save --------------------─
    print('\nStep 4 -- AI extraction')
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
        mark        = '[OK]' if result.get('success') else '[ERR]'
        print(f'\n  [{bank_id.upper()}] {bank_name}  {mark}  ({chars:,} chars)')

        if not ai_ok:
            print('    [WARN]  AI unavailable - skip')
            continue
        if not result.get('success') and not _SKIP_SCRAPE:
            print(f'    [WARN]  Scrape failed - skip AI for {bank_name}')
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
            safe_exc = str(exc).encode('ascii', 'replace').decode('ascii')
            print(f'    [ERR] AI extraction error for {bank_name}: {safe_exc}')
            continue

        if not promos:
            print(f'    [WARN]  0 promotions extracted for {bank_name}')
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
                    f'    [AI] Within-batch dedup: {removed} removed '
                    f'({before} -> {len(promos)}) for {bank_name}'
                )
        except Exception as exc:
            print(f'    [WARN]  Within-batch dedup error for {bank_name}: {exc}')

        if not promos:
            print(f'    [WARN]  0 promotions after within-batch dedup for {bank_name}')
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
                print(f'    [INFO]  No existing DB records for {bank_name} - all will be new')
        except Exception as exc:
            print(f'    [WARN]  DB-match error for {bank_name}: {exc} - formula pass only')

        # 4d: Normalize bank name and save to DB
        total_extracted += len(promos)
        canonical_name = _canonical_bank_name(bank_name)
        if canonical_name != bank_name:
            print(f'    [INFO]  Normalized bank name: "{bank_name}" -> "{canonical_name}"')
            bank_name = canonical_name
        try:
            db_result = save_promotions(
                bank_id, bank_name, promos,
                current_run_id = current_run_id,
                today_str      = RUN_DATE,
            )
        except Exception as exc:
            print(f'    [ERR] save_promotions error for {bank_name}: {exc}')
            continue

        banks_ai_saved.append(bank_id)
        total_new     += db_result['new']
        total_updated += db_result['updated']
        print(
            f"    [OK] {db_result['new']} new, {db_result['updated']} updated, "
            f"{db_result['skipped']} skipped - {bank_name}"
        )

    print(f'  [TIME]  AI extraction completed in {time.monotonic() - t4:.1f}s')
    print(
        f"\n[STATS] Extracted:{total_extracted}  New:{total_new}  Updated:{total_updated}  "
        f"Deduped:{total_deduped}  DB-matched:{total_db_matched}"
    )

    # -- Step 4b: Product extraction --------------------------------
    if ai_ok:
        print('\nStep 4b -- Extract banking products from scraped text')
        total_products_new = 0
        total_products_updated = 0

        for bank_id, result in scraped.items():
            bank_name = result.get('bank_name', bank_id)

            if not result.get('success') and not _SKIP_SCRAPE:
                continue
            if len(result.get('text', '')) < 500:
                continue

            try:
                products = extract_products(bank_id, bank_name, result.get('text', ''))
                if products:
                    prod_result = save_products(bank_id, bank_name, products, today_str=RUN_DATE)
                    total_products_new += prod_result['new']
                    total_products_updated += prod_result['updated']
            except Exception as exc:
                print(f'    [WARN] Product extraction error for {bank_name}: {exc}')

        print(f'  [PRODUCTS] Summary: {total_products_new} new, {total_products_updated} updated across all banks')
    else:
        print('\nStep 4b -- Product extraction skipped (AI unavailable)')

    # -- Step 5: Mark stale / old inactive ------------------------
    print('\nStep 5 -- Mark stale / old promos inactive')

    if not ai_ok:
        print(
            '  [WARN]  AI unavailable - skipping mark_stale_as_inactive and '
            'mark_inactive_old to preserve existing data'
        )
    elif not banks_ai_saved:
        print(
            '  [WARN]  No banks were successfully saved this run - '
            'skipping mark_stale_as_inactive to avoid false-expiry'
        )
    else:
        mark_stale_as_inactive(banks_ai_saved, today_str=RUN_DATE)
        mark_inactive_old(days_threshold=90)
        # Also mark products as inactive if not seen today
        mark_stale_products_as_inactive()

    # -- Step 5b: Post-staleness sanity check --------------------─
    _active_after_stale = get_active_promotions(include_bau=True)
    if not _active_after_stale and banks_ai_saved:
        print(
            f'  [ALERT] CRITICAL: 0 active promotions after mark_stale_as_inactive! '
            f'Triggering date-skew recovery for RUN_DATE={RUN_DATE}'
        )
        recovered = reactivate_promotions_seen_on(RUN_DATE)
        if not recovered:
            print(
                '  [ERR] Recovery found nothing - attempting broad recovery'
            )
            reactivate_most_recently_seen(window_days=7)
    elif not _active_after_stale and not banks_ai_saved:
        print(
            '  [WARN]  Still 0 active promotions after Step 2b recovery attempt'
        )

    # -- Step 5b: Migrate legacy bank names --------------------
    print('\nStep 5b -- Migrate legacy bank names to canonical names')
    try:
        migrated_count = migrate_legacy_bank_names(dry_run=False)
        if migrated_count > 0:
            print(f'  [OK]  Migrated {migrated_count} legacy bank name(s)')
    except Exception as exc:
        print(f'  [WARN]  Bank name migration error: {exc} - continuing')

    # -- Step 5c: Repair re-inserted promotions --------------------
    print('\nStep 5c -- Repair re-inserted promotions')
    if ai_ok:
        try:
            repair_reinserted_promotions(dry_run=False)
        except Exception as exc:
            print(f'  [WARN]  repair_reinserted_promotions error: {exc} - continuing')
    else:
        print('  [NEXT]  Skipping repair - AI unavailable this run (no new insertions possible)')

    # -- Step 6: Export data.json for website --------------------─
    print('\nStep 6 -- Export data.json for website')

    _active_for_export = get_active_promotions(include_bau=True)
    if not _active_for_export:
        print(
            '  [WARN]  Skipping data.json export - 0 active promotions in DB '
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
            print(f'  [OK] data.json timestamp patched -> {_run_ts}')
            if not ai_ok:
                print('  [INFO]  data.json flagged as cached (ai_unavailable=true)')
        except Exception as exc:
            print(f'  [WARN]  data.json timestamp patch failed: {exc}')

    # -- Step 6b: Load data.json as canonical count source for email ----------─
    print('\nStep 6b -- Load data.json for email count source')
    data_json_content = _load_data_json(DATA_JSON_PATH)

    # -- Step 7: Daily report --------------------------------------
    print('\nStep 7 -- Generate daily report')
    report         = generate_daily_report(current_run_id)
    active_promos  = report['active']
    expired_promos = report['expired']
    summary        = report['summary']

    print(f'  [NEW] New:     {summary["new_count"]}')
    print(f'  [OK] Active:  {len(active_promos)}')
    print(f'  [ERR] Expired: {summary["expired_count"]}')
    for bid, count in summary['by_bank'].items():
        print(f'    {bid.upper()}: {count} active')

    # -- Step 8: Strategic insights --------------------------------
    print('\nStep 8 -- Generate AI strategic insights')
    all_active_with_bau = get_active_promotions(include_bau=True)
    bau_count_insights  = sum(1 for p in all_active_with_bau if p.get('is_bau', False))

    print(
        f'  [STATS] Insights input: {len(all_active_with_bau)} promos '
        f'({bau_count_insights} BAU + '
        f'{len(all_active_with_bau) - bau_count_insights} time-limited)'
    )

    # ★ BUILD promos_by_name with canonical name normalisation.
    #   Without this, stale DB bank_name values ("Airstar Bank", "PAObank") cause
    #   the AI to receive fragmented / mislabelled bank groups.
    promos_by_name: dict = {}
    for p in all_active_with_bau:
        bname = p.get('bank_name') or p.get('bName') or p.get('bank') or 'Unknown'
        bname = _canonical_bank_name(bname)   # ★ normalise legacy/stale DB names
        promos_by_name.setdefault(bname, []).append(p)

    # Diagnostic: warn if any legacy names slipped through normalisation
    _legacy_names = {'Airstar Bank', 'PAObank', 'PAO Bank', 'PAOB'}
    _found_legacy = _legacy_names & set(promos_by_name.keys())
    if _found_legacy:
        print(
            f'  [WARN]  Legacy bank name(s) still in promos_by_name after normalisation: '
            f'{_found_legacy} - run migrate_bank_names.py to fix DB rows'
        )

    all_promos_email = [p for p in all_active_with_bau if not p.get('is_bau', False)]

    new_promos_email = get_new_promotions_today(include_bau=False)

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
    print(f'  [INFO] Banks in promos_by_name:    {sorted(promos_by_name.keys())}')

    strategic_insights = None
    if ai_ok and promos_by_name:
        try:
            # ★ Pass legacy-alias-aware db_fetch_fn so supplement queries
            #   still work when DB has stale bank_name values.
            strategic_insights = generate_strategic_insights(
                promos_by_name,
                db_fetch_fn=_make_db_fetch_fn(),
            )
        except Exception as exc:
            print(f'  [WARN]  Insights error: {exc}')

    if strategic_insights:
        _patch_data_json(DATA_JSON_PATH, {'strategic_insights': strategic_insights})
    else:
        _patch_data_json(DATA_JSON_PATH, {'strategic_insights': None})
        print('  [WARN]  Insights unavailable - continuing without it')

    # -- Step 8b: Reload data.json after insights patch ------------------------
    if strategic_insights:
        print('\nStep 8b -- Reload data.json after insights patch')
        _reloaded = _load_data_json(DATA_JSON_PATH)
        if _reloaded is not None:
            data_json_content = _reloaded

    # -- Step 9: Build & send email --------------------------------
    print('\nStep 9 -- Build & send email')

    # Get new products for email
    new_products = get_new_products_today() if ai_ok else []

    html = build_html_email(
        promotions_data    = all_promos_email,
        scraped_data       = data_json_content,
        strategic_insights = strategic_insights,
        new_promos         = new_promos_email,
        new_promos_week    = new_promos_week_email,
        new_products       = new_products,
        ai_unavailable     = not ai_ok,
    )
    print('  [OK] HTML email built')

    output_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..', 'output', 'email_preview.html',
    )
    _save_html_fallback(html, output_path)

    smtp_ready = all([addr, pwd, to])

    email_subject = (
        f'[BANK] VBank Daily Report - {datetime.now().strftime("%d %b %Y")} '
        f'{"[Cached Data - AI Unavailable]" if not ai_ok else ""}'
    ).strip()

    if _NO_EMAIL:
        print('  [SKIP] Email skipped (--no-email)')
        print(f'  [FILE] HTML preview -> {output_path}')
    elif not smtp_ready:
        missing = [
            name for name, val in [
                ('GMAIL_ADDRESS',      addr),
                ('GMAIL_APP_PASSWORD', pwd),
                ('RECIPIENT_EMAIL',    to),
            ] if not val
        ]
        print(f'  [ERR] Missing {" / ".join(missing)} - email skipped')
        print(f'  [FILE] HTML preview -> {output_path}')
    else:
        try:
            success = send_email(
                html_content    = html,
                subject         = email_subject,
                recipient       = to,
                new_promos      = new_promos_email,
                new_promos_week = new_promos_week_email,
                promotions_data = all_promos_email,
                ai_unavailable  = not ai_ok,
                scraped_data    = data_json_content,
            )
            if success:
                print(f'  [OK] Email sent -> {", ".join(to)}')
            else:
                print('  [ERR] send_email() returned False')
                print(f'  [FILE] HTML preview -> {output_path}')
        except Exception as exc:
            print(f'  [ERR] Email failed: {exc}')
            print(f'  [FILE] HTML preview -> {output_path}')

    # -- Done ------------------------------------------------------
    elapsed  = time.monotonic() - t_start
    db_stats = get_db_stats()

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

    print(f'\n{"="*60}')
    print(
        f'  Done in {elapsed:.1f}s  |  '
        f'[NEW] {len(new_promos_email)} new today  |  '
        f'[WEEK] {len(new_promos_week_email)} new this week  |  '
        f'[OK] {len(all_promos_email)} active (DB)  |  '
        f'[FILE] {_summary_total} active (data.json)  |  '
        f'[ERR] {summary["expired_count"]} expired  |  '
        f'[AI] deduped:{total_deduped} matched:{total_db_matched}  |  '
        f'[CONFIG]  {bau_count_insights} BAU  |  '
        f'[DB] DB:{db_stats.get("total_promotions", "?")} total'
        + (f'  |  [WARN]  AI UNAVAILABLE (cached)' if not ai_ok else '')
    )
    print(f'{"="*60}\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())