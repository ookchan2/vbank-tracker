#!/usr/bin/env python3
"""
One-time cleanup utility for the promotions database.

Usage (from project root):
    python scripts/cleanup_reset.py --dry-run         # preview only
    python scripts/cleanup_reset.py                    # formula pass only
    python scripts/cleanup_reset.py --ai               # formula + AI pass
    python scripts/cleanup_reset.py --purge mox        # delete ALL mox records
    python scripts/cleanup_reset.py --summary          # show DB state only
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import _get_conn, _normalize_title, _jaccard_similarity, _common_prefix_ratio
from ai_helper import AI_AVAILABLE, init_ai, ai_dedup_titles


# ── More aggressive thresholds than live dedup ────────────────────────────────
_CLEANUP_JACCARD = 0.22   # lower = more aggressive (live: 0.28)
_CLEANUP_LCP     = 0.55   # lower = more aggressive (live: 0.60)
_CLEANUP_MIN_LEN = 6


def show_summary():
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT bank_id, active, COUNT(*) as cnt "
            "FROM promotions GROUP BY bank_id, active ORDER BY bank_id, active"
        ).fetchall()

        print("\n  📊 Current DB state:")
        print(f"  {'Bank':<16} {'Active':>8} {'Inactive':>10} {'Total':>8}")
        print(f"  {'-'*16} {'-'*8} {'-'*10} {'-'*8}")

        by_bank: dict = {}
        for row in rows:
            b = row['bank_id']
            by_bank.setdefault(b, {'active': 0, 'inactive': 0})
            by_bank[b]['active' if row['active'] else 'inactive'] = row['cnt']

        ta = ti = 0
        for bank_id, c in sorted(by_bank.items()):
            a, i = c['active'], c['inactive']
            ta += a; ti += i
            print(f"  {bank_id:<16} {a:>8} {i:>10} {a+i:>8}")

        print(f"  {'-'*16} {'-'*8} {'-'*10} {'-'*8}")
        print(f"  {'TOTAL':<16} {ta:>8} {ti:>10} {ta+ti:>8}\n")
    finally:
        conn.close()


def formula_merge(dry_run: bool = True) -> int:
    """
    Aggressive formula-based dedup pass.
    Operates on ALL active records with lower thresholds than the live guard.
    """
    conn = _get_conn()
    merged = 0
    try:
        rows = conn.execute(
            "SELECT id, bank_id, title, highlight "
            "FROM promotions WHERE active = 1 ORDER BY id ASC"
        ).fetchall()

        by_bank: dict = {}
        for row in rows:
            by_bank.setdefault(row['bank_id'], []).append(dict(row))

        discard_ids: set = set()

        for bank_id, promos in by_bank.items():
            bank_hits = 0
            for i, pa in enumerate(promos):
                if pa['id'] in discard_ids:
                    continue
                norm_a = _normalize_title(pa['title'])
                hi_a   = (pa['highlight'] or '').strip()[:150]

                for pb in promos[i + 1:]:
                    if pb['id'] in discard_ids:
                        continue
                    norm_b = _normalize_title(pb['title'])
                    hi_b   = (pb['highlight'] or '').strip()[:150]

                    is_dup = False
                    reason = ''

                    if norm_a and norm_b:
                        if norm_a == norm_b:
                            is_dup, reason = True, 'exact'
                        elif norm_a in norm_b or norm_b in norm_a:
                            is_dup, reason = True, 'substring'
                        else:
                            j = _jaccard_similarity(pa['title'], pb['title'])
                            if j >= _CLEANUP_JACCARD:
                                is_dup, reason = True, f'Jaccard={j:.2f}'
                            elif (
                                len(norm_a) >= _CLEANUP_MIN_LEN
                                and len(norm_b) >= _CLEANUP_MIN_LEN
                            ):
                                lcp = _common_prefix_ratio(norm_a, norm_b)
                                if lcp >= _CLEANUP_LCP:
                                    is_dup, reason = True, f'LCP={lcp:.2f}'

                    if not is_dup and hi_a and hi_b and hi_a == hi_b:
                        is_dup, reason = True, 'same-highlight'

                    if is_dup:
                        keep    = pa if len(pa['title']) >= len(pb['title']) else pb
                        discard = pb if keep['id'] == pa['id'] else pa
                        discard_ids.add(discard['id'])
                        bank_hits += 1
                        tag = '[DRY RUN] ' if dry_run else ''
                        print(
                            f"    🔀 {tag}[{reason}]\n"
                            f"       KEEP    #{keep['id']:>5}  '{keep['title'][:70]}'\n"
                            f"       DISCARD #{discard['id']:>5}  '{discard['title'][:70]}'"
                        )

            if bank_hits:
                print(f"\n  [{bank_id.upper()}] formula found {bank_hits} duplicate(s)\n")

        if not dry_run and discard_ids:
            conn.execute(
                f"UPDATE promotions SET active = 0 "
                f"WHERE id IN ({','.join('?' * len(discard_ids))})",
                list(discard_ids),
            )
            conn.commit()

        merged = len(discard_ids)
        tag = '[DRY RUN] ' if dry_run else ''
        print(f"  {tag}Formula pass total: {merged} duplicate(s)")
        return merged

    except Exception as e:
        conn.rollback()
        print(f'  ❌ formula_merge error: {e}')
        return 0
    finally:
        conn.close()


def ai_merge(dry_run: bool = True) -> int:
    """
    AI-based semantic dedup pass — catches synonyms the formula misses.
    Run after formula_merge() for best results.
    """
    if not AI_AVAILABLE:
        print("  ⚠️  AI not available — skipping AI merge pass")
        return 0

    conn = _get_conn()
    total = 0

    try:
        rows = conn.execute(
            "SELECT id, bank_id, title FROM promotions "
            "WHERE active = 1 ORDER BY bank_id, id"
        ).fetchall()

        by_bank: dict = {}
        for row in rows:
            by_bank.setdefault(row['bank_id'], []).append(dict(row))

        for bank_id, promos in by_bank.items():
            if len(promos) < 2:
                continue

            print(f"\n  🤖 AI pass: {bank_id.upper()} ({len(promos)} promos) ...")
            titles  = [p['title'] for p in promos]
            dup_map = ai_dedup_titles(titles, bank_id)

            if not dup_map:
                print(f"     No AI duplicates found")
                continue

            discard_ids = []
            for dup_idx, keep_idx in dup_map.items():
                if 0 <= dup_idx < len(promos) and 0 <= keep_idx < len(promos):
                    dup_row  = promos[dup_idx]
                    keep_row = promos[keep_idx]
                    tag = '[DRY RUN] ' if dry_run else ''
                    print(
                        f"     🔀 {tag}\n"
                        f"        KEEP    #{keep_row['id']:>5}  '{keep_row['title'][:70]}'\n"
                        f"        DISCARD #{dup_row['id']:>5}  '{dup_row['title'][:70]}'"
                    )
                    discard_ids.append(dup_row['id'])

            if not dry_run and discard_ids:
                conn.execute(
                    f"UPDATE promotions SET active = 0 "
                    f"WHERE id IN ({','.join('?' * len(discard_ids))})",
                    discard_ids,
                )
                conn.commit()

            total += len(discard_ids)

        tag = '[DRY RUN] ' if dry_run else ''
        print(f"\n  {tag}AI pass total: {total} duplicate(s)")
        return total

    except Exception as e:
        conn.rollback()
        print(f'  ❌ ai_merge error: {e}')
        return 0
    finally:
        conn.close()


def purge_bank(bank_id: str, dry_run: bool = True) -> int:
    """
    Hard-delete ALL records (active + inactive) for one bank.
    Use when a bank's data is too messy to fix — re-scrape after.
    """
    conn = _get_conn()
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM promotions WHERE bank_id = ?", (bank_id,)
        ).fetchone()[0]

        if dry_run:
            print(f"  [DRY RUN] Would delete {count} record(s) for '{bank_id}'")
        else:
            confirm = input(
                f"  ⚠️  This permanently deletes {count} records for '{bank_id}'. "
                f"Type bank_id to confirm: "
            ).strip()
            if confirm != bank_id:
                print("  ❌ Cancelled")
                return 0
            conn.execute("DELETE FROM promotions WHERE bank_id = ?", (bank_id,))
            conn.commit()
            print(f"  🗑️  Deleted {count} record(s) for '{bank_id}'")

        return count
    except Exception as e:
        conn.rollback()
        print(f'  ❌ purge_bank error: {e}')
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Cleanup duplicate promotions in DB'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes only, no DB writes')
    parser.add_argument('--ai',      action='store_true',
                        help='Also run AI semantic dedup pass after formula pass')
    parser.add_argument('--purge',   metavar='BANK_ID',
                        help='Delete ALL records for one bank (use with caution)')
    parser.add_argument('--summary', action='store_true',
                        help='Show DB summary only, then exit')
    args = parser.parse_args()

    print(f'\n{"═"*64}')
    print(f'  Promotions DB Cleanup Utility')
    print(f'{"═"*64}')

    show_summary()

    if args.summary:
        sys.exit(0)

    if args.purge:
        print(f'\n🗑️  Purging bank: {args.purge.lower()}')
        purge_bank(args.purge.lower(), dry_run=args.dry_run)
        show_summary()
        sys.exit(0)

    print('📐 Formula-based dedup pass...\n')
    n1 = formula_merge(dry_run=args.dry_run)

    n2 = 0
    if args.ai:
        print('\n🤖 AI-based dedup pass...')
        init_ai()
        n2 = ai_merge(dry_run=args.dry_run)

    print(f'\n{"═"*64}')
    verb = 'Would remove' if args.dry_run else 'Removed'
    print(
        f'  {verb}: {n1} (formula) + {n2} (AI) = {n1 + n2} duplicate(s) total'
    )
    if args.dry_run:
        print('  💡 Re-run without --dry-run to apply changes')
    print(f'{"═"*64}\n')

    if not args.dry_run:
        show_summary()