#!/usr/bin/env python3
"""
migrate_bank_names.py
─────────────────────
One-time migration: rename stale bank_name values in promotions.db
to their current canonical names.

Usage:
  python scripts/migrate_bank_names.py            # apply changes
  python scripts/migrate_bank_names.py --dry-run  # preview only
"""

import argparse
import sqlite3
import sys
from pathlib import Path

# ── Canonical rename map ──────────────────────────────────────────────────────
# Format: { "old / stale name in DB" : "new canonical name" }
RENAME_MAP = {
    "Airstar Bank":   "EleBank",
    "PAObank":        "PADB",
    "livi Bank":      "livi bank",   # case normalisation
    "WeLab":          "WeLab Bank",
    "Mox":            "Mox Bank",
    "ZA":             "ZA Bank",
    "Ant Bank HK":    "Ant Bank",
    "Fusion":         "Fusion Bank",
}

# ── DB path ───────────────────────────────────────────────────────────────────
ROOT    = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "promotions.db"

# ── Tables + columns to migrate ───────────────────────────────────────────────
# Each entry: (table_name, column_name)
TARGETS = [
    ("promotions",         "bank_name"),
    ("promotions_archive", "bank_name"),   # skipped gracefully if table absent
]


def get_connection() -> sqlite3.Connection:
    if not DB_PATH.exists():
        print(f"❌  Database not found: {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    return sqlite3.connect(DB_PATH)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    return cur.fetchone() is not None


def preview(conn: sqlite3.Connection) -> dict:
    """Return { (table, old_name): count } for every stale name that exists."""
    found = {}
    for table, col in TARGETS:
        if not table_exists(conn, table):
            print(f"   ⚠️  Table '{table}' not found — skipping.")
            continue
        for old, new in RENAME_MAP.items():
            cur = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} = ?", (old,)
            )
            n = cur.fetchone()[0]
            if n:
                found[(table, old, new)] = n
    return found


def apply(conn: sqlite3.Connection, found: dict) -> None:
    for (table, old, new), count in found.items():
        _, col = next((t, c) for t, c in TARGETS if t == table)
        conn.execute(
            f"UPDATE {table} SET {col} = ? WHERE {col} = ?", (new, old)
        )
        print(f"   ✅  {table}.{col}: '{old}' → '{new}'  ({count} rows)")
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Migrate stale bank names in promotions.db")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing anything")
    args = parser.parse_args()

    print(f"\n{'🔍 DRY RUN — no changes will be written' if args.dry_run else '🔧 APPLYING migration'}")
    print(f"   DB: {DB_PATH}\n")

    conn  = get_connection()
    found = preview(conn)

    if not found:
        print("✨  Nothing to migrate — all bank names are already canonical.")
        conn.close()
        sys.exit(0)

    print(f"Found {len(found)} rename(s) to apply:\n")
    for (table, old, new), count in found.items():
        print(f"   • [{table}] '{old}' → '{new}'  ({count} row{'s' if count != 1 else ''})")

    if args.dry_run:
        print("\n⚠️  Dry-run mode — nothing written. Re-run without --dry-run to apply.")
    else:
        print()
        apply(conn, found)
        print("\n🎉  Migration complete.")

    conn.close()


if __name__ == "__main__":
    main()