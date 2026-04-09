#!/usr/bin/env python3
# scripts/main.py
"""
VBank Tracker — main entry point.
Scrapes HK virtual bank promotion pages, stores in SQLite,
writes docs/data.json for the GitHub Pages frontend,
generates strategic insights via AI, and sends the daily email digest.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR   = SCRIPT_DIR.parent
DB_PATH    = ROOT_DIR / "data"   / "promotions.db"
DATA_JSON  = ROOT_DIR / "docs"   / "data.json"

# Ensure scripts/ is importable
sys.path.insert(0, str(SCRIPT_DIR))

import ai_helper
from emailer import send_daily_email

# ── HKT ───────────────────────────────────────────────────────────────────────
HKT = timezone(timedelta(hours=8))


def _hkt_now() -> datetime:
    return datetime.now(HKT)


def _hkt_today() -> str:
    return _hkt_now().strftime("%Y-%m-%d")


# ── Bank registry ─────────────────────────────────────────────────────────────
BANKS: list[dict] = [
    {
        "id":   "za",
        "name": "ZA Bank",
        "urls": [
            "https://www.zabank.com.hk/en/promotions",
            "https://www.zabank.com.hk/promotions",
        ],
    },
    {
        "id":   "mox",
        "name": "Mox Bank",
        "urls": ["https://mox.com/promotions/"],
    },
    {
        "id":   "welab",
        "name": "WeLab Bank",
        "urls": ["https://www.welabbank.com/en/promotions/"],
    },
    {
        "id":   "livi",
        "name": "livi bank",
        "urls": ["https://www.livi.com.hk/en/promotions/"],
    },
    {
        "id":   "pao",
        "name": "PAObank",
        "urls": ["https://www.paobank.hk/en/promotions"],
    },
    {
        "id":   "airstar",
        "name": "Airstar Bank",
        "urls": ["https://www.airstarbank.com/en/promotions"],
    },
    {
        "id":   "fusion",
        "name": "Fusion Bank",
        "urls": ["https://www.fusionbank.com.hk/en/promotions"],
    },
    {
        "id":   "ant",
        "name": "Ant Bank",
        "urls": ["https://www.antbank.hk/en/promotions"],
    },
]

# ── Database ──────────────────────────────────────────────────────────────────

def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS promotions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            bank        TEXT    NOT NULL,
            bank_name   TEXT    NOT NULL,
            title       TEXT    NOT NULL,
            types       TEXT    DEFAULT '[]',
            is_bau      INTEGER DEFAULT 0,
            start_date  TEXT,
            end_date    TEXT,
            period      TEXT    DEFAULT 'Ongoing',
            highlight   TEXT    DEFAULT '',
            description TEXT    DEFAULT '',
            quota       TEXT    DEFAULT '',
            cost        TEXT    DEFAULT '',
            tc_link     TEXT    DEFAULT '',
            url         TEXT    DEFAULT '',
            active      INTEGER DEFAULT 1,
            created_at  TEXT    NOT NULL,
            last_seen   TEXT    NOT NULL
        )
        """
    )
    # Schema migrations — add columns that may be absent in older DBs
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(promotions)")}
    migrations = [
        ("highlight",   'TEXT DEFAULT ""'),
        ("description", 'TEXT DEFAULT ""'),
        ("quota",       'TEXT DEFAULT ""'),
        ("cost",        'TEXT DEFAULT ""'),
        ("tc_link",     'TEXT DEFAULT ""'),
        ("url",         'TEXT DEFAULT ""'),
        ("is_bau",      "INTEGER DEFAULT 0"),
        ("start_date",  "TEXT"),
        ("period",      'TEXT DEFAULT "Ongoing"'),
    ]
    for col, defn in migrations:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE promotions ADD COLUMN {col} {defn}")
    conn.commit()
    return conn


def db_upsert(conn: sqlite3.Connection, promo: dict, today: str) -> tuple[bool, bool]:
    """
    Insert or update one promotion.
    Returns (is_new, was_reactivated).
    """
    title     = (promo.get("title") or promo.get("name") or "").strip()
    bank_id   = promo.get("bank", "")
    bank_name = promo.get("bank_name") or promo.get("bName") or bank_id

    if not title or not bank_id:
        return False, False

    row = conn.execute(
        "SELECT id, active FROM promotions "
        "WHERE bank = ? AND title = ? COLLATE NOCASE",
        (bank_id, title),
    ).fetchone()

    types_json = json.dumps(promo.get("types") or ["Others"])

    fields = dict(
        types       = types_json,
        is_bau      = 1 if promo.get("is_bau") else 0,
        start_date  = promo.get("start_date"),
        end_date    = promo.get("end_date"),
        period      = promo.get("period") or "Ongoing",
        highlight   = promo.get("highlight") or "",
        description = promo.get("description") or "",
        quota       = promo.get("quota") or "",
        cost        = promo.get("cost") or "",
        tc_link     = promo.get("tc_link") or promo.get("url") or "",
        url         = promo.get("url") or promo.get("tc_link") or "",
        last_seen   = today,
    )

    if row:
        was_inactive = not row["active"]
        conn.execute(
            """
            UPDATE promotions SET
                types=:types, is_bau=:is_bau, start_date=:start_date,
                end_date=:end_date, period=:period, highlight=:highlight,
                description=:description, quota=:quota, cost=:cost,
                tc_link=:tc_link, url=:url, active=1, last_seen=:last_seen
            WHERE id = ?
            """,
            {**fields, "id": row["id"]},
        )
        conn.commit()
        return False, was_inactive

    conn.execute(
        """
        INSERT INTO promotions
            (bank, bank_name, title, types, is_bau, start_date, end_date,
             period, highlight, description, quota, cost, tc_link, url,
             active, created_at, last_seen)
        VALUES
            (:bank,:bank_name,:title,:types,:is_bau,:start_date,:end_date,
             :period,:highlight,:description,:quota,:cost,:tc_link,:url,
             1,:created_at,:last_seen)
        """,
        {
            **fields,
            "bank":       bank_id,
            "bank_name":  bank_name,
            "title":      title,
            "created_at": today,
        },
    )
    conn.commit()
    return True, False


def db_mark_unseen_inactive(conn: sqlite3.Connection, bank_id: str, today: str) -> None:
    """Promotions not seen in today's run → mark inactive."""
    conn.execute(
        "UPDATE promotions SET active = 0 "
        "WHERE bank = ? AND last_seen != ? AND active = 1",
        (bank_id, today),
    )
    conn.commit()


def db_fetch_all(conn: sqlite3.Connection) -> list[dict]:
    """All promotions (active + recently expired) for data.json."""
    rows = conn.execute(
        """
        SELECT * FROM promotions
        ORDER BY bank_name, is_bau, created_at DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def db_fetch_bank_active(conn: sqlite3.Connection, bank_id: str) -> list[dict]:
    """Active promotions for a specific bank — used for AI matching."""
    rows = conn.execute(
        "SELECT * FROM promotions WHERE bank = ? AND active = 1 "
        "ORDER BY created_at DESC",
        (bank_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def db_fetch_bank_by_name(conn: sqlite3.Connection, bank_name: str) -> list[dict]:
    """Active promotions by display name — used for AI supplement fallback."""
    rows = conn.execute(
        "SELECT * FROM promotions WHERE bank_name = ? AND active = 1 "
        "ORDER BY created_at DESC",
        (bank_name,),
    ).fetchall()
    return [dict(r) for r in rows]


# ── Scraping ──────────────────────────────────────────────────────────────────

_SCRAPE_WAIT_MS  = 5_000   # wait after page load
_SCROLL_WAIT_MS  = 2_000   # wait after scroll


def _scrape_url(page: Page, url: str) -> str:
    """Navigate to URL and return cleaned visible text."""
    try:
        page.goto(url, timeout=60_000, wait_until="domcontentloaded")
        page.wait_for_timeout(_SCRAPE_WAIT_MS)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(_SCROLL_WAIT_MS)
        text: str = page.evaluate(
            """() => {
                // Drop non-content elements
                ['nav','footer','script','style','noscript',
                 '.cookie-banner','#cookie-notice','.header',
                 '[aria-hidden="true"]'].forEach(sel => {
                    document.querySelectorAll(sel).forEach(el => el.remove());
                });
                return document.body.innerText || document.body.textContent || '';
            }"""
        )
        return (text or "").strip()
    except Exception as exc:
        print(f"    ⚠️  Failed to scrape {url}: {exc}")
        return ""


def scrape_bank(page: Page, bank: dict) -> str:
    """Scrape all registered URLs for a bank; return combined text."""
    parts: list[str] = []
    for url in bank["urls"]:
        print(f"    🌐 {url}")
        text = _scrape_url(page, url)
        if text:
            parts.append(f"[URL: {url}]\n{text}")
            print(f"       → {len(text):,} chars")
        else:
            print("       → empty / failed")
    return "\n\n".join(parts)


# ── Per-bank processing ───────────────────────────────────────────────────────

def process_bank(
    conn:       sqlite3.Connection,
    page:       Page,
    bank:       dict,
    today:      str,
    ai_enabled: bool,
) -> tuple[list[dict], int, int]:
    """
    Scrape → extract → dedup → match → upsert one bank.
    Returns (new_promo_list, new_count, reactivated_count).
    """
    bank_id   = bank["id"]
    bank_name = bank["name"]
    primary_url = bank["urls"][0]

    print(f"\n🏦  {bank_name}")

    raw_text = scrape_bank(page, bank)

    if not raw_text:
        print(f"  ⚠️  No text scraped — skipping AI extraction")
        db_mark_unseen_inactive(conn, bank_id, today)
        return [], 0, 0

    if not ai_enabled:
        print(f"  ℹ️  AI disabled — skipping extraction")
        db_mark_unseen_inactive(conn, bank_id, today)
        return [], 0, 0

    # 1. AI extraction
    ai_promos = ai_helper.analyze_promotions(
        bank_id     = bank_id,
        bank_name   = bank_name,
        text        = raw_text,
        default_url = primary_url,
    )
    if not ai_promos:
        print(f"  ℹ️  No promotions extracted")
        db_mark_unseen_inactive(conn, bank_id, today)
        return [], 0, 0

    # 2. Intra-run dedup (duplicates within this single scrape)
    if len(ai_promos) >= 2:
        titles  = [p.get("title") or p.get("name") or "" for p in ai_promos]
        dup_map = ai_helper.ai_dedup_titles(titles, bank_name)
        if dup_map:
            ai_promos = [p for i, p in enumerate(ai_promos) if i not in dup_map]
            print(f"  🧹 After intra-run dedup: {len(ai_promos)}")

    # 3. Match against existing DB entries
    existing = db_fetch_bank_active(conn, bank_id)
    match_map: dict[int, int] = (
        ai_helper.ai_match_against_existing(ai_promos, existing, bank_name)
        if existing
        else {}
    )

    # 4. Upsert
    new_promos: list[dict] = []
    new_count = react_count = 0

    for i, promo in enumerate(ai_promos):
        if i in match_map:
            db_id   = match_map[i]
            matched = next((r for r in existing if r["id"] == db_id), None)
            if matched:
                promo["title"] = matched["title"]   # preserve canonical title

        is_new, was_react = db_upsert(conn, promo, today)
        if is_new:
            new_count += 1
            new_promos.append(promo)
        if was_react:
            react_count += 1

    # 5. Retire anything not seen today
    db_mark_unseen_inactive(conn, bank_id, today)

    print(
        f"  ✅ {len(ai_promos)} extracted → "
        f"{new_count} new, {react_count} reactivated"
    )
    return new_promos, new_count, react_count


# ── data.json helpers ─────────────────────────────────────────────────────────

def _row_to_promo(r: dict) -> dict:
    try:
        types: list = json.loads(r.get("types") or "[]")
    except Exception:
        types = ["Others"]
    return {
        "id":          r["id"],
        "bank_name":   r["bank_name"],
        "title":       r["title"],
        "types":       types,
        "is_bau":      bool(r.get("is_bau")),
        "start_date":  r.get("start_date"),
        "end_date":    r.get("end_date"),
        "period":      r.get("period") or "Ongoing",
        "highlight":   r.get("highlight") or "",
        "description": r.get("description") or "",
        "quota":       r.get("quota") or "",
        "cost":        r.get("cost") or "",
        "tc_link":     r.get("tc_link") or r.get("url") or "",
        "url":         r.get("url") or r.get("tc_link") or "",
        "active":      bool(r.get("active")),
        "created_at":  r.get("created_at") or "",
        "last_seen":   r.get("last_seen") or "",
    }


def build_data_payload(conn: sqlite3.Connection) -> dict:
    now_hkt = _hkt_now().strftime("%Y-%m-%d %H:%M")
    promos  = [_row_to_promo(r) for r in db_fetch_all(conn)]
    return {
        "updated":      now_hkt,
        "last_updated": now_hkt,
        "promotions":   promos,
    }


def write_data_json(conn: sqlite3.Connection) -> dict:
    DATA_JSON.parent.mkdir(parents=True, exist_ok=True)
    data = build_data_payload(conn)
    with open(DATA_JSON, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    non_bau = sum(1 for p in data["promotions"] if not p["is_bau"])
    print(
        f"\n📄 Wrote {DATA_JSON.relative_to(ROOT_DIR)}: "
        f"{len(data['promotions'])} total ({non_bau} non-BAU)"
    )
    return data


# ── Strategic insights ────────────────────────────────────────────────────────

def _build_insights_input(conn: sqlite3.Connection) -> dict[str, list]:
    """Assemble promotions_by_bank dict for generate_strategic_insights."""
    result: dict[str, list] = {}
    for bank in BANKS:
        rows = db_fetch_bank_active(conn, bank["id"])
        if rows:
            result[bank["name"]] = [
                {
                    "name":        r.get("title", ""),
                    "title":       r.get("title", ""),
                    "types":       json.loads(r.get("types") or "[]"),
                    "is_bau":      bool(r.get("is_bau")),
                    "highlight":   r.get("highlight", ""),
                    "description": r.get("description", ""),
                    "period":      r.get("period", "Ongoing"),
                }
                for r in rows
            ]
    return result


def _db_supplement_fn(conn: sqlite3.Connection):
    """Factory that returns a callable for ai_helper.supplement_from_db."""
    def fetch(bank_name: str) -> list[dict]:
        rows = db_fetch_bank_by_name(conn, bank_name)
        result = []
        for r in rows:
            try:
                types = json.loads(r.get("types") or "[]")
            except Exception:
                types = ["Others"]
            result.append({
                "name":    r.get("title", ""),
                "title":   r.get("title", ""),
                "types":   types,
                "is_bau":  bool(r.get("is_bau")),
                "highlight": r.get("highlight", ""),
                "period":  r.get("period", "Ongoing"),
            })
        return result
    return fetch


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 65)
    print("🏦  VBank Tracker")
    print(f"    {_hkt_now().strftime('%Y-%m-%d %H:%M:%S')} HKT")
    print("=" * 65)

    today      = _hkt_today()
    ai_enabled = ai_helper.init_ai()

    conn = init_db(DB_PATH)

    all_new:  dict[str, list] = {}    # bank_name → list of new promos
    run_log:  list[str]       = []

    # ── Browser session ───────────────────────────────────────────────────────
    with sync_playwright() as pw:
        browser: Browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        ctx: BrowserContext = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-HK",
            timezone_id="Asia/Hong_Kong",
        )
        page: Page = ctx.new_page()

        for bank in BANKS:
            try:
                new_promos, new_cnt, react_cnt = process_bank(
                    conn, page, bank, today, ai_enabled
                )
                if new_promos:
                    all_new[bank["name"]] = new_promos
                flags = ("🆕 " if new_cnt else "") + ("🔄 " if react_cnt else "")
                run_log.append(
                    f"{flags}{bank['name']}: "
                    f"{new_cnt} new, {react_cnt} reactivated"
                )
            except Exception as exc:
                print(f"  ❌ Error processing {bank['name']}: {exc}")
                traceback.print_exc()
                run_log.append(f"❌ {bank['name']}: {exc}")

        browser.close()

    # ── Strategic insights ────────────────────────────────────────────────────
    insights = None
    if ai_enabled:
        try:
            pbb = _build_insights_input(conn)
            if pbb:
                insights = ai_helper.generate_strategic_insights(
                    promotions_by_bank = pbb,
                    db_fetch_fn        = _db_supplement_fn(conn),
                )
        except Exception as exc:
            print(f"  ⚠️  Strategic insights failed: {exc}")
            traceback.print_exc()

    # ── Write data.json ───────────────────────────────────────────────────────
    data = write_data_json(conn)

    # ── Email ─────────────────────────────────────────────────────────────────
    gmail_addr = os.environ.get("GMAIL_ADDRESS",      "").strip()
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
    recipient  = os.environ.get("RECIPIENT_EMAIL",    "").strip()

    if gmail_addr and gmail_pass and recipient:
        try:
            send_daily_email(
                sender_address  = gmail_addr,
                app_password    = gmail_pass,
                recipient       = recipient,
                all_promotions  = data["promotions"],
                new_promotions  = all_new,
                insights        = insights,
                run_date        = today,
            )
        except Exception as exc:
            print(f"  ⚠️  Email failed: {exc}")
            traceback.print_exc()
    else:
        print("  ℹ️  Email skipped (credentials not set)")

    # ── Summary ───────────────────────────────────────────────────────────────
    total_new = sum(len(v) for v in all_new.values())
    print("\n" + "=" * 65)
    print("📊  Run summary:")
    for line in run_log:
        print(f"    {line}")
    print(f"\n    Total new promotions this run: {total_new}")
    print("=" * 65)

    conn.close()


if __name__ == "__main__":
    main()