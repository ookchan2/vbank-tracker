#!/usr/bin/env python3
"""Quick diagnostic to compare website vs email counting methods."""

import sys, os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import load_promotions, get_active_promotions

def _hkt_today():
    from datetime import timezone, timedelta as td
    now = datetime.now(timezone(td(hours=8)))
    return now.date()

print("=" * 70)
print("WEBSITE vs EMAIL STATS COMPARISON")
print("=" * 70)

all_promos = load_promotions(active_only=False)
active_promos = [p for p in all_promos if p.get('active', False)]
non_bau_all = [p for p in all_promos if not p.get('is_bau', False)]
non_bau_active = [p for p in active_promos if not p.get('is_bau', False)]

today = _hkt_today()
threshold = today + timedelta(days=30)

print(f"\nALL PROMOTIONS (from DB):")
print(f"  Total: {len(all_promos)}")
print(f"  Active: {len(active_promos)}")
print(f"  Inactive/Expired: {len(all_promos) - len(active_promos)}")
print(f"  Non-BAU (all): {len(non_bau_all)}")
print(f"  Non-BAU (active): {len(non_bau_active)}")

print(f"\nWEBSITE COUNTING METHOD (export_to_json):")
website_active_non_bau = sum(1 for p in all_promos if p.get('active') and not p.get('is_bau'))
website_bau = sum(1 for p in all_promos if p.get('is_bau'))
website_expired = sum(1 for p in all_promos if not p.get('active'))
print(f"  Active non-BAU: {website_active_non_bau}")
print(f"  BAU: {website_bau}")
print(f"  Expired/inactive: {website_expired}")

print(f"\nEMAIL COUNTING METHOD (_classify_promo on ACTIVE non-BAU):")
past_count = 0
expiring_count = 0
active_count = 0

for p in non_bau_active:
    end_date = p.get('end_date')
    if end_date:
        try:
            end_d = datetime.strptime(str(end_date)[:10], '%Y-%m-%d').date()
            if end_d < today:
                past_count += 1
            elif end_d <= threshold:
                expiring_count += 1
            else:
                active_count += 1
        except:
            active_count += 1
    else:
        active_count += 1

print(f"  Past ended (end_date < today): {past_count}")
print(f"  Expiring soon (within 30 days): {expiring_count}")
print(f"  Truly active: {active_count}")
print(f"  Total shown (non-BAU - past): {len(non_bau_active) - past_count}")

print(f"\n[DIAGNOSTIC] DISCREPANCY EXPLANATION:")
print(f"  Website shows: {website_active_non_bau} active non-BAU promotions")
print(f"  Email shows: {active_count + expiring_count} promotions (classified by end_date)")
print(f"  Difference: The email uses end_date classification even for 'active' promos")
print(f"  Some promos marked 'active=True' may have end_date < today -> classified as 'past'")
print("=" * 70)
