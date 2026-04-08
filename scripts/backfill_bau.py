#!/usr/bin/env python3
"""
One-time script: mark known BAU promotions in the DB using the ground-truth
list from promotions.xlsx.

Usage:
    python scripts/backfill_bau.py --dry-run
    python scripts/backfill_bau.py
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import _get_conn, _normalize_title

# ── Ground-truth BAU titles from promotions.xlsx (column B = "BAU") ──────────
# Key = bank_id, value = list of canonical BAU title substrings (normalised match)

BAU_BY_BANK: dict[str, list[str]] = {
    'za': [
        'Zero ZA Bank Fee SWIFT Transfers',
        'Zero-Fee Payment Connect',
        'No Annual or Hidden Fees on ZA Card',
        'Free Instant FPS Transfers',
        'Custom Card Number Selection',
        'Zero Fee ZA Card',
        'Free Cash Withdrawals at Visa ATMs',
        'Multi-Currency Savings Account',
        'Fast Loan Approval Service',
        'Quick Insurance Application',
        'Group Lai See Feature',
        'Express Wise Remittance',
    ],
    'airstar': [
        'Savings Deposit with No Minimum Balance',
        'Bank Securities Transfer Support',
        'Trade Hong Kong & US Stocks Instantly',
        'Quick Mobile Account Opening',
        'Personal Loan with Personalized Stellar APR',
        'Free Multi-Currency Transfers',
        'Low-Cost US Stock Trading',
        'Fractional Shares Investment',
        'High-Interest Savings Deposit',
        '24/7 Foreign Exchange Service',
        'Lifetime $0 Commissions on HK Stocks',
        'Flexible Time Deposit',
    ],
    'ant': [
        'Deposit Protection Scheme',
        'Time Deposit with Low Minimum Deposit',
        'Fund Investment with HK$1 Minimum',
        'Securities Trading with Zero Commission',
        'Insurance Products through YF Life',
        'Enhanced Fund Transfer Capabilities',
        'Savings Account with High Interest Rate',
        'Fraud Prevention Secure Storage Service',
        'Multi-Currency Savings Account Upgrade',
        'New Ant Bank Investment Fund Platform',
        'One-stop Trading Platform for Global Investment',
        'Installment Loan with Special Rates',
        'Personal Revolving Loan',
        'Insurance with 100% Premium Rebate',
        'Installment Loan with Fast Approval',
    ],
    'fusion': [
        'Personal Loan Credit Line',
        'First Digital Bank with Foreign Exchange Service in HK',
        'Instant Account Opening',
        'Deposit Protection Coverage',
        'Small Investments Starting from HKD 1',
    ],
    'mox': [
        'Instant Interest on Deposits',
        'Asia Miles Time Deposit',
        'FlexiBoost Savings Solution',
        'Unlimited CashBack with Mox Credit Card',
        'Instant Clear 0% Interest Credit Card Bill Payment',
        'Year-round Mox Credit Card Offers',
    ],
    'pao': [
        '24/7 Mobile Banking Services',
        'Services for Hong Kong Residents and Visitors',
        'Integrated Investment Trading Platform',
        'Quick Account Opening',
        'Extended US Stock Trading Hours',
        'Global Investment Access',
        'Digital Insurance Application',
        'Dual-Strength Investment Account',
        'Comprehensive Insurance Offerings',
    ],
    'welab': [
        'Zero Fee Investment Funds',
        'Transfer/FPS/Remittance Services',
        'Time Deposit',
        'Digital Wealth Advisory Service',
        'WeLab Money Plus Investment Platform',
        'Personal Instalment Loan',
        'Card Debt Consolidation Loan',
        'Policy Loan',
        'Money Safe Security Feature',
    ],
    'livi': [
        'liviSave Preferential Interest Rate',
        'Digital Account Opening',
        'livi QR Payment Service',
        'Fast Payment System (FPS) Transfers',
        'Fee-Free liviSave Account',
        '100% Online Account Opening',
        'Enhanced Account Security Features',
    ],
}


def backfill(dry_run: bool = True) -> int:
    conn = _get_conn()
    total = 0
    try:
        for bank_id, titles in BAU_BY_BANK.items():
            bau_norms = [_normalize_title(t) for t in titles]
            rows = conn.execute(
                "SELECT id, title FROM promotions WHERE bank_id = ?",
                (bank_id,)
            ).fetchall()

            to_mark = []
            for row in rows:
                norm = _normalize_title(row['title'])
                for bau_norm in bau_norms:
                    if bau_norm and (norm == bau_norm or bau_norm in norm or norm in bau_norm):
                        to_mark.append(row['id'])
                        tag = '[DRY RUN] ' if dry_run else ''
                        print(f"  {tag}[{bank_id}] BAU ← #{row['id']}  '{row['title']}'")
                        break

            if not dry_run and to_mark:
                conn.execute(
                    f"UPDATE promotions SET is_bau = 1 "
                    f"WHERE id IN ({','.join('?' * len(to_mark))})",
                    to_mark,
                )
                conn.commit()

            total += len(to_mark)

        tag = '[DRY RUN] ' if dry_run else ''
        print(f"\n  {tag}Marked {total} BAU promotion(s) total")
        return total

    except Exception as e:
        conn.rollback()
        print(f'  ❌ backfill error: {e}')
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    print(f'\n{"═"*60}')
    print('  BAU Backfill Utility')
    print(f'{"═"*60}\n')
    backfill(dry_run=args.dry_run)