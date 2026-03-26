# scripts/database.py
# This file handles saving and loading data from local database

import sqlite3
import os
from datetime import datetime

# Database file location
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "promotions.db")


def init_database():
    """
    Create database tables if they don't exist
    Run this once when program starts
    """
    print("📦 Initializing database...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create main promotions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS promotions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_name TEXT NOT NULL,
            bank_color TEXT DEFAULT '#333333',
            raw_text TEXT,
            ai_summary TEXT,
            scraped_date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create email log table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            recipient TEXT,
            status TEXT,
            num_banks INTEGER
        )
    """)
    
    conn.commit()
    conn.close()
    print("✅ Database ready!")


def save_promotions(bank_name: str, bank_color: str, raw_text: str, ai_summary: str):
    """
    Save one bank's promotion data to database
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    cursor.execute("""
        INSERT INTO promotions (bank_name, bank_color, raw_text, ai_summary, scraped_date)
        VALUES (?, ?, ?, ?, ?)
    """, (bank_name, bank_color, raw_text, ai_summary, today))
    
    conn.commit()
    conn.close()
    print(f"  💾 Saved {bank_name} promotions to database")


def get_latest_promotions() -> list:
    """
    Get today's promotions from database
    Returns list of dictionaries
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    cursor.execute("""
        SELECT bank_name, bank_color, ai_summary, scraped_date 
        FROM promotions 
        WHERE scraped_date = ?
        ORDER BY id DESC
    """, (today,))
    
    rows = cursor.fetchall()
    conn.close()
    
    results = []
    for row in rows:
        results.append({
            "bank": row[0],
            "color": row[1],
            "promotions": row[2],
            "date": row[3]
        })
    
    return results


def log_email_sent(recipient: str, status: str, num_banks: int):
    """
    Log when an email was sent
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO email_log (recipient, status, num_banks)
        VALUES (?, ?, ?)
    """, (recipient, status, num_banks))
    
    conn.commit()
    conn.close()


def get_database_stats() -> dict:
    """
    Get some stats about the database
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM promotions")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM email_log WHERE status = 'success'")
    emails_sent = cursor.fetchone()[0]
    
    cursor.execute("SELECT MAX(scraped_date) FROM promotions")
    last_scrape = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        "total_records": total_records,
        "emails_sent": emails_sent,
        "last_scrape": last_scrape
    }