# scripts/database.py
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'promotions.db')

def init_db():
    """Initialize database and create tables."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS promotions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_name   TEXT NOT NULL,
            title       TEXT,
            description TEXT,
            url         TEXT,
            scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            raw_text    TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS email_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            sent_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            recipient  TEXT,
            subject    TEXT,
            status     TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("✅ Database initialized")

def save_promotions(bank_name, analysis_text, raw_text=""):
    """Save analysis to database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        INSERT INTO promotions (bank_name, description, raw_text)
        VALUES (?, ?, ?)
    ''', (bank_name, str(analysis_text), raw_text))
    conn.commit()
    conn.close()

def get_total_records():
    """Return total promotion records."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM promotions')
        count = c.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0

def log_email(recipient, subject, status):
    """Log email attempts."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            INSERT INTO email_log (recipient, subject, status)
            VALUES (?, ?, ?)
        ''', (recipient, subject, status))
        conn.commit()
        conn.close()
    except Exception:
        pass

def get_total_emails_sent():
    """Return count of sent emails."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM email_log WHERE status='sent'")
        count = c.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0