# scripts/main.py
# This is the MAIN file - run this to start everything

import sys
import os
from datetime import datetime

# Add parent folder to path so we can import ai_helper
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our modules
from ai_helper import extract_promotions, create_email_digest, test_ai_connection
from scripts.database import init_database, save_promotions, get_latest_promotions, log_email_sent, get_database_stats
from scripts.scraper import run_scraper
from scripts.emailer import send_email, send_test_email


def run_full_pipeline():
    """
    Run the complete pipeline:
    1. Scrape bank websites
    2. Use AI to extract promotions  
    3. Save to database
    4. Create email digest
    5. Send email
    """
    start_time = datetime.now()
    print("\n" + "🚀"*25)
    print("HK BANK PROMOTIONS BOT — STARTING")
    print(f"Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("🚀"*25 + "\n")
    
    # ─────────────────────────────────────
    # PHASE 1: Test AI connection
    # ─────────────────────────────────────
    print("\n📡 PHASE 1: Testing AI Connection")
    print("-"*40)
    if not test_ai_connection():
        print("❌ Cannot connect to AI. Check your OPENROUTER_API_KEY in .env")
        print("   Stopping here.")
        return False
    
    # ─────────────────────────────────────
    # PHASE 2: Initialize Database
    # ─────────────────────────────────────
    print("\n📦 PHASE 2: Setting Up Database")
    print("-"*40)
    init_database()
    
    # ─────────────────────────────────────
    # PHASE 3: Scrape Bank Websites
    # ─────────────────────────────────────
    print("\n🌐 PHASE 3: Scraping Bank Websites")
    print("-"*40)
    scraped_results = run_scraper()
    
    if not scraped_results:
        print("❌ Scraping returned no results!")
        return False
    
    # ─────────────────────────────────────
    # PHASE 4: AI Analysis
    # ─────────────────────────────────────
    print("\n🤖 PHASE 4: AI Analyzing Promotions")
    print("-"*40)
    
    ai_results = []
    success_count = 0
    
    for result in scraped_results:
        if result["success"] and result["raw_text"]:
            print(f"\n  Processing: {result['bank']}")
            
            # Ask AI to extract promotions
            ai_summary = extract_promotions(
                raw_text=result["raw_text"],
                bank_name=result["bank"]
            )
            
            # Save to database
            save_promotions(
                bank_name=result["bank"],
                bank_color=result["color"],
                raw_text=result["raw_text"],
                ai_summary=ai_summary
            )
            
            ai_results.append({
                "bank": result["bank"],
                "color": result["color"],
                "promotions": ai_summary
            })
            
            success_count += 1
        else:
            print(f"\n  ⚠️ Skipping {result['bank']} (scraping failed)")
    
    print(f"\n✅ AI analysis complete: {success_count} banks processed")
    
    if not ai_results:
        print("❌ No data to send in email!")
        return False
    
    # ─────────────────────────────────────
    # PHASE 5: Create Email Digest
    # ─────────────────────────────────────
    print("\n📝 PHASE 5: Creating Email Digest")
    print("-"*40)
    email_digest = create_email_digest(ai_results)
    
    # ─────────────────────────────────────
    # PHASE 6: Send Email
    # ─────────────────────────────────────
    print("\n📧 PHASE 6: Sending Email")
    print("-"*40)
    
    today = datetime.now().strftime("%d %B %Y")
    email_sent = send_email(
        subject=f"🏦 HK Bank Promotions Digest — {today}",
        text_content=email_digest,
        promotions_data=ai_results
    )
    
    # Log the result
    log_email_sent(
        recipient=os.getenv("RECIPIENT_EMAIL", "unknown"),
        status="success" if email_sent else "failed",
        num_banks=len(ai_results)
    )
    
    # ─────────────────────────────────────
    # FINAL SUMMARY
    # ─────────────────────────────────────
    end_time = datetime.now()
    duration = (end_time - start_time).seconds
    
    print("\n" + "="*50)
    print("🏁 PIPELINE COMPLETE")
    print("="*50)
    print(f"⏱️  Duration: {duration} seconds")
    print(f"🏦 Banks processed: {success_count}")
    print(f"📧 Email: {'✅ Sent!' if email_sent else '❌ Failed'}")
    
    stats = get_database_stats()
    print(f"📦 Total records in database: {stats['total_records']}")
    print(f"📨 Total emails sent: {stats['emails_sent']}")
    print("="*50 + "\n")
    
    return email_sent


def run_test_mode():
    """
    Test mode - just test AI and email, no scraping
    """
    print("\n🧪 RUNNING IN TEST MODE")
    print("="*50)
    
    # Test 1: AI Connection
    print("\n[Test 1] AI Connection...")
    ai_ok = test_ai_connection()
    
    # Test 2: Database
    print("\n[Test 2] Database...")
    init_database()
    print("✅ Database OK")
    
    # Test 3: Email
    print("\n[Test 3] Email...")
    email_ok = send_test_email()
    
    print("\n" + "="*50)
    print("🧪 TEST RESULTS:")
    print(f"  AI Connection: {'✅ Pass' if ai_ok else '❌ Fail'}")
    print(f"  Database: ✅ Pass")
    print(f"  Email: {'✅ Pass' if email_ok else '❌ Fail'}")
    print("="*50)


# ─────────────────────────────────────────────────
# MAIN ENTRY POINT
# Run different modes based on command line argument
# ─────────────────────────────────────────────────
if __name__ == "__main__":
    
    # Check if user passed a mode argument
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        if mode == "test":
            # Run: python scripts/main.py test
            run_test_mode()
            
        elif mode == "scrape":
            # Run: python scripts/main.py scrape  (no email)
            init_database()
            results = run_scraper()
            for r in results:
                print(f"\n{r['bank']}: {'✅' if r['success'] else '❌'}")
                
        else:
            print(f"❌ Unknown mode: {mode}")
            print("   Available modes: test, scrape")
            print("   Or run with no arguments for full pipeline")
    else:
        # Run full pipeline
        # Run: python scripts/main.py
        run_full_pipeline()