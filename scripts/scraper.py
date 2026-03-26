# scripts/scraper.py
# This file visits bank websites and collects promotion text

import asyncio
from playwright.async_api import async_playwright

# List of HK banks to scrape
BANKS = [
    {
        "name": "HSBC Hong Kong",
        "url": "https://www.hsbc.com.hk/credit-cards/offers/",
        "color": "#DB0011"
    },
    {
        "name": "Hang Seng Bank",
        "url": "https://bank.hangseng.com/1/2/cards/credit-cards/offers",
        "color": "#008B6E"
    },
    {
        "name": "Standard Chartered HK",
        "url": "https://www.sc.com/hk/credit-cards/offers/",
        "color": "#0080A1"
    },
    {
        "name": "Citibank HK",
        "url": "https://www.citibank.com.hk/english/credit-cards/promotions/",
        "color": "#003B70"
    },
    {
        "name": "Bank of China HK",
        "url": "https://www.bochk.com/en/more/creditcard/promotion.html",
        "color": "#CC0000"
    }
]


async def scrape_one_bank(playwright, bank: dict) -> dict:
    """
    Visit one bank website and get all the text
    """
    # Launch hidden browser
    browser = await playwright.chromium.launch(headless=True)
    
    # Set up browser to look like a real user
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 720}
    )
    
    page = await context.new_page()
    
    # Prepare result object
    result = {
        "bank": bank["name"],
        "color": bank["color"],
        "url": bank["url"],
        "raw_text": "",
        "success": False,
        "error": None
    }
    
    try:
        print(f"  🌐 Opening {bank['name']} website...")
        
        # Go to bank website (30 second timeout)
        await page.goto(
            bank["url"],
            timeout=30000,
            wait_until="domcontentloaded"
        )
        
        # Wait 3 seconds for page to fully load
        await page.wait_for_timeout(3000)
        
        # Extract all visible text from the page
        content = await page.evaluate("""
            () => {
                // Remove unnecessary elements
                const removeElements = document.querySelectorAll(
                    'script, style, nav, footer, header, .cookie-banner, .popup'
                );
                removeElements.forEach(el => el.remove());
                
                // Get all remaining text
                return document.body.innerText || document.body.textContent;
            }
        """)
        
        # Clean up the text
        lines = [line.strip() for line in content.split('\n') if line.strip()]
        clean_text = '\n'.join(lines)
        
        result["raw_text"] = clean_text[:5000]  # Keep first 5000 characters
        result["success"] = True
        
        char_count = len(clean_text)
        print(f"  ✅ {bank['name']} — got {char_count} characters of text")
        
    except Exception as e:
        result["error"] = str(e)
        result["success"] = False
        print(f"  ❌ {bank['name']} failed — {str(e)[:100]}")
    
    finally:
        await browser.close()
    
    return result


async def scrape_all_banks() -> list:
    """
    Scrape all banks one by one
    Returns list of results
    """
    print("\n" + "="*50)
    print("🏦 STARTING BANK WEBSITE SCRAPING")
    print("="*50)
    
    results = []
    
    async with async_playwright() as playwright:
        for i, bank in enumerate(BANKS, 1):
            print(f"\n[{i}/{len(BANKS)}] Processing {bank['name']}...")
            
            result = await scrape_one_bank(playwright, bank)
            results.append(result)
            
            # Wait 2 seconds between banks (be polite to servers)
            if i < len(BANKS):
                print(f"  ⏳ Waiting 2 seconds before next bank...")
                await asyncio.sleep(2)
    
    # Print summary
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful
    
    print(f"\n{'='*50}")
    print(f"📊 SCRAPING COMPLETE")
    print(f"✅ Successful: {successful}/{len(BANKS)} banks")
    if failed > 0:
        print(f"❌ Failed: {failed}/{len(BANKS)} banks")
    print("="*50)
    
    return results


def run_scraper() -> list:
    """
    Main function to run the scraper
    This is what other files will call
    """
    return asyncio.run(scrape_all_banks())


# Test - run this file directly to test scraping only
if __name__ == "__main__":
    print("🧪 Testing scraper...")
    results = run_scraper()
    
    print("\n📋 RESULTS PREVIEW:")
    for r in results:
        print(f"\n{'='*40}")
        print(f"Bank: {r['bank']}")
        print(f"Success: {r['success']}")
        if r['raw_text']:
            print(f"Preview: {r['raw_text'][:200]}...")
        if r['error']:
            print(f"Error: {r['error']}")