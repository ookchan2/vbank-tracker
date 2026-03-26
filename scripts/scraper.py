# scripts/scraper.py
import asyncio
from playwright.async_api import async_playwright

# 只保留香港虛擬銀行
BANKS = [
    {
        "name": "ZA Bank",
        "url": "https://www.zabank.com/promotions",
        "color": "#FF0000"
    },
    {
        "name": "Mox Bank",
        "url": "https://mox.com/promotions",
        "color": "#FF69B4"
    },
    {
        "name": "livi bank",
        "url": "https://www.livi.com.hk/en/promotions",
        "color": "#6A0DAD"
    },
    {
        "name": "WeLab Bank",
        "url": "https://www.welab.bank/en/promotions",
        "color": "#FF4500"
    },
    {
        "name": "Ant Bank HK",
        "url": "https://www.antbank.hk/en/promotions",
        "color": "#1677FF"
    },
    {
        "name": "PAObank",
        "url": "https://www.paobank.hk/en/promotions",
        "color": "#00BFFF"
    },
    {
        "name": "Airstar Bank",
        "url": "https://www.airstarbank.com/en/promotions",
        "color": "#00CED1"
    },
    {
        "name": "Fusion Bank",
        "url": "https://www.fusionbank.com.hk/en/promotions",
        "color": "#FF8C00"
    }
]


async def scrape_one_bank(playwright, bank: dict) -> dict:
    browser = await playwright.chromium.launch(headless=True)
    
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 900}
    )
    
    page = await context.new_page()
    
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
        
        await page.goto(
            bank["url"],
            timeout=45000,          # 虛擬銀行網站較慢，45秒
            wait_until="networkidle" # 等到網絡完全空閒（適合 React/SPA）
        )
        
        # 額外等待 5 秒讓 JS 渲染完成
        await page.wait_for_timeout(5000)
        
        # 嘗試滾動頁面觸發 lazy load
        await page.evaluate("""
            () => {
                window.scrollTo(0, document.body.scrollHeight / 2);
            }
        """)
        await page.wait_for_timeout(2000)
        await page.evaluate("""
            () => {
                window.scrollTo(0, document.body.scrollHeight);
            }
        """)
        await page.wait_for_timeout(2000)
        
        # 提取文字
        content = await page.evaluate("""
            () => {
                const removeElements = document.querySelectorAll(
                    'script, style, nav, footer, header, .cookie-banner, .popup, .modal'
                );
                removeElements.forEach(el => el.remove());
                return document.body.innerText || document.body.textContent;
            }
        """)
        
        lines = [line.strip() for line in content.split('\n') if line.strip()]
        clean_text = '\n'.join(lines)
        
        # 虛擬銀行優惠資料較多，保留 8000 字
        result["raw_text"] = clean_text[:8000]
        result["success"] = True
        
        print(f"  ✅ {bank['name']} — got {len(clean_text)} characters")
        
    except Exception as e:
        result["error"] = str(e)
        result["success"] = False
        print(f"  ❌ {bank['name']} failed — {str(e)[:100]}")
    
    finally:
        await browser.close()
    
    return result


async def scrape_all_banks() -> list:
    print("\n" + "="*50)
    print("🏦 SCRAPING HK VIRTUAL BANKS")
    print("="*50)
    
    results = []
    
    async with async_playwright() as playwright:
        for i, bank in enumerate(BANKS, 1):
            print(f"\n[{i}/{len(BANKS)}] Processing {bank['name']}...")
            result = await scrape_one_bank(playwright, bank)
            results.append(result)
            
            if i < len(BANKS):
                print(f"  ⏳ Waiting 3 seconds...")
                await asyncio.sleep(3)
    
    successful = sum(1 for r in results if r["success"])
    print(f"\n{'='*50}")
    print(f"✅ Successful: {successful}/{len(BANKS)} banks")
    print("="*50)
    
    return results


def run_scraper() -> list:
    return asyncio.run(scrape_all_banks())


if __name__ == "__main__":
    results = run_scraper()
    for r in results:
        print(f"\n{r['bank']}: {'✅' if r['success'] else '❌'}")
        if r['raw_text']:
            print(f"Preview: {r['raw_text'][:200]}...")