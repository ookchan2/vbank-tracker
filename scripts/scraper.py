# scripts/scraper.py
import asyncio
from playwright.async_api import async_playwright

BANKS = [
    {
        "name": "ZA Bank",
        "url": "https://www.zabank.com/promotions",
        "fallback_url": "https://zabank.com/promotions",
        "color": "#FF0000"
    },
    {
        "name": "Mox Bank",
        "url": "https://mox.com/promotions",
        "fallback_url": "https://www.mox.com/promotions",
        "color": "#FF69B4"
    },
    {
        "name": "livi bank",
        "url": "https://livi.com.hk/en/promotions",
        "fallback_url": "https://www.livi.com.hk/en/promotions",
        "color": "#6A0DAD"
    },
    {
        "name": "WeLab Bank",
        "url": "https://www.welab.bank/en/promotions",
        "fallback_url": "https://welab.bank/en/promotions",
        "color": "#FF4500"
    },
    {
        "name": "Ant Bank HK",
        "url": "https://www.antbank.hk/en/promotions",
        "fallback_url": "https://antbank.hk/en/promotions",
        "color": "#1677FF"
    },
    {
        "name": "PAObank",
        "url": "https://www.paobank.hk/en/promotions",
        "fallback_url": "https://paobank.hk/en/promotions",
        "color": "#00B0F0"
    },
    {
        "name": "Airstar Bank",
        "url": "https://www.airstarbank.com/en/promotions",
        "fallback_url": "https://airstarbank.com/en/promotions",
        "color": "#00BFFF"
    },
    {
        "name": "Fusion Bank",
        "url": "https://www.fusionbank.com.hk/en/promotions",
        "fallback_url": "https://fusionbank.com.hk/en/promotions",
        "color": "#FF8C00"
    },
]

async def scrape_bank(page, bank):
    name = bank["name"]
    url = bank["url"]
    fallback_url = bank.get("fallback_url", "")

    async def try_scrape(target_url):
        await page.goto(target_url, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except:
            pass
        content = await page.inner_text("body")
        content = content.strip()
        if len(content) < 100:
            content = await page.content()
        return content

    try:
        print(f"  🌐 Opening {name} website...")
        content = await try_scrape(url)
        print(f"  ✅ {name} — got {len(content)} characters")
        return {"bank": bank, "content": content, "success": True}

    except Exception as e:
        if fallback_url:
            try:
                print(f"  🔄 Trying fallback URL for {name}...")
                content = await try_scrape(fallback_url)
                print(f"  ✅ {name} (fallback) — got {len(content)} characters")
                return {"bank": bank, "content": content, "success": True}
            except Exception as e2:
                print(f"  ❌ {name} failed — {str(e2)[:100]}")
        else:
            print(f"  ❌ {name} failed — {str(e)[:100]}")

        return {"bank": bank, "content": "", "success": False}


async def scrape_all_banks():
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage"
            ]
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )

        page = await context.new_page()

        print("\n" + "="*50)
        print("🏦 SCRAPING HK VIRTUAL BANKS")
        print("="*50 + "\n")

        for i, bank in enumerate(BANKS, 1):
            print(f"[{i}/{len(BANKS)}] Processing {bank['name']}...")
            result = await scrape_bank(page, bank)
            results.append(result)
            print(f"  ⏳ Waiting 3 seconds...")
            await asyncio.sleep(3)

        await browser.close()

    successful = sum(1 for r in results if r["success"])
    print(f"\n{'='*50}")
    print(f"✅ Successful: {successful}/{len(BANKS)} banks")
    print("="*50 + "\n")

    return results