# scripts/scraper.py
import asyncio
from playwright.async_api import async_playwright

BANKS = [
    {
        "name":         "ZA Bank",
        "url":          "https://www.zabank.com/promotions",
        "fallback_url": "https://bank.za.group/en",
        "color":        "#FF0000"
    },
    {
        "name":         "Mox Bank",
        "url":          "https://mox.com/promotions",
        "fallback_url": "https://mox.com",
        "color":        "#FF5500"
    },
    {
        "name":         "livi bank",
        "url":          "https://livi.com.hk/en/promotions/",
        "fallback_url": "https://livi.com.hk/en/",
        "color":        "#7B2D8B"
    },
    {
        "name":         "WeLab Bank",
        "url":          "https://www.welab.co/en/promotions",
        "fallback_url": "https://www.welab.co/en/",
        "color":        "#FF6B35"
    },
    {
        "name":         "Ant Bank HK",
        "url":          "https://www.antbank.hk/en/promotions",
        "fallback_url": "https://www.antbank.hk/en/",
        "color":        "#1677FF"
    },
    {
        "name":         "PAObank",
        "url":          "https://www.paobank.hk/en/promotions",
        "fallback_url": "https://www.paobank.hk/",        # ← removed .com
        "color":        "#00A0DC"
    },
    {
        "name":         "Airstar Bank",
        "url":          "https://www.airstarbank.com/en/promotions",
        "fallback_url": "https://www.airstarbank.com/",
        "color":        "#00AEEF"
    },
    {
        "name":         "Fusion Bank",
        "url":          "https://www.fusionbank.hk/en/promotions",
        "fallback_url": "https://www.fusionbank.hk/",     # ← removed .com
        "color":        "#E31837"
    }
]


async def scrape_bank(page, bank):
    name         = bank["name"]
    url          = bank["url"]
    fallback_url = bank.get("fallback_url", "")

    print(f"  🌐 Opening {name} website...")

    async def try_url(target_url):
        await page.goto(
            target_url,
            wait_until="domcontentloaded",
            timeout=30000
        )
        text = (await page.inner_text("body")).strip()
        if len(text) < 50:
            raise Exception(f"Too little content ({len(text)} chars)")
        return text

    # Try primary URL
    try:
        text = await try_url(url)
        print(f"  ✅ {name} — got {len(text)} characters")
        return {
            "name":    name,
            "success": True,
            "text":    text,
            "url":     url,
            "color":   bank.get("color", "#333333")
        }
    except Exception as e:
        pass  # try fallback next

    # Try fallback URL
    if fallback_url and fallback_url != url:
        print(f"  🔄 Trying fallback URL for {name}...")
        try:
            text = await try_url(fallback_url)
            print(f"  ✅ {name} — got {len(text)} characters (fallback)")
            return {
                "name":    name,
                "success": True,
                "text":    text,
                "url":     fallback_url,
                "color":   bank.get("color", "#333333")
            }
        except Exception as e2:
            print(f"  ❌ {name} failed — {str(e2)[:100]}")
    else:
        print(f"  ❌ {name} failed — no fallback available")

    return {
        "name":    name,
        "success": False,
        "text":    "",
        "url":     url,
        "color":   bank.get("color", "#333333")
    }


async def scrape_all_banks():
    print("=" * 50)
    print("🏦 SCRAPING HK VIRTUAL BANKS")
    print("=" * 50)
    print()

    # ✅ KEY FIX: results is keyed by bank NAME (a string),
    #    NOT by the bank dict itself (which caused 'unhashable type: dict')
    results = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page    = await browser.new_page()

        await page.set_extra_http_headers({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })

        for i, bank in enumerate(BANKS, 1):
            name = bank["name"]   # ← always a plain string
            print(f"[{i}/{len(BANKS)}] Processing {name}...")

            result          = await scrape_bank(page, bank)
            results[name]   = result   # ✅ string key — no more 'unhashable' error

            print(f"  ⏳ Waiting 3 seconds...")
            await asyncio.sleep(3)

        await browser.close()

    successful = sum(1 for r in results.values() if r.get("success"))
    print()
    print("=" * 50)
    print(f"✅ Successful: {successful}/{len(BANKS)} banks")
    print("=" * 50)
    print()

    return results   # ← returns a proper dict, main.py's .items() will work fine