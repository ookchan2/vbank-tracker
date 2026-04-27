# Code Changes Summary

## ✅ Completed Changes

### 1. **Date Handling with Proper Timezone (Item 2)** ✓

**Files Modified:**
- `scripts/database.py`
- `scripts/emailer.py`

**Changes:**
- Replaced manual UTC+8 calculation with Python timezone-aware datetimes
- Added `_HKT = timezone(timedelta(hours=8))` constant
- Updated `_hkt_today()` and `_hkt_n_days_ago()` to use `datetime.now(_HKT)` instead of `datetime.utcnow() + timedelta(hours=8)`

**Benefits:**
- More robust date handling
- Follows Python best practices
- Easier to maintain and understand

---

### 2. **Email Stats Consistency Fix (Item 4)** ✓

**Status:** Already correct in code

**Verification:**
- Checked `main.py` lines 529-536 and 567-576
- Both `build_html_email()` and `send_email()` receive `scraped_data=data_json_content`
- Email stats now use data.json as canonical source (same as website)

**No changes needed** - implementation was already correct per your existing code.

---

### 3. **Improved Error Logging (Item 5)** ✓

**Files Modified:**
- `scripts/database.py`
- `scripts/ai_helper.py`
- `scripts/emailer.py`

**Changes:**
- Added `import logging` and `logger = logging.getLogger(__name__)` to all three files
- Enhanced SMTP error handling in `emailer.py`:
  - Distinguishes between authentication failures, recipient rejections, connection refused, and generic SMTP errors
  - Logs specific error types with appropriate log levels (ERROR vs WARNING)
  - Provides actionable error messages (e.g., "Check GMAIL_APP_PASSWORD" for auth failures)

- Improved AI extraction error handling in `ai_helper.py`:
  - Wrapped AI calls in try-except with proper logging
  - Logs empty results and retry attempts
  - Captures exception type and message for debugging

**Benefits:**
- Better debugging capabilities
- Structured logging for monitoring tools
- Clearer error messages for operators

---

### 4. **Screenshot Caching for Performance (Item 6)** ✓

**File Modified:**
- `scripts/scraper.py`

**Changes:**
- Added `_SCREENSHOT_CACHE_DIR` for disk-based cache storage
- Created `_cache_screenshot(url, data)` function to save screenshots to `.screenshot_cache/` directory
- Created `_load_cached_screenshot(url)` function to retrieve cached screenshots
- Modified `_try_url()` to check cache before taking new screenshot

**How it works:**
- Screenshots are hashed by URL (MD5) and stored as PNG files
- Subsequent scrapes of the same URL load from disk instead of browser
- Cache persists across runs, reducing bandwidth and scraping time

**Benefits:**
- Faster subsequent scrapes (no need to render pages again)
- Reduced memory usage (screenshots on disk, not in RAM)
- Can survive process restarts

---

### 5. **Migrated from Poe API to Anthropic SDK (Claude Code)** ✓

**Files Modified:**
- `scripts/ai_helper.py` (major rewrite)
- `requirements.txt`
- `.env`
- `.github/workflows/daily-update.yml`

**Key Changes:**

#### Before (Poe API):
```python
import fastapi_poe as fp
# Used POE_API_KEY and bot names like "Claude-3-7-Sonnet"
async for partial in fp.get_bot_response(...):
    response_text += partial.text
```

#### After (Anthropic SDK):
```python
import anthropic
# Uses ANTHROPIC_API_KEY and model ID "claude-3-7-sonnet-20250219"
response = _client.messages.create(
    model="claude-3-7-sonnet-20250219",
    max_tokens=4096,
    temperature=0.1,
    messages=[...]
)
result = response.content[0].text
```

**What changed:**
1. **Dependency:** `fastapi-poe` → `anthropic`
2. **Environment variable:** `POE_API_KEY` → `ANTHROPIC_API_KEY`
3. **Model selection:** Bot names → Model IDs (`claude-3-7-sonnet-20250219`)
4. **API call:** Async streaming → Synchronous direct call
5. **Initialization:** Test multiple models → Direct client initialization

**Setup Instructions:**
1. Install new dependency: `pip install anthropic`
2. Get your Anthropic API key from https://console.anthropic.com
3. Set environment variable: `export ANTHROPIC_API_KEY=sk-ant-...`
4. Update GitHub Secrets: Add `ANTHROPIC_API_KEY` secret

**Benefits:**
- Direct API access (no middleman)
- Latest Claude 3.7 Sonnet model
- Better error messages
- Official SDK support
- More control over parameters (temperature, max_tokens, etc.)

---

## ⚠️ Important Notes

### Bank Name Migration Question

You asked about running `migrate_bank_names.py` and whether it would cause data loss.

**Answer:** The migration script is **SAFE** and will NOT cause data loss. Here's what it does:

```python
RENAME_MAP = {
    "Airstar Bank":   "EleBank",  # Updates old name to new
    "PAObank":        "PADB",     # Updates old name to new
}
```

**What happens:**
- Existing promotions with `bank_name='Airstar Bank'` → become `bank_name='EleBank'`
- Existing promotions with `bank_name='PAObank'` → become `bank_name='PADB'`
- All promotion data, dates, and relationships are preserved
- Only the bank name string is updated

**To run safely:**
```bash
# Preview mode (no changes)
python scripts/migrate_bank_names.py --dry-run

# Apply changes
python scripts/migrate_bank_names.py
```

**Recommendation:** Run the migration, then the extensive backward-compatibility code in your system can be simplified in a future cleanup.

---

## 📋 Next Steps

1. **Install new dependency:**
   ```bash
   pip install anthropic
   ```

2. **Get Anthropic API key:**
   - Visit https://console.anthropic.com
   - Create/get your API key
   - Update `.env`: `ANTHROPIC_API_KEY=sk-ant-...`

3. **Update GitHub Secrets:**
   - Go to repo Settings → Secrets
   - Replace `POE_API_KEY` with `ANTHROPIC_API_KEY`

4. **Rotate exposed credentials:**
   - Your current POE API key and Gmail password are exposed in git history
   - Rotate both immediately
   - Consider using git filter-branch or BFG to remove from history

5. **Run bank name migration (optional but recommended):**
   ```bash
   python scripts/migrate_bank_names.py --dry-run  # Preview
   python scripts/migrate_bank_names.py             # Apply
   ```

6. **Test the changes:**
   ```bash
   cd scripts
   python main.py --skip-scrape  # Test with existing DB data
   ```

---

## 🔒 Security Reminder

Your `.env` file contains real credentials that are already committed to Git. Even though `.gitignore` excludes it, you should:
1. Rotate both credentials immediately
2. Consider using a tool like `git-filter-repo` to remove the file from git history
3. Never commit `.env` files to version control
