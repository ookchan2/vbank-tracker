# scripts/ai_helper.py
import os
from anthropic import Anthropic

_client = None

def init_ai():
    global _client
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set in environment/secrets")
    _client = Anthropic(api_key=api_key)
    print("✅ Anthropic client ready")
    return _client

def analyze_promotions(bank_name, raw_text):
    """Use Claude to extract promotions from raw scraped text."""
    global _client
    if _client is None:
        init_ai()

    prompt = f"""You are analyzing the website content of {bank_name}, a Hong Kong virtual bank.

Extract all current promotions and offers from the content below.

For each promotion found, provide:
- Title: Short name of the promotion
- Details: Key numbers (interest rate %, cashback %, bonus amount)
- Conditions: Any requirements (min deposit, new customers only, etc.)
- Expiry: End date if mentioned

Website content:
{raw_text[:3000]}

If no specific promotions are found, briefly summarize what the page contains.
Focus on: savings rates, cashback, welcome bonuses, referral rewards."""

    message = _client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text

def create_digest(banks_data):
    """Create a formatted email digest from all analyzed banks."""
    global _client
    if _client is None:
        init_ai()

    parts = []
    for bank, data in banks_data.items():
        if data.get('analysis'):
            parts.append(f"=== {bank} ===\n{data['analysis']}\n")

    if not parts:
        return "No promotions data available today."

    all_data = "\n".join(parts)

    prompt = f"""Create a concise email digest of today's Hong Kong virtual bank promotions.

Source data:
{all_data[:4000]}

Format:
1. One-line intro with today's date
2. 🏆 TOP PICKS — highlight the 2-3 best deals
3. Quick summary per bank
4. Notable trends

Keep it scannable. Target audience: people choosing where to save money in HK."""

    message = _client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text