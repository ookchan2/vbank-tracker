# scripts/ai_helper.py
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = None

def init_ai():
    global client
    client = OpenAI(
        api_key=os.getenv("POE_API_KEY"),
        base_url="https://api.poe.com/v1",
    )
    # Test connection
    response = client.chat.completions.create(
        model="claude-sonnet-4.6",
        messages=[{"role": "user", "content": "Say OK only"}]
    )
    print(f"✅ Poe AI connected!")

def analyze_promotions(bank_name: str, raw_text: str) -> str:
    try:
        response = client.chat.completions.create(
            model="claude-sonnet-4.6",
            messages=[{
                "role": "user",
                "content": f"""Analyze {bank_name} promotions below.
List top 3 key benefits in bullet points:

{raw_text[:2000]}"""
            }]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Analysis failed: {str(e)}"

def create_digest(analyzed_data: dict) -> str:
    try:
        summary_parts = []
        for bank, info in analyzed_data.items():
            summary_parts.append(
                f"{bank}:\n{info.get('analysis', 'No data')[:300]}"
            )
        all_text = "\n\n".join(summary_parts)

        response = client.chat.completions.create(
            model="claude-sonnet-4.6",
            messages=[{
                "role": "user",
                "content": f"""Create a short weekly digest summary 
of these HK bank promotions in 3-5 sentences:

{all_text[:3000]}"""
            }]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Digest failed: {str(e)}"

def test_connection() -> bool:
    try:
        init_ai()
        return True
    except Exception as e:
        print(f"❌ Poe connection failed: {e}")
        return False