import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(
    api_key=os.getenv("POE_API_KEY"),
    base_url="https://api.poe.com/v1",
)

def analyze_promotion(promotion_text: str) -> str:
    try:
        response = client.chat.completions.create(
            model="claude-sonnet-4.6",
            messages=[
                {
                    "role": "user", 
                    "content": f"Analyze this HK bank promotion. List key benefits in 2-3 bullet points:\n\n{promotion_text}"
                }
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"AI analysis failed: {str(e)}"

def test_connection() -> bool:
    try:
        response = client.chat.completions.create(
            model="claude-sonnet-4.6",
            messages=[{"role": "user", "content": "Say OK only"}]
        )
        result = response.choices[0].message.content
        print(f"✅ Poe AI connected! Response: {result}")
        return True
    except Exception as e:
        print(f"❌ Poe connection failed: {e}")
        return False