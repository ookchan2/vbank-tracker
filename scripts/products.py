# scripts/products.py
#
# Product tracking module for HK virtual banks.
# Categories: US Stock, HK Stock, Investment Funds, Crypto Trading, Credit Card, Loans, Saving/Current Deposit, Time Deposit
#
# @author Claude

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ── Product Categories ─────────────────────────────────────────────────────────

PRODUCT_CATEGORIES = [
    "US Stock",
    "HK Stock",
    "Investment Funds",
    "Crypto Trading",
    "Credit Card",
    "Loans",
    "Saving/Current Deposit",
    "Time Deposit",
]

PRODUCT_CATEGORY_CHINESE = {
    "US Stock": "美股",
    "HK Stock": "港股",
    "Investment Funds": "基金",
    "Crypto Trading": "加密貨幣",
    "Credit Card": "信用卡",
    "Loans": "貸款",
    "Saving/Current Deposit": "存款",
    "Time Deposit": "定期存款",
}

# ── Product Dataclass ──────────────────────────────────────────────────────────

@dataclass
class RawProduct:
    product_name: str
    category: str
    bank_id: str = ''
    bank_name: str = ''
    description: str = ''
    highlight: str = ''
    features: List[str] = field(default_factory=list)
    fees: str = ''
    min_amount: str = ''
    currency: str = ''
    url: str = ''
    tc_link: str = ''

    def to_dict(self) -> dict:
        return {
            'product_name': self.product_name,
            'category': self.category,
            'bank_id': self.bank_id,
            'bank_name': self.bank_name,
            'description': self.description,
            'highlight': self.highlight,
            'features': self.features,
            'fees': self.fees,
            'min_amount': self.min_amount,
            'currency': self.currency,
            'url': self.url,
            'tc_link': self.tc_link,
        }


# ── Extraction Prompt ──────────────────────────────────────────────────────────

PRODUCT_EXTRACTION_PROMPT = """\
You are a specialist at extracting bank PRODUCT information from website text.

Bank: BANK_NAME_PLACEHOLDER
Source URL: URL_PLACEHOLDER

╔══════════════════════════════════════════════════════════════════════╗
║  PRODUCT EXTRACTION RULES                                           ║
║                                                                      ║
║  1. Extract ALL financial products offered by the bank.             ║
║     Products include: trading platforms, savings accounts,          ║
║     time deposits, credit cards, loans, investment funds, etc.      ║
║                                                                      ║
║  2. DO NOT extract promotions — only permanent products/services.   ║
║     Promotions have end dates; products are always available.       ║
║                                                                      ║
║  3. Categorize each product into ONE of these categories:           ║
║     • US Stock       — US stock trading platform                    ║
║     • HK Stock       — HK stock trading platform                    ║
║     • Investment Funds — Fund subscription/investment platform      ║
║     • Crypto Trading — Cryptocurrency/virtual asset trading         ║
║     • Credit Card    — Credit card products                         ║
║     • Loans          — Personal loans, tax loans, instalment loans  ║
║     • Saving/Current Deposit — Savings/current account products     ║
║     • Time Deposit   — Time/fixed deposit products                  ║
║                                                                      ║
║  4. For each product, provide:                                      ║
║     • product_name: Full English name of the product                ║
║     • category: One of the 8 categories above                      ║
║     • description: 3+ detailed sentences about the product          ║
║       Include: features, fees, eligibility, how to apply           ║
║     • highlight: One-line key benefit starting with an emoji        ║
║     • features: List of key features (strings)                      ║
║     • fees: Fee structure description                               ║
║     • min_amount: Minimum deposit/investment amount                 ║
║     • currency: Supported currencies (comma-separated)              ║
║                                                                      ║
║  5. ⛔ DO NOT EXTRACT:                                              ║
║     • Promotions with end dates                                     ║
║     • Scam alerts / security notices                                ║
║     • Service hour information                                      ║
║     • Generic bank information                                      ║
║                                                                      ║
║  6. If a bank offers both US and HK stock trading, extract as       ║
║     TWO separate products with different categories.                ║
╚══════════════════════════════════════════════════════════════════════╝

REQUIRED OUTPUT: A valid JSON array — NO other text, NO markdown fences.

Schema for each object:
{{
  "product_name": "Full English name of the product",
  "category":     "One of: US Stock, HK Stock, Investment Funds, Crypto Trading, Credit Card, Loans, Saving/Current Deposit, Time Deposit",
  "description":  "REQUIRED: 3+ detailed sentences describing the product features, fees, eligibility, and how to use it.",
  "highlight":    "One-line key benefit starting with an emoji",
  "features":     ["feature1", "feature2", "feature3"],
  "fees":         "Fee structure description",
  "min_amount":   "Minimum amount required",
  "currency":     "HKD, USD, etc."
}}

WEBSITE TEXT TO ANALYSE:
────────────────────────────────────────────────────────────────────────
TEXT_PLACEHOLDER
────────────────────────────────────────────────────────────────────────
Remember: return ONLY the JSON array starting with [ and ending with ].
If no products found, return []."""


# ── Product Name Synonyms ──────────────────────────────────────────────────────

PRODUCT_SYNONYMS = {
    # Savings products
    "gosave": "GoSave",
    "gosave 2.0": "GoSave",
    "gosave time deposit": "GoSave",
    "livisave": "liviSave",
    "livisave preferential": "liviSave",

    # Stock trading
    "us stock trading": "US Stock Trading",
    "us equity trading": "US Stock Trading",
    "hk stock trading": "HK Stock Trading",
    "hk equity trading": "HK Stock Trading",
    "stock trading platform": "Stock Trading",

    # Crypto
    "crypto trading": "Crypto Trading",
    "digital asset trading": "Crypto Trading",
    "virtual asset trading": "Crypto Trading",

    # Fund
    "fund investment": "Fund Investment",
    "fund platform": "Fund Investment",
    "investment fund": "Fund Investment",
    "mutual fund": "Fund Investment",

    # Time Deposit
    "time deposit": "Time Deposit",
    "fixed deposit": "Time Deposit",
    "定期存款": "Time Deposit",
}


def normalize_product_name(name: str) -> str:
    """Normalize product name to canonical form."""
    if not name:
        return name
    name_lower = name.lower().strip()
    return PRODUCT_SYNONYMS.get(name_lower, name.strip())


def get_product_category_keywords() -> Dict[str, List[str]]:
    """Return keywords for each product category."""
    return {
        "US Stock": [
            "us stock", "us equity", "us securities", "nyse", "nasdaq",
            "american stock", "us market", "fractional share",
        ],
        "HK Stock": [
            "hk stock", "hong kong stock", "hkex", "local stock",
            "hk securities", "hk shares", "hk brokerage",
        ],
        "Investment Funds": [
            "fund", "基金", "mutual fund", "unit trust", "fund subscription",
            "fund platform", "investment fund",
        ],
        "Crypto Trading": [
            "crypto", "bitcoin", "virtual asset", "digital asset",
            "cryptocurrency", "btc", "eth",
        ],
        "Credit Card": [
            "credit card", "visa", "mastercard", "card reward",
            "cashback card", "卡片", "信用卡",
        ],
        "Loans": [
            "loan", "personal loan", "instalment loan", "tax loan",
            "revolving loan", "mortgage", "貸款",
        ],
        "Saving/Current Deposit": [
            "savings account", "current account", "saving account",
            "deposit account", "multi-currency account",
            "存款", "儲蓄", "往來戶口",
        ],
        "Time Deposit": [
            "time deposit", "fixed deposit", "term deposit",
            "定期存款", "定存",
        ],
    }
