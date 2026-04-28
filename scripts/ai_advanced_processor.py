#!/usr/bin/env python3
"""
Advanced AI Processor - Rule-Based Extraction with Quality Filtering
Mimics the quality of AI extraction without external API calls
"""

import re
import json
from datetime import datetime
from typing import List, Dict, Any, Tuple

# ── Non-bank content filtering (from ai_helper.py) ─────────────────────────────

_NON_BANK_CONTENT_PATTERNS = [
    'taipofire.gov.hk',
    'taxdeduction.html',
    'hab033',
    'cefs.gov.hk',
    'wang fuk court',
    'wangfuk',
    '宏福苑',
    'support fund for wang fuk',
    'support fund for',
    '大埔宏福苑',
    '援助基金',
    'tai po fire',
    'inland revenue',
    'ird.gov.hk',
    'gov.hk/taxdeduction',
    'tax deduction for donation',
    'tax deduction arrangement',
    'donation receipt',
    'donation acknowledgement',
    'letter of appreciation',
    'government sincerely thanks',
    '政府衷心感謝',
    '捐款致謝',
    '稅務扣除安排',
    'charity donation',
    'relief fund',
    'disaster relief',
    'taipofire',
    'fire support',
    'fire.gov',
    'approved charitable donation',
    'inland revenue ordinance',
    'bank of china (hong kong) account number 012-875',
]

_NON_BANK_DOMAINS = [
    'gov.hk',
    'ird.gov.hk',
    'taipofire.gov.hk',
    'taipofire',
    'cefs.gov.hk',
    'police.gov.hk',
    'welfare.gov.hk',
    'charities',
    'redcross',
]

# ── BAU Detection Patterns ───────────────────────────────────────────────────

BAU_PATTERNS = [
    r'account opening in \d+ minutes',
    r'quick account opening',
    r'mobile account opening',
    r'open account in minutes',
    r'24/7 mobile banking',
    r'24/7 digital banking',
    r'24×7 banking',
    r'free instant fps transfers',
    r'multi-currency savings account',
]

# ── Category Keywords ─────────────────────────────────────────────────────────

CATEGORY_KEYWORDS = {
    '迎新': ['welcome', 'new customer', 'sign up', 'open account', '迎新', '新客', '首次'],
    '消費': ['cashback', 'rebate', 'spending', 'purchase', '消費', '回贈', '簽賬', 'card'],
    '投資': ['investment', 'stock', 'fund', 'trading', 'securities', '投資', '股票', '基金', 'brokerage', 'commission'],
    '旅遊': ['travel', 'airline', 'hotel', 'flight', '旅遊', '航空', '酒店', 'trip.com'],
    '保險': ['insurance', 'protection', 'coverage', '保險', '保障'],
    '貸款': ['loan', 'borrow', 'mortgage', 'personal loan', '貸款', '按揭', '借貸', 'apr'],
    '存款': ['deposit', 'savings', 'interest rate', 'time deposit', '存款', '儲蓄', '定期'],
    '外匯': ['forex', 'foreign exchange', 'currency', 'fx', '外匯', '兌換', 'global wallet'],
    '推薦': ['referral', 'refer a friend', 'recommend', '推薦', '介紹朋友', 'invite'],
    '新資金': ['new funds', 'fresh funds', 'new money', '新資金', '新增資金']
}

# ── Main Extraction Functions ─────────────────────────────────────────────────

def extract_promotions_advanced(text: str, bank_id: str, bank_name: str) -> List[Dict[str, Any]]:
    """
    Advanced promotion extraction with quality filtering
    """
    promotions = []

    # Step 1: Clean the text
    cleaned_text = _clean_text(text)

    # Step 2: Identify promotion sections
    promo_sections = _identify_promotion_sections(cleaned_text)

    # Step 3: Extract individual promotions
    for section in promo_sections:
        promo = _extract_single_promotion(section, bank_id, bank_name)
        if promo and _validate_promotion(promo):
            promotions.append(promo)

    return promotions


def _clean_text(text: str) -> str:
    """Remove website boilerplate and navigation text"""

    # Remove SOURCE headers
    text = re.sub(r'=== SOURCE:.*?===', '', text)

    # Remove common navigation patterns
    nav_patterns = [
        r'Get \w+ TC',
        r'繁體中文',
        r'EN English',
        r'\w+\+ Travel Playbook',
        r'Features Arrow Created with Sketch',
        r'Go to',
        r'Click here',
        r'Learn more',
        r'Download app',
        r'Contact us',
        r'Follow us',
        r'Privacy Policy',
        r'Terms of Use',
        r'Cookie Policy',
        r'Accept',
        r'Decline',
        r'Close',
    ]

    for pattern in nav_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)

    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def _identify_promotion_sections(text: str) -> List[str]:
    """Split text into potential promotion sections"""

    # Try splitting by common delimiters
    delimiters = [
        r'\n\n+',  # Double newlines
        r'─+',     # Horizontal rules
        r'•',      # Bullet points
        r'\d+\.',  # Numbered lists
    ]

    sections = []

    for delim in delimiters:
        if re.search(delim, text):
            parts = re.split(delim, text)
            sections.extend([p.strip() for p in parts if len(p.strip()) > 100])
            break

    if not sections:
        sections = [text]

    return sections


def _extract_single_promotion(section: str, bank_id: str, bank_name: str) -> Dict[str, Any]:
    """Extract a single promotion from text section"""

    # Skip if non-bank content
    if _is_non_bank_content(section):
        return None

    # Extract title
    title = _extract_title(section)
    if not title:
        return None

    # Extract description
    description = _extract_description(section)

    # Extract dates
    start_date, end_date = _extract_dates(section)

    # Extract period
    period = _extract_period(section, start_date, end_date)

    # Categorize
    categories = _categorize(section)

    # Extract quota and cost
    quota = _extract_quota(section)
    cost = _extract_cost(section)

    # Extract links
    url, tc_link = _extract_links(section)

    # Determine if BAU
    is_bau = _is_bau_promotion(section, end_date)

    # Generate highlight
    highlight = _generate_highlight(description or title)

    return {
        'bank_id': bank_id,
        'bank_name': bank_name,
        'title': title,
        'highlight': highlight,
        'description': description,
        'types': categories,
        'start_date': start_date,
        'end_date': end_date,
        'period': period,
        'quota': quota,
        'cost': cost,
        'url': url,
        'tc_link': tc_link,
        'is_bau': is_bau
    }


def _is_non_bank_content(text: str) -> bool:
    """Check if content is from non-bank sources"""
    text_lower = text.lower()

    # Check for non-bank patterns
    for pattern in _NON_BANK_CONTENT_PATTERNS:
        if pattern in text_lower:
            return True

    # Check for non-bank domains
    for domain in _NON_BANK_DOMAINS:
        if domain in text_lower:
            return True

    return False


def _extract_title(text: str) -> str:
    """Extract promotion title - first meaningful line"""

    lines = text.split('\n')

    for line in lines[:10]:  # Check first 10 lines
        line = line.strip()

        # Skip short lines
        if len(line) < 15:
            continue

        # Skip if too long (likely description)
        if len(line) > 150:
            continue

        # Skip navigation text
        if any(word in line.lower() for word in ['get tc', 'english', 'created with', '繁體']):
            continue

        # This is likely a title
        return line[:150]

    # Fallback: use first sentence
    sentences = re.split(r'[.!?]', text)
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) > 15 and len(sentence) < 150:
            return sentence[:150]

    return ''


def _extract_description(text: str) -> str:
    """Extract detailed description"""

    # Remove title (first significant line)
    lines = text.split('\n')
    content_lines = []
    title_found = False

    for line in lines:
        line = line.strip()

        # Skip the title line
        if not title_found and len(line) > 15:
            title_found = True
            continue

        # Skip short lines and navigation
        if len(line) < 20:
            continue

        if any(word in line.lower() for word in ['get tc', 'english', 'created with', '繁體']):
            continue

        content_lines.append(line)

    content = ' '.join(content_lines[:5])  # Take first 5 substantial lines

    # Clean up
    content = re.sub(r'\s+', ' ', content).strip()

    # Limit length
    if len(content) > 500:
        # Try to cut at sentence boundary
        sentences = re.split(r'[.!?]', content[:500])
        if len(sentences) > 1:
            content = '. '.join(sentences[:-1]) + '.'
        else:
            content = content[:497] + '...'

    return content


def _extract_dates(text: str) -> Tuple[str, str]:
    """Extract start and end dates"""

    start_date = None
    end_date = None

    # Pattern 1: YYYY-MM-DD
    dates = re.findall(r'(\d{4}-\d{2}-\d{2})', text)
    if len(dates) >= 2:
        start_date = dates[0]
        end_date = dates[-1]
    elif len(dates) == 1:
        end_date = dates[0]

    # Pattern 2: DD/MM/YYYY
    if not end_date:
        dates = re.findall(r'(\d{1,2}/\d{1,2}/\d{4})', text)
        if len(dates) >= 2:
            start_date = _normalize_date(dates[0])
            end_date = _normalize_date(dates[-1])
        elif len(dates) == 1:
            end_date = _normalize_date(dates[0])

    # Pattern 3: "Until DD Month YYYY" or "until DD Month YYYY"
    if not end_date:
        match = re.search(r'until\s+(\d{1,2}\s+\w+\s+\d{4})', text, re.IGNORECASE)
        if match:
            end_date = _parse_month_date(match.group(1))

    # Pattern 4: "From DD Month YYYY to DD Month YYYY"
    if not start_date or not end_date:
        match = re.search(r'from\s+(\d{1,2}\s+\w+\s+\d{4})\s+to\s+(\d{1,2}\s+\w+\s+\d{4})', text, re.IGNORECASE)
        if match:
            start_date = _parse_month_date(match.group(1))
            end_date = _parse_month_date(match.group(2))

    return start_date, end_date


def _normalize_date(date_str: str) -> str:
    """Normalize date to YYYY-MM-DD format"""
    try:
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                day, month, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    except:
        pass
    return date_str


def _parse_month_date(date_str: str) -> str:
    """Parse 'DD Month YYYY' format"""
    months = {
        'january': '01', 'february': '02', 'march': '03', 'april': '04',
        'may': '05', 'june': '06', 'july': '07', 'august': '08',
        'september': '09', 'october': '10', 'november': '11', 'december': '12',
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'jun': '06', 'jul': '07', 'aug': '08', 'sep': '09',
        'oct': '10', 'nov': '11', 'dec': '12'
    }

    parts = date_str.split()
    if len(parts) == 3:
        day = parts[0].zfill(2)
        month_name = parts[1].lower()
        year = parts[2]

        month = months.get(month_name, '01')
        return f"{year}-{month}-{day}"

    return None


def _extract_period(text: str, start_date: str, end_date: str) -> str:
    """Extract promotional period description"""

    # Look for explicit period text
    patterns = [
        r'(?:period|valid)[:\s]+([^.]+\.)',
        r'(\d{1,2}\s+\w+\s+\d{4}\s+to\s+\d{1,2}\s+\w+\s+\d{4})',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

    # Construct from dates
    if start_date and end_date:
        return f"{start_date} to {end_date}"
    elif end_date:
        return f"Until {end_date}"

    return "Ongoing"


def _categorize(text: str) -> List[str]:
    """Categorize promotion using keywords"""

    text_lower = text.lower()
    scores = {}

    for category, keywords in CATEGORY_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in text_lower)
        if score > 0:
            scores[category] = score

    # Return top 3 categories
    if scores:
        sorted_cats = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [cat for cat, score in sorted_cats[:3]]

    return ['Others']


def _extract_quota(text: str) -> str:
    """Extract quota/eligibility information"""

    patterns = [
        r'(?:quota|eligibility|available to|eligible)[:\s]+([^.]{10,150})',
        r'(first\s+\d+\s+(?:customers|applicants))',
        r'(limited\s+(?:to|quota)[:\s]+[^.]{10,100})',
        r'(new customers? only)',
        r'(existing customers? only)',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            quota = match.group(1).strip()
            # Clean up
            quota = re.sub(r'\s+', ' ', quota)
            return quota[:200]

    return ''


def _extract_cost(text: str) -> str:
    """Extract cost/minimum spend requirements"""

    patterns = [
        r'(?:minimum|min\.?\s+(?:spend|purchase|deposit))[:\s]+([^.]{10,150})',
        r'(spend\s+HKD\s+[\d,]+)',
        r'(minimum\s+HKD\s+[\d,]+)',
        r'(min\s+spend[:\s]+[^.]{10,100})',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            cost = match.group(1).strip()
            cost = re.sub(r'\s+', ' ', cost)
            return cost[:200]

    return ''


def _extract_links(text: str) -> Tuple[str, str]:
    """Extract URLs from text"""

    urls = re.findall(r'https?://[^\s<>"{}|\\^`\[\]]+', text)

    url = urls[0] if urls else ''
    tc_link = urls[1] if len(urls) > 1 else ''

    return url, tc_link


def _is_bau_promotion(text: str, end_date: str) -> bool:
    """Determine if this is a BAU (ongoing) promotion"""

    # Check BAU patterns
    text_lower = text.lower()
    for pattern in BAU_PATTERNS:
        if re.search(pattern, text_lower):
            return True

    # No end date suggests BAU
    if not end_date:
        return True

    return False


def _generate_highlight(description: str) -> str:
    """Generate brief highlight from description"""

    # Take first sentence
    sentences = re.split(r'[.!?]', description)
    if sentences:
        highlight = sentences[0].strip()
        if len(highlight) > 150:
            highlight = highlight[:147] + '...'
        return highlight

    return description[:150]


def _validate_promotion(promo: Dict[str, Any]) -> bool:
    """Validate promotion has minimum required fields"""

    # Must have title
    if not promo.get('title') or len(promo['title']) < 10:
        return False

    # Must have bank info
    if not promo.get('bank_id') or not promo.get('bank_name'):
        return False

    # Should have some description
    if not promo.get('description') or len(promo.get('description', '')) < 20:
        return False

    # Check for non-bank content
    title = promo.get('title', '').lower()
    desc = promo.get('description', '').lower()

    for pattern in _NON_BANK_CONTENT_PATTERNS:
        if pattern in title or pattern in desc:
            return False

    return True


# ── Product Extraction ───────────────────────────────────────────────────────

def extract_products_advanced(text: str, bank_id: str, bank_name: str) -> List[Dict[str, Any]]:
    """Extract banking products"""

    products = []

    product_keywords = {
        'card': ['credit card', 'debit card', 'visa', 'mastercard'],
        'deposit': ['savings account', 'current account', 'deposit account', 'time deposit'],
        'loan': ['personal loan', 'mortgage', 'overdraft'],
        'investment': ['investment account', 'securities account', 'trading account', 'stock trading']
    }

    text_lower = text.lower()

    for category, keywords in product_keywords.items():
        if any(keyword in text_lower for keyword in keywords):
            product = {
                'bank_id': bank_id,
                'bank_name': bank_name,
                'product_name': f'{bank_name} {category.title()} Product',
                'category': category,
                'subcategory': '',
                'description': f'{category.title()} product from {bank_name}',
                'features': [],
                'interest_rate': '',
                'fees': '',
                'eligibility': '',
                'url': '',
                'first_seen_at': datetime.now().strftime('%Y-%m-%d')
            }
            products.append(product)

    return products


# Export
__all__ = [
    'extract_promotions_advanced',
    'extract_products_advanced'
]
