#!/usr/bin/env python3
"""
Advanced AI Processor - Sophisticated Rule-Based Extraction
Mimics the quality of AI extraction through careful pattern matching and validation
"""

import re
import json
from datetime import datetime
from typing import List, Dict, Any, Tuple

# ── Non-bank content filtering ─────────────────────────────────────────────────

NON_BANK_PATTERNS = [
    'taipofire.gov.hk', 'taxdeduction.html', 'hab033', 'cefs.gov.hk',
    'wang fuk court', 'wangfuk', '宏福苑', 'support fund for wang fuk',
    '大埔宏福苑', '援助基金', 'tai po fire', 'inland revenue', 'ird.gov.hk',
    'gov.hk/taxdeduction', 'tax deduction for donation', 'donation receipt',
    'donation acknowledgement', 'letter of appreciation', '政府衷心感謝',
    '捐款致謝', '稅務扣除安排', 'charity donation', 'relief fund',
    'disaster relief', 'taipofire', 'fire support', 'fire.gov',
    'approved charitable donation', 'inland revenue ordinance',
    'bank of china (hong kong) account number 012-875',
]

NON_BANK_DOMAINS = [
    'gov.hk', 'ird.gov.hk', 'taipofire.gov.hk', 'taipofire',
    'cefs.gov.hk', 'police.gov.hk', 'welfare.gov.hk', 'charities', 'redcross',
]

# ── BAU Detection Patterns (from ai_helper.py) ─────────────────────────────────

BAU_PATTERNS = [
    r'account opening in \d+ minutes',
    r'quick account opening',
    r'mobile account opening in \d+ minutes',
    r'open account in minutes',
    r'open an account in minutes',
    r'sign up in the time it takes',
    r'open account in the time',
    r'24/7 mobile banking',
    r'24/7 digital banking',
    r'24×7 banking',
    r'free instant fps transfers',
    r'multi-currency savings account',
    r'new crypto customer fee waiver',
]

# ── Category Keywords (from ai_helper.py) ───────────────────────────────────────

CATEGORY_KEYWORDS = {
    '迎新': ['welcome', 'new customer', 'sign up', 'open account', '迎新', '新客', '首次', 'account opening'],
    '消費': ['cashback', 'rebate', 'spending', 'purchase', '消費', '回贈', '簽賬', 'card spending', 'merchant'],
    '投資': ['investment', 'stock', 'fund', 'trading', 'securities', '投資', '股票', '基金', 'brokerage', 'commission', 'ipo', 'equities'],
    '旅遊': ['travel', 'airline', 'hotel', 'flight', '旅遊', '航空', '酒店', 'trip.com', 'asia miles', 'lounge'],
    '保險': ['insurance', 'protection', 'coverage', '保險', '保障'],
    '貸款': ['loan', 'borrow', 'mortgage', 'personal loan', '貸款', '按揭', '借貸', 'apr'],
    '存款': ['deposit', 'savings', 'interest rate', 'time deposit', '存款', '儲蓄', '定期', 'hkd'],
    '外匯': ['forex', 'foreign exchange', 'currency', 'fx', '外匯', '兌換', 'global wallet', 'remittance'],
    '推薦': ['referral', 'refer a friend', 'recommend', '推薦', '介紹朋友', 'invite', '多友多賞'],
    '新資金': ['new funds', 'fresh funds', 'new money', '新資金', '新增資金']
}

# ── Navigation/Boilerplate Patterns ───────────────────────────────────────────

NAVIGATION_PATTERNS = [
    r'^\d+\s+\d+\s+\d+\s+\d+\s+\d+',  # "1 2 3 4 5"
    r'previous\s+next',
    r'get\s+\w+\s+tc',
    r'^繁體中文$',
    r'^en\s+english$',
    r'features\s+arrow\s+created\s+with\s+sketch',
    r'^go to$',
    r'^click here$',
    r'^learn more$',
    r'^download app$',
    r'^contact us$',
    r'^follow us$',
    r'^privacy policy$',
    r'^terms of use$',
    r'^cookie policy$',
    r'this website uses cookies',
    r'^accept$',
    r'^decline$',
    r'^close$',
    r'^\d+\s*$',  # Just a number
]

# ── Main Extraction Functions ─────────────────────────────────────────────────

def extract_promotions_advanced(text: str, bank_id: str, bank_name: str) -> List[Dict[str, Any]]:
    """
    Advanced promotion extraction with multi-stage processing
    """
    promotions = []

    # Stage 1: Split into sections by SOURCE markers
    sections = _split_by_sources(text)

    # Stage 2: Process each section
    for section_url, section_text in sections:
        # Skip non-bank content
        if _is_non_bank_content(section_text, section_url):
            continue

        # Extract promotion
        promo = _extract_single_promotion(section_text, bank_id, bank_name, section_url)
        if promo and _validate_promotion(promo):
            promotions.append(promo)

    return promotions


def _split_by_sources(text: str) -> List[Tuple[str, str]]:
    """Split text by SOURCE markers, returning (url, content) pairs"""

    sections = []
    current_url = ''
    current_text = []

    lines = text.split('\n')

    for line in lines:
        # Check if this is a SOURCE marker
        source_match = re.match(r'=== SOURCE:\s*(https?://[^\s=]+)\s*===', line)

        if source_match:
            # Save previous section
            if current_url and current_text:
                content = '\n'.join(current_text).strip()
                if content:
                    sections.append((current_url, content))

            # Start new section
            current_url = source_match.group(1)
            current_text = []
        else:
            # Add to current section
            current_text.append(line)

    # Don't forget the last section
    if current_url and current_text:
        content = '\n'.join(current_text).strip()
        if content:
            sections.append((current_url, content))

    # If no SOURCE markers found, treat entire text as one section
    if not sections:
        sections.append(('', text.strip()))

    return sections


def _is_non_bank_content(text: str, url: str) -> bool:
    """Check if content is from non-bank sources"""

    text_lower = text.lower()
    url_lower = url.lower()

    # Check URL for non-bank domains
    for domain in NON_BANK_DOMAINS:
        if domain in url_lower:
            return True

    # Check content for non-bank patterns
    for pattern in NON_BANK_PATTERNS:
        if pattern in text_lower:
            return True

    return False


def _extract_single_promotion(text: str, bank_id: str, bank_name: str, url: str) -> Dict[str, Any]:
    """Extract a single promotion from cleaned text"""

    # Clean the text first
    cleaned_text = _clean_promotion_text(text)

    # Skip if too short after cleaning
    if len(cleaned_text) < 50:
        return None

    # Extract title
    title = _extract_title(cleaned_text)
    if not title:
        return None

    # Extract description
    description = _extract_description(cleaned_text, title)

    # Extract dates
    start_date, end_date = _extract_dates(cleaned_text)

    # Extract period
    period = _extract_period(cleaned_text, start_date, end_date)

    # Categorize
    categories = _categorize(cleaned_text)

    # Extract quota and cost
    quota = _extract_quota(cleaned_text)
    cost = _extract_cost(cleaned_text)

    # Extract links
    tc_link = _extract_tc_link(cleaned_text, url)

    # Determine if BAU
    is_bau = _is_bau_promotion(cleaned_text, end_date)

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


def _clean_promotion_text(text: str) -> str:
    """Remove navigation and boilerplate text"""

    lines = text.split('\n')
    cleaned_lines = []

    for line in lines:
        line = line.strip()

        # Skip empty lines
        if not line:
            continue

        # Skip navigation patterns
        skip = False
        for pattern in NAVIGATION_PATTERNS:
            if re.search(pattern, line, re.IGNORECASE):
                skip = True
                break

        if skip:
            continue

        cleaned_lines.append(line)

    return ' '.join(cleaned_lines)


def _extract_title(text: str) -> str:
    """Extract promotion title - look for headline-style content"""

    # Split into sentences
    sentences = re.split(r'[.!?]', text)

    for sentence in sentences:
        sentence = sentence.strip()

        # Skip too short or too long
        if len(sentence) < 15 or len(sentence) > 200:
            continue

        # Skip if contains navigation keywords
        if any(nav in sentence.lower() for nav in ['previous', 'next', 'cookie', 'privacy', 'terms']):
            continue

        # Skip if mostly numbers (like "1 2 3 4 5")
        if len(re.findall(r'\d', sentence)) > len(sentence) * 0.3:
            continue

        # This looks like a title
        return sentence

    # Fallback: use first 100 chars
    return text[:100].strip() if text else ''


def _extract_description(text: str, title: str) -> str:
    """Extract detailed description"""

    # Remove the title from text
    if title and title in text:
        text = text.replace(title, '', 1).strip()

    # Take first 500 chars
    desc = text[:500]

    # Try to cut at sentence boundary
    sentences = re.split(r'[.!?]', desc)
    if len(sentences) > 1:
        desc = '. '.join(sentences[:-1]) + '.'

    return desc.strip()


def _extract_dates(text: str) -> Tuple[str, str]:
    """Extract start and end dates with multiple patterns"""

    start_date = None
    end_date = None

    # Pattern 1: YYYY-MM-DD
    dates = re.findall(r'\b(\d{4}-\d{2}-\d{2})\b', text)
    if len(dates) >= 2:
        start_date = dates[0]
        end_date = dates[-1]
    elif len(dates) == 1:
        end_date = dates[0]

    # Pattern 2: DD/MM/YYYY
    if not end_date:
        dates = re.findall(r'\b(\d{1,2}/\d{1,2}/\d{4})\b', text)
        if len(dates) >= 2:
            start_date = _normalize_date(dates[0])
            end_date = _normalize_date(dates[-1])
        elif len(dates) == 1:
            end_date = _normalize_date(dates[0])

    # Pattern 3: "until DD Month YYYY"
    if not end_date:
        match = re.search(r'\buntil\s+(\d{1,2}\s+\w+\s+\d{4})\b', text, re.IGNORECASE)
        if match:
            end_date = _parse_month_date(match.group(1))

    # Pattern 4: "from DD Month YYYY to DD Month YYYY"
    if not start_date or not end_date:
        match = re.search(
            r'\bfrom\s+(\d{1,2}\s+\w+\s+\d{4})\s+to\s+(\d{1,2}\s+\w+\s+\d{4})\b',
            text,
            re.IGNORECASE
        )
        if match:
            start_date = _parse_month_date(match.group(1))
            end_date = _parse_month_date(match.group(2))

    # Pattern 5: "until 31 December 2026" etc.
    if not end_date:
        months = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)'
        match = re.search(
            rf'\buntil\s+(\d{{1,2}}\s+{months}\s+\d{{4}})\b',
            text,
            re.IGNORECASE
        )
        if match:
            end_date = _parse_month_date(match.group(1))

    return start_date, end_date


def _normalize_date(date_str: str) -> str:
    """Normalize DD/MM/YYYY to YYYY-MM-DD"""
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

        month = months.get(month_name)
        if month:
            return f"{year}-{month}-{day}"

    return None


def _extract_period(text: str, start_date: str, end_date: str) -> str:
    """Extract promotional period description"""

    # Look for explicit period mentions
    patterns = [
        r'(?:period|valid)[:\s]+([^.]{10,100})',
        r'(?:from|until)\s+[^.]{10,100}',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            period = match.group(0).strip()
            # Clean up
            period = re.sub(r'\s+', ' ', period)
            return period[:200]

    # Construct from dates
    if start_date and end_date:
        return f"{start_date} to {end_date}"
    elif end_date:
        return f"Until {end_date}"

    return "Ongoing"


def _categorize(text: str) -> List[str]:
    """Categorize promotion using keyword matching"""

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
            quota = re.sub(r'\s+', ' ', quota)
            return quota[:200]

    return ''


def _extract_cost(text: str) -> str:
    """Extract cost/minimum spend requirements"""

    patterns = [
        r'(?:minimum|min\.?\s+(?:spend|purchase|deposit))[:\s]+([^.]{10,150})',
        r'(spend\s+hkd\s+[\d,]+)',
        r'(minimum\s+hkd\s+[\d,]+)',
        r'(min\s+spend[:\s]+[^.]{10,100})',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            cost = match.group(1).strip()
            cost = re.sub(r'\s+', ' ', cost)
            return cost[:200]

    return ''


def _extract_tc_link(text: str, default_url: str) -> str:
    """Extract terms and conditions link"""

    # Look for URLs in text
    urls = re.findall(r'https?://[^\s<>"{}|\\^`\[\]]+', text)

    # Return first URL that looks like a TC link, or the default
    for url in urls:
        if 'tc' in url.lower() or 'terms' in url.lower() or 'condition' in url.lower():
            return url

    return default_url


def _is_bau_promotion(text: str, end_date: str) -> bool:
    """Determine if this is a BAU (ongoing) promotion"""

    # Check BAU patterns
    text_lower = text.lower()
    for pattern in BAU_PATTERNS:
        if re.search(pattern, text_lower):
            return True

    # Check for permanent keywords
    permanent_keywords = [
        'ongoing', 'permanent', 'no end date', 'always on',
        '長期', '永久', '長期優惠', 'lifetime'
    ]

    if any(keyword in text_lower for keyword in permanent_keywords):
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

    # Must have title (at least 10 characters)
    if not promo.get('title') or len(promo['title']) < 10:
        return False

    # Must have bank info
    if not promo.get('bank_id') or not promo.get('bank_name'):
        return False

    # Should have some description (at least 20 characters)
    if not promo.get('description') or len(promo.get('description', '')) < 20:
        return False

    # Title should not be just a URL
    title = promo.get('title', '').lower()
    if 'http' in title or '===' in title:
        return False

    # Description should not be mostly numbers
    desc = promo.get('description', '')
    digit_ratio = len(re.findall(r'\d', desc)) / max(len(desc), 1)
    if digit_ratio > 0.4:
        return False

    return True


# ── Product Extraction ───────────────────────────────────────────────────────

def extract_products_advanced(text: str, bank_id: str, bank_name: str) -> List[Dict[str, Any]]:
    """Extract banking products"""

    products = []

    product_keywords = {
        'card': ['credit card', 'debit card', 'visa', 'mastercard', 'platinum card'],
        'deposit': ['savings account', 'current account', 'deposit account', 'time deposit', '活期'],
        'loan': ['personal loan', 'mortgage', 'overdraft', 'tax loan'],
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
