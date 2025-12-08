import re
import html
from typing import Dict, Any

def clean_text(raw_text: str) -> str:
    """
    Clean and normalize text for embedding generation.
    
    Args:
        raw_text: Raw text from social media posts
        
    Returns:
        Cleaned, normalized text
    """
    if not raw_text:
        return ""
    
    text = raw_text
    
    # Remove URLs
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    
    # Remove HTML entities
    text = html.unescape(text)
    
    # Remove emojis and special unicode characters
    text = re.sub(r'[^\w\s#@.,!?\'-]', '', text)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove extra punctuation but keep basic sentence structure
    text = re.sub(r'[^\w\s.,!?]', '', text)
    
    return text

def calculate_engagement_score(likes: int = 0, shares: int = 0, comments: int = 0) -> float:
    """
    Calculate weighted engagement score.
    
    Formula: (0.2 * likes) + (0.3 * shares) + (0.5 * comments)
    
    Args:
        likes: Number of likes
        shares: Number of shares/retweets
        comments: Number of comments
        
    Returns:
        Weighted engagement score
    """
    return (0.2 * likes) + (0.3 * shares) + (0.5 * comments)

def classify_source_type(author: str, content: str, company_keywords: list) -> str:
    """
    Classify source type based on author and content analysis.
    
    Args:
        author: Author username/handle
        content: Post content
        company_keywords: List of competitor company names
        
    Returns:
        Source type: 'Customer', 'Competitor', or 'Reviewer'
    """
    author_lower = author.lower()
    content_lower = content.lower()
    
    # Check if it's a competitor official account
    for company in company_keywords:
        if company.lower() in author_lower or f"official{company.lower()}" in author_lower:
            return 'Competitor'
    
    # Check if it's a reviewer/influencer
    reviewer_indicators = ['review', 'impression', 'hands on', 'unboxing', 'test', 'vs', 'comparison']
    if any(indicator in content_lower for indicator in reviewer_indicators):
        return 'Reviewer'
    
    # Default to customer
    return 'Customer'

def deduplicate_posts(posts: list) -> list:
    """
    Remove duplicate posts based on content similarity.
    
    Args:
        posts: List of post dictionaries
        
    Returns:
        Deduplicated list of posts
    """
    seen_hashes = set()
    unique_posts = []
    
    for post in posts:
        # Create content hash for exact duplicate detection
        content = post.get('content', '')
        content_hash = hash(content.lower().strip())
        
        if content_hash not in seen_hashes:
            seen_hashes.add(content_hash)
            unique_posts.append(post)
    
    return unique_posts
