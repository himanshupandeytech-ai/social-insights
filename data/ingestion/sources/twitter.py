import logging
import time
from typing import List, Dict
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

NITTER_INSTANCES = [
    "https://nitter.net",
    "https://nitter.it",
    "https://nitter.privacydev.net",
    "https://nitter.eu"
]

def get_working_nitter() -> str:
    for instance in NITTER_INSTANCES:
        try:
            r = requests.head(instance, timeout=5)
            if r.status_code == 200:
                return instance.rstrip("/")
        except:
            continue
    raise Exception("All nitter instances down")

def scrape_twitter(query: str, company: str, limit: int = 200, since_date: datetime = None) -> List[Dict]:
    posts = []
    try:
        base = get_working_nitter()
    except Exception as e:
        logger.error(f"No working nitter instances: {e}")
        return posts
    
    url = f"{base}/search?f=tweets&q={quote_plus(query)}&since=&until=&near="
    headers = {"User-Agent": "social-insights-bot/0.1"}
    
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Try multiple selectors for tweets
        tweet_selectors = [
            "div.tweet",
            "div.timeline-item",
            "article",
            "div.status"
        ]
        
        found_tweets = False
        for selector in tweet_selectors:
            for tweet in soup.select(selector):
                # Try multiple selectors for tweet content
                content_selectors = [
                    ".tweet-content",
                    ".status-content",
                    "div.e-content",
                    "p",
                    ".tweet-text"
                ]
                
                content_el = None
                for content_sel in content_selectors:
                    content_el = tweet.select_one(content_sel)
                    if content_el:
                        break
                
                if not content_el:
                    continue
                
                content = content_el.get_text(strip=True)
                if not content or len(content) < 10:
                    continue
                
                # Get author
                author = "unknown"
                author_selectors = [
                    ".tweet-name",
                    ".username",
                    ".author",
                    "strong.fullname",
                    "span.username"
                ]
                for auth_sel in author_selectors:
                    auth_el = tweet.select_one(auth_sel)
                    if auth_el:
                        author = auth_el.get_text(strip=True).replace("@", "")
                        break
                
                # Get link
                link_el = tweet.find("a", href=True)
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = base + link
                
                # Get timestamp if available
                tweet_id = f"twitter_{len(posts)}_{int(time.time()*1e6)}"
                time_el = tweet.select_one(".time", "span.tweet-date", "a.tweet-link")
                if time_el and time_el.get("href"):
                    tweet_id = f"twitter_{time_el.get('href', '').split('/')[-1]}"
                
                posts.append({
                    "post_id": tweet_id,
                    "company": company,
                    "platform": "twitter",
                    "author_username": author,
                    "content": content[:2000],
                    "posted_at": datetime.utcnow(),
                    "url": link,
                })
                
                found_tweets = True
                if len(posts) >= limit:
                    break
            
            if found_tweets:
                break
        
        if not found_tweets:
            logger.warning(f"No tweets found with any selector on {base}")
                
    except Exception as e:
        logger.error(f"Twitter scrape failed: {e}")
    
    logger.info(f"Scraped {len(posts)} Twitter posts for {company}")
    return posts