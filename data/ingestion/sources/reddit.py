import logging
import time
from typing import List, Dict
from datetime import datetime
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def scrape_reddit(company: str, subreddits: List[str], limit_per_sub: int = 100, since_date: datetime = None) -> List[Dict]:
    posts = []
    headers = {
        "User-Agent": "social-insights-bot/0.1 (educational non-commercial)"
    }
    
    for subreddit in subreddits:
        # Try both old.reddit.com and www.reddit.com
        urls = [
            f"https://old.reddit.com/r/{subreddit}/search?q={company}&restrict_sr=1&sort=new",
            f"https://www.reddit.com/r/{subreddit}/search?q={company}&restrict_sr=1&sort=new"
        ]
        
        for url in urls:
            try:
                r = requests.get(url, headers=headers, timeout=10)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
                
                # Try multiple selectors for Reddit posts
                post_selectors = [
                    "div[data-testid='search-post-unit']",
                    "div[data-testid='post-container']",
                    "div.thing",
                    "article",
                    "div[data-adclicklocation='media']"
                ]
                
                found_posts = False
                for selector in post_selectors:
                    for post in soup.select(selector):
                        title_el = None
                        # Try multiple title selectors
                        title_selectors = [
                            "h3",
                            "a.title",
                            "h1",
                            "[data-testid='post-title']",
                            "a[href*='/comments/']"
                        ]
                        
                        for title_sel in title_selectors:
                            title_el = post.select_one(title_sel)
                            if title_el:
                                break
                        
                        if not title_el:
                            continue
                            
                        title = title_el.get_text(strip=True)
                        if not title or len(title) < 10:
                            continue
                        
                        # Get link - prioritize comment links
                        link_el = post.find("a", href=lambda x: x and '/comments/' in x)
                        if not link_el:
                            link_el = post.find("a", href=True)
                        link = link_el.get("href", "") if link_el else ""
                        if link and not link.startswith("http"):
                            link = "https://www.reddit.com" + link
                        
                        # Get author
                        author = "unknown"
                        author_selectors = [
                            "[data-testid='post-author-link']",
                            "a.author",
                            ".tagline .author",
                            "a[href*='/u/']",
                            "a[href*='/user/']"
                        ]
                        for auth_sel in author_selectors:
                            auth_el = post.select_one(auth_sel)
                            if auth_el:
                                author = auth_el.get_text(strip=True).replace("u/", "").replace("@", "")
                                break
                        
                        posts.append({
                            "post_id": f"reddit_{post.get('data-fullname', f'reddit_{len(posts)}_{int(time.time()*1e6)}')}",
                            "company": company,
                            "platform": "reddit",
                            "author_username": author,
                            "content": title[:2000],
                            "posted_at": datetime.utcnow(),
                            "url": link,
                        })
                        
                        found_posts = True
                        if len(posts) >= limit_per_sub:
                            break
                    
                    if found_posts:
                        break
                        
                if found_posts:
                    break
                    
            except Exception as e:
                logger.warning(f"Reddit scrape failed for r/{subreddit} with {url}: {e}")
                continue
        
        time.sleep(2)  # be nice
    
    logger.info(f"Scraped {len(posts)} Reddit posts for {company}")
    return posts
