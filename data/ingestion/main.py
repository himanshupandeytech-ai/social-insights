import logging
from pathlib import Path
import yaml
import time
from datetime import datetime, timedelta
from data.ingestion.sources.twitter import scrape_twitter
from data.ingestion.sources.reddit import scrape_reddit
from data.ingestion.utils.db import upsert_bronze_posts

logger = logging.getLogger(__name__)
CONFIG_PATH = Path(__file__).parent / "config" / "companies.yaml"
TIMESTAMP_FILE = Path(__file__).parent.parent.parent / ".last_ingest.txt"

def load_config():
    with open(CONFIG_PATH) as f:
        data = yaml.safe_load(f)
        return data.get("companies", [])

def get_last_ingest_time():
    """Get the timestamp of the last ingestion run"""
    if TIMESTAMP_FILE.exists():
        try:
            timestamp = int(TIMESTAMP_FILE.read_text().strip())
            return datetime.fromtimestamp(timestamp)
        except:
            pass
    # Default to 7 days ago if no timestamp file exists
    return datetime.utcnow() - timedelta(days=7)

def save_ingest_time():
    """Save the current ingestion timestamp"""
    TIMESTAMP_FILE.write_text(str(int(time.time())))

def main():
    companies = load_config()
    if not companies:
        logger.error("No companies in config!")
        return

    last_ingest = get_last_ingest_time()
    logger.info(f"Delta ingestion: fetching posts since {last_ingest.strftime('%Y-%m-%d %H:%M')}")

    all_posts = []
    for comp in companies:
        name = comp["name"]
        logger.info(f"Scraping {name}...")
        all_posts.extend(scrape_twitter(comp.get("twitter_keywords", name), name, since_date=last_ingest))
        all_posts.extend(scrape_reddit(name, comp.get("reddit_subreddits", []), limit_per_sub=100, since_date=last_ingest))

    if all_posts:
        upsert_bronze_posts(all_posts)
        logger.info(f"Inserted {len(all_posts)} posts")
        save_ingest_time()
    else:
        logger.info("No new posts")

if __name__ == "__main__":
    main()
