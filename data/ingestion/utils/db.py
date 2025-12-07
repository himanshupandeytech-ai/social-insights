import logging
from typing import List, Dict
from datetime import datetime
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

def upsert_bronze_posts(posts: List[Dict]):
    """
    Upsert posts to the bronze layer database.
    
    Args:
        posts: List of post dictionaries with keys like:
            - post_id: str
            - company: str  
            - platform: str
            - author_username: str
            - content: str
            - posted_at: datetime
            - url: str
    """
    if not posts:
        logger.info("No posts to upsert")
        return
    
    logger.info(f"Upserting {len(posts)} posts to bronze layer")
    
    try:
        # Connect to PostgreSQL in Docker
        engine = create_engine("postgresql://postgres:postgres@localhost:5432/social_insights")
        
        with engine.connect() as conn:
            # Create bronze schema if not exists
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
            
            # Create table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS bronze.social_posts (
                    post_id VARCHAR(255) PRIMARY KEY,
                    company VARCHAR(100),
                    platform VARCHAR(50),
                    author_username VARCHAR(100),
                    content TEXT,
                    posted_at TIMESTAMP,
                    url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Upsert posts
            for post in posts:
                conn.execute(text("""
                    INSERT INTO bronze.social_posts 
                    (post_id, company, platform, author_username, content, posted_at, url)
                    VALUES (:post_id, :company, :platform, :author_username, :content, :posted_at, :url)
                    ON CONFLICT (post_id) DO UPDATE SET
                        company = EXCLUDED.company,
                        platform = EXCLUDED.platform,
                        author_username = EXCLUDED.author_username,
                        content = EXCLUDED.content,
                        posted_at = EXCLUDED.posted_at,
                        url = EXCLUDED.url
                """), post)
            
            conn.commit()
            logger.info(f"Successfully upserted {len(posts)} posts to bronze.social_posts")
            
    except Exception as e:
        logger.error(f"Failed to upsert posts to database: {e}")
        raise
