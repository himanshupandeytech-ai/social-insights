import re
from typing import List, Optional
from datetime import datetime
import numpy as np
from ..models import BronzePost, SilverPost, SourceType

class BronzeToSilverProcessor:
    """Process raw social media posts from Bronze to Silver layer."""
    
    def __init__(self, embedding_model):
        self.embedding_model = embedding_model
    
    def clean_text(self, text: str) -> str:
        """Clean and standardize text content."""
        if not text:
            return ""
        
        # Remove URLs
        text = re.sub(r'https?://\S+|www\.\S+', '', text)
        # Remove emojis and special characters (keeping basic punctuation)
        text = re.sub(r'[^\w\s.,!?-]', ' ', text)
        # Convert to lowercase and remove extra whitespace
        text = ' '.join(text.lower().split())
        return text
    
    def calculate_engagement_score(self, post: BronzePost) -> float:
        """Calculate weighted engagement score."""
        return (0.2 * post.likes) + (0.3 * post.shares) + (0.5 * post.comments)
    
    def generate_embedding(self, text: str) -> List[float]:
        """Generate text embedding using the provided model."""
        # This is a placeholder - in practice, you would call your embedding model here
        # For example: return self.embedding_model.encode(text)
        # For now, return a dummy 384-dim vector
        return [0.0] * 384
    
    def process_post(self, post: BronzePost) -> SilverPost:
        """Process a single post from Bronze to Silver layer."""
        cleaned_text = self.clean_text(post.post_text)
        engagement_score = self.calculate_engagement_score(post)
        embedding = self.generate_embedding(cleaned_text)
        
        return SilverPost(
            post_id=post.post_id,
            post_text_cleaned=cleaned_text,
            engagement_score=engagement_score,
            post_embedding=embedding,
            source_type=post.source_type,
            created_at=post.created_at
        )
    
    def process_batch(self, posts: List[BronzePost]) -> List[SilverPost]:
        """Process a batch of posts."""
        return [self.process_post(post) for post in posts]
    
    def deduplicate_posts(self, posts: List[SilverPost]) -> List[SilverPost]:
        """Remove duplicate posts based on post_id."""
        seen = set()
        unique_posts = []
        for post in posts:
            if post.post_id not in seen:
                seen.add(post.post_id)
                unique_posts.append(post)
        return unique_posts
    
    def run_quality_checks(self, posts: List[SilverPost]) -> dict:
        """Run data quality checks on processed posts."""
        if not posts:
            return {
                "completeness": 0.0,
                "uniqueness": 1.0,
                "schema_compliance": 1.0
            }
            
        total_posts = len(posts)
        complete_posts = sum(1 for p in posts if p.post_text_cleaned.strip())
        unique_posts = len({p.post_id for p in posts})
        valid_embeddings = sum(1 for p in posts if 
                             isinstance(p.post_embedding, list) and 
                             len(p.post_embedding) == 384 and 
                             all(isinstance(x, float) for x in p.post_embedding))
        
        return {
            "completeness": complete_posts / total_posts,
            "uniqueness": unique_posts / total_posts,
            "schema_compliance": valid_embeddings / total_posts
        }
