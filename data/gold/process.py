from typing import List, Dict, Any, Optional
import numpy as np
import uuid
from sqlalchemy import create_engine, text, Column, String, Float, JSON, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sentence_transformers import SentenceTransformer
import logging
from datetime import datetime
import json
from rich.console import Console

# Configure logging
console = Console()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize SQLAlchemy
Base = declarative_base()

class MarketingInsightsFact(Base):
    """SQLAlchemy model for marketing insights fact table"""
    __tablename__ = 'marketing_insights_fact'
    __table_args__ = {'schema': 'gold'}
    
    id = Column(String, primary_key=True)
    query_text = Column(String, nullable=False)
    post_id = Column(String, nullable=False)
    cosine_similarity_score = Column(Float, nullable=False)
    engagement_score = Column(Float, nullable=False)
    post_text_cleaned = Column(String)
    source_type = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    metadata = Column(JSON)  # For additional metadata

class GoldLayerProcessor:
    """
    Enhanced Gold Layer Processor that integrates with the Bronze-Silver-Gold pipeline
    and provides marketing insights functionality.
    """
    
    def __init__(self, db_uri: str = "postgresql://postgres:postgres@localhost:5432/social_insights"):
        """
        Initialize the Gold Layer Processor.
        
        Args:
            db_uri: Database connection string
        """
        self.engine = create_engine(db_uri)
        self.Session = sessionmaker(bind=self.engine)
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Initialize database schema
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema if it doesn't exist"""
        with self.engine.connect() as conn:
            # Create schemas
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
            
            # Enable vector extension if not exists
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            except Exception as e:
                logger.warning(f"Could not enable vector extension: {e}")
            
            # Create tables
            try:
                Base.metadata.create_all(self.engine)
                logger.info("Database schema initialized successfully")
            except Exception as e:
                logger.error(f"Error initializing database schema: {e}")
    
    def generate_query_embedding(self, query: str) -> List[float]:
        """
        Generate embedding for a search query using the sentence transformer model.
        
        Args:
            query: The search query text
            
        Returns:
            List[float]: The 384-dimensional embedding vector
        """
        return self.model.encode(query, convert_to_numpy=True).tolist()
    
    def find_similar_posts(
        self, 
        query: str, 
        top_k: int = 10, 
        engagement_threshold: float = 0.0,
        min_similarity: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Find posts similar to the query using vector similarity search.
        
        Args:
            query: The search query text
            top_k: Maximum number of results to return
            engagement_threshold: Minimum engagement score for posts to be included
            min_similarity: Minimum cosine similarity score (0-1)
            
        Returns:
            List of dictionaries containing post information and similarity scores
        """
        query_embedding = self.generate_query_embedding(query)
        
        with self.engine.connect() as conn:
            # Convert query_embedding to string format for SQL
            embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'
            
            # Execute the similarity search query
            results = conn.execute(text("""
                WITH similarity_scores AS (
                    SELECT 
                        post_id,
                        post_text_cleaned,
                        engagement_score,
                        source_type,
                        created_at,
                        1 - (post_embedding <=> CAST(:embedding AS vector)) as cosine_similarity
                    FROM silver.social_posts_cleaned_features
                    WHERE engagement_score >= :engagement_threshold
                    AND (1 - (post_embedding <=> CAST(:embedding AS vector))) >= :min_similarity
                    ORDER BY post_embedding <=> CAST(:embedding AS vector)
                    LIMIT :top_k
                )
                SELECT 
                    gen_random_uuid() as id,
                    post_id,
                    post_text_cleaned,
                    cosine_similarity,
                    engagement_score,
                    source_type,
                    created_at
                FROM similarity_scores
                WHERE cosine_similarity >= :min_similarity
                ORDER BY cosine_similarity DESC
            "), {
                'embedding': embedding_str,
                'query_text': query,
                'engagement_threshold': engagement_threshold,
                'min_similarity': min_similarity,
                'top_k': top_k
            })
            
            # Convert results to list of dicts
            columns = ['id', 'post_id', 'post_text_cleaned', 'cosine_similarity', 
                     'engagement_score', 'source_type', 'created_at']
            
            posts = []
            for row in results:
                post = dict(zip(columns, row))
                posts.append({
                    'post_id': post['post_id'],
                    'post_text': post['post_text_cleaned'],
                    'similarity_score': float(post['cosine_similarity']),
                    'engagement_score': float(post['engagement_score']),
                    'source_type': post['source_type'],
                    'created_at': post['created_at'].isoformat() if post['created_at'] else None
                })
            
            return posts
    
    def get_marketing_insights(
        self, 
        query: str, 
        top_k: int = 10,
        similarity_threshold: float = 0.8
    ) -> Dict[str, Any]:
        """
        Generate marketing insights including high-value content and content gaps.
        
        Args:
            query: The search query text
            top_k: Number of results to return for each category
            similarity_threshold: Minimum similarity score to consider a match
            
        Returns:
            Dictionary containing marketing insights
        """
        # First, find all similar posts
        similar_posts = self.find_similar_posts(
            query=query,
            top_k=top_k * 4,  # Get more posts to analyze
            min_similarity=similarity_threshold
        )
        
        if not similar_posts:
            return {
                'high_value_content': [],
                'content_gaps': [],
                'top_performing_topics': [],
                'engagement_metrics': {
                    'avg_engagement': 0.0,
                    'max_engagement': 0.0,
                    'high_engagement_threshold': 0.0,
                    'low_engagement_threshold': 0.0
                }
            }
        
        # Calculate engagement statistics
        engagement_scores = [p['engagement_score'] for p in similar_posts]
        avg_engagement = sum(engagement_scores) / len(engagement_scores)
        max_engagement = max(engagement_scores)
        
        # Define thresholds (using quartiles)
        import numpy as np
        engagement_threshold_high = np.percentile(engagement_scores, 75)  # Top 25%
        engagement_threshold_low = np.percentile(engagement_scores, 25)   # Bottom 25%
        
        # Categorize posts
        high_value = []
        content_gaps = []
        
        for post in similar_posts:
            if post['engagement_score'] >= engagement_threshold_high:
                high_value.append(post)
            elif post['engagement_score'] <= engagement_threshold_low:
                content_gaps.append(post)
        
        # Limit to top_k results
        high_value = sorted(high_value, key=lambda x: x['similarity_score'], reverse=True)[:top_k]
        content_gaps = sorted(content_gaps, key=lambda x: x['similarity_score'], reverse=True)[:top_k]
        
        # Extract top performing topics (simplified example)
        top_topics = self._extract_top_topics(similar_posts)
        
        return {
            'high_value_content': high_value,
            'content_gaps': content_gaps,
            'top_performing_topics': top_topics,
            'engagement_metrics': {
                'avg_engagement': float(avg_engagement),
                'max_engagement': float(max_engagement),
                'high_engagement_threshold': float(engagement_threshold_high),
                'low_engagement_threshold': float(engagement_threshold_low)
            }
        }
    
    def _extract_top_topics(self, posts: List[Dict[str, Any]], top_n: int = 5) -> List[Dict[str, Any]]:
        """
        Extract top performing topics from posts.
        
        Args:
            posts: List of post dictionaries
            top_n: Number of topics to return
            
        Returns:
            List of topic dictionaries with name and score
        """
        from collections import defaultdict
        import re
        
        # Simple word frequency analysis (in a real app, use proper topic modeling)
        word_counts = defaultdict(int)
        
        for post in posts:
            text = post.get('post_text', '').lower()
            words = re.findall(r'\b\w{4,}\b', text)  # Only words with 4+ chars
            
            # Common English stopwords to exclude
            stopwords = {'this', 'that', 'with', 'have', 'from', 'your', 'they', 'their', 'there',
                        'what', 'when', 'where', 'which', 'will', 'would', 'been', 'also'}
            
            for word in words:
                if word not in stopwords and not word.isdigit():
                    word_counts[word] += 1
        
        # Get top N words by frequency
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        
        # Convert to list of dicts with normalized scores
        max_count = sorted_words[0][1] if sorted_words else 1
        topics = []
        
        for word, count in sorted_words[:top_n]:
            topics.append({
                'topic': word,
                'score': count / max_count  # Normalize to 0-1
            })
        
        return topics
    
    def save_insights_to_db(self, insights: Dict[str, Any], query: str) -> None:
        """
        Save marketing insights to the database.
        
        Args:
            insights: Dictionary containing marketing insights
            query: The original search query
        """
        session = self.Session()
        
        try:
            # Save high-value content
            for post in insights.get('high_value_content', []):
                insight = MarketingInsightsFact(
                    id=post.get('id', str(uuid.uuid4())),
                    query_text=query,
                    post_id=post['post_id'],
                    cosine_similarity_score=post['similarity_score'],
                    engagement_score=post['engagement_score'],
                    post_text_cleaned=post.get('post_text'),
                    source_type=post.get('source_type'),
                    metadata={
                        'insight_type': 'high_value',
                        'created_at': datetime.utcnow().isoformat()
                    }
                )
                session.merge(insight)
            
            # Save content gaps
            for post in insights.get('content_gaps', []):
                insight = MarketingInsightsFact(
                    id=post.get('id', str(uuid.uuid4())),
                    query_text=query,
                    post_id=post['post_id'],
                    cosine_similarity_score=post['similarity_score'],
                    engagement_score=post['engagement_score'],
                    post_text_cleaned=post.get('post_text'),
                    source_type=post.get('source_type'),
                    metadata={
                        'insight_type': 'content_gap',
                        'created_at': datetime.utcnow().isoformat()
                    }
                )
                session.merge(insight)
            
            session.commit()
            logger.info(f"Saved {len(insights.get('high_value_content', [])) + len(insights.get('content_gaps', []))} insights to database")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving insights to database: {e}")
            raise
            
        finally:
            session.close()

    def get_marketing_insights(self, query: str) -> Dict[str, Any]:
        """Get marketing insights for a given query"""
        # Find top performing content
        high_value = self.find_similar_posts(
            query, 
            top_k=10,
            engagement_threshold=0.75  # Top quartile threshold
        )
        
        # Find content gaps (high similarity but low engagement)
        content_gaps = self.find_similar_posts(
            query,
            top_k=10,
            engagement_threshold=0.0
        )
        content_gaps = [p for p in content_gaps if p['engagement_score'] < 0.25]  # Bottom quartile
        
        return {
            "query": query,
            "high_value_content": high_value,
            "content_gaps": content_gaps
        }
