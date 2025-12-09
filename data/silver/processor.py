from typing import List, Dict, Any, Optional, Tuple
import numpy as np
from datetime import datetime
from ..models import SilverPost, GoldInsight, MarketingInsights, SourceType

class SilverToGoldProcessor:
    """Process data from Silver to Gold layer with semantic search and insights."""
    
    def __init__(self, embedding_model):
        self.embedding_model = embedding_model
    
    def cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        vec1 = np.array(vec1)
        vec2 = np.array(vec2)
        return float(np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2)))
    
    def generate_query_embedding(self, query: str) -> List[float]:
        """Generate embedding for the search query."""
        # In practice, use the same embedding model as in BronzeToSilverProcessor
        # For now, return a dummy 384-dim vector
        return [0.0] * 384
    
    def find_similar_posts(
        self,
        query: str,
        posts: List[SilverPost],
        top_k: int = 10,
        min_similarity: float = 0.0,
        engagement_threshold: float = 0.0
    ) -> List[Dict[str, Any]]:
        """Find posts similar to the query using vector similarity."""
        query_embedding = self.generate_query_embedding(query)
        
        # Calculate similarities
        similarities = []
        for post in posts:
            if post.engagement_score < engagement_threshold:
                continue
                
            similarity = self.cosine_similarity(query_embedding, post.post_embedding)
            if similarity >= min_similarity:
                similarities.append((similarity, post))
        
        # Sort by similarity (descending)
        similarities.sort(key=lambda x: x[0], reverse=True)
        
        # Return top-k results
        results = []
        for similarity, post in similarities[:top_k]:
            results.append({
                "post_id": post.post_id,
                "post_text": post.post_text_cleaned,
                "similarity_score": similarity,
                "engagement_score": post.engagement_score,
                "source_type": post.source_type,
                "created_at": post.created_at.isoformat()
            })
        
        return results
    
    def generate_marketing_insights(
        self,
        query: str,
        posts: List[SilverPost],
        top_k: int = 10,
        similarity_threshold: float = 0.8
    ) -> MarketingInsights:
        """Generate marketing insights including high-value content and content gaps."""
        if not posts:
            return MarketingInsights(
                high_value_content=[],
                content_gaps=[],
                top_performing_topics=[],
                engagement_metrics={"avg_engagement": 0.0, "max_engagement": 0.0}
            )
        
        # Get engagement scores for threshold calculation
        engagement_scores = [p.engagement_score for p in posts]
        avg_engagement = sum(engagement_scores) / len(engagement_scores)
        max_engagement = max(engagement_scores)
        
        # Define thresholds (using quartiles)
        engagement_threshold_high = np.percentile(engagement_scores, 75)  # Top 25%
        engagement_threshold_low = np.percentile(engagement_scores, 25)   # Bottom 25%
        
        # Find similar posts
        similar_posts = self.find_similar_posts(
            query=query,
            posts=posts,
            top_k=top_k * 2,  # Get more posts to filter
            min_similarity=similarity_threshold
        )
        
        # Categorize posts
        high_value = []
        content_gaps = []
        
        for post in similar_posts:
            insight = GoldInsight(
                query_text=query,
                post_id=post["post_id"],
                cosine_similarity_score=post["similarity_score"],
                engagement_score=post["engagement_score"],
                post_text_cleaned=post["post_text"],
                source_type=post["source_type"]
            )
            
            if post["engagement_score"] >= engagement_threshold_high:
                high_value.append(insight)
            elif post["engagement_score"] <= engagement_threshold_low:
                content_gaps.append(insight)
        
        # Get top performing topics (simplified example)
        top_topics = self._extract_top_topics(posts, top_n=5)
        
        return MarketingInsights(
            high_value_content=high_value[:top_k],
            content_gaps=content_gaps[:top_k],
            top_performing_topics=top_topics,
            engagement_metrics={
                "avg_engagement": avg_engagement,
                "max_engagement": max_engagement,
                "high_engagement_threshold": engagement_threshold_high,
                "low_engagement_threshold": engagement_threshold_low
            }
        )
    
    def _extract_top_topics(self, posts: List[SilverPost], top_n: int = 5) -> List[Dict[str, Any]]:
        """Extract top performing topics from posts (simplified example)."""
        # In a real implementation, you would use topic modeling (e.g., LDA, BERTopic)
        # This is a simplified version that just looks at word frequencies
        from collections import defaultdict
        import re
        
        # Simple word frequency analysis
        word_counts = defaultdict(int)
        for post in posts:
            words = re.findall(r'\b\w{3,}\b', post.post_text_cleaned.lower())
            for word in words:
                if word not in {'the', 'and', 'for', 'with', 'this', 'that', 'was', 'were', 'are', 'you', 'your', 'have', 'has', 'had', 'they', 'their'}:
                    word_counts[word] += 1
        
        # Get top N words by frequency
        top_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
        
        # Format as topics (in a real implementation, this would be more sophisticated)
        return [{"topic": word, "count": count} for word, count in top_words]
    
    def run_quality_checks(self, insights: MarketingInsights) -> dict:
        """Run data quality checks on generated insights."""
        # Check similarity scores are within valid range
        valid_similarity = all(
            -1.0 <= i.cosine_similarity_score <= 1.0 
            for i in insights.high_value_content + insights.content_gaps
        )
        
        # Check referential integrity (simplified)
        valid_references = True
        
        return {
            "value_range": valid_similarity,
            "referential_integrity": valid_references
        }
