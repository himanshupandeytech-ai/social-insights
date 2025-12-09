from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class SourceType(str, Enum):
    CUSTOMER = "customer"
    COMPETITOR = "competitor"

class BronzePost(BaseModel):
    """Raw data model for social media posts in the Bronze layer."""
    post_id: str
    post_text: str
    likes: int = 0
    shares: int = 0
    comments: int = 0
    source_type: SourceType
    created_at: datetime
    raw_metadata: Dict[str, Any] = Field(default_factory=dict)

class SilverPost(BaseModel):
    """Processed data model for social media posts in the Silver layer."""
    post_id: str
    post_text_cleaned: str
    engagement_score: float
    post_embedding: List[float]
    source_type: SourceType
    created_at: datetime

class GoldInsight(BaseModel):
    """Aggregated insights in the Gold layer."""
    query_text: str
    post_id: str
    cosine_similarity_score: float
    engagement_score: float
    post_text_cleaned: str
    source_type: SourceType
    created_at: datetime = Field(default_factory=datetime.utcnow)

class MarketingInsights(BaseModel):
    """Marketing insights including high-value content and content gaps."""
    high_value_content: List[GoldInsight]
    content_gaps: List[GoldInsight]
    top_performing_topics: List[Dict[str, Any]]
    engagement_metrics: Dict[str, float]
