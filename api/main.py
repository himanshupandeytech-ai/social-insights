from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from data.gold.process import GoldLayerProcessor
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Marketing Insights API",
             description="API for accessing marketing insights and content recommendations",
             version="1.0.0")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

processor = GoldLayerProcessor()

class QueryRequest(BaseModel):
    query: str
    top_k: int = 10
    engagement_threshold: float = 0.0

class SearchResponse(BaseModel):
    status: str
    results: List[Dict[str, Any]]

class InsightsResponse(BaseModel):
    status: str
    insights: Dict[str, Any]

@app.post("/api/search", response_model=SearchResponse)
async def search_similar_posts(request: QueryRequest):
    """
    Search for posts similar to the query text using semantic search
    """
    try:
        results = processor.find_similar_posts(
            query=request.query,
            top_k=request.top_k,
            engagement_threshold=request.engagement_threshold
        )
        return {"status": "success", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/insights", response_model=InsightsResponse)
async def get_marketing_insights(request: QueryRequest):
    """
    Get marketing insights including high-value content and content gaps
    """
    try:
        insights = processor.get_marketing_insights(request.query)
        return {"status": "success", "insights": insights}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}
