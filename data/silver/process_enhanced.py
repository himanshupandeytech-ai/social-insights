#!/usr/bin/env python
"""
Enhanced Silver Layer Processing with FAANG-level data quality and transformations.
"""

from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, text
from rich.console import Console
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'ingestion', 'utils'))
from text_cleaning import clean_text, calculate_engagement_score, classify_source_type, deduplicate_posts
from great_expectations import DataQualityValidator

console = Console()
engine = create_engine("postgresql://postgres:postgres@localhost:5432/social_insights")
model = SentenceTransformer('all-MiniLM-L6-v2')

def get_competitor_keywords() -> List[str]:
    """Get list of competitor companies for source classification."""
    return ['huawei', 'samsung', 'pixel', 'google']

def get_dynamic_engagement_threshold() -> float:
    """
    Calculate dynamic engagement threshold based on 75th percentile of existing data.
    """
    try:
        with engine.connect() as conn:
            # Get engagement scores from last 30 days
            result = conn.execute(text("""
                SELECT engagement_score 
                FROM silver.social_posts_cleaned_features 
                WHERE created_at >= NOW() - INTERVAL '30 days'
                AND engagement_score IS NOT NULL
            """)).fetchall()
            
            if result:
                scores = [row[0] for row in result]
                q3 = np.percentile(scores, 75)
                console.print(f"[green]Dynamic engagement threshold (75th percentile): {q3:.2f}[/green]")
                return q3
            else:
                console.print("[yellow]No existing engagement data, using default threshold: 10.0[/yellow]")
                return 10.0
                
    except Exception as e:
        console.print(f"[red]Error calculating threshold: {e}. Using default: 10.0[/red]")
        return 10.0

def process_bronze_to_silver() -> Dict[str, Any]:
    """
    Process Bronze layer data to Silver layer with full FAANG-level transformations.
    """
    console.print("[bold blue]Starting Bronze → Silver transformation...[/bold blue]")
    
    with engine.connect() as conn:
        # Get watermark for incremental processing
        watermark_result = conn.execute(text("""
            SELECT last_successful_watermark 
            FROM metadata.pipeline_watermarks 
            WHERE pipeline_name = 'bronze_to_silver'
        """)).fetchone()
        
        last_watermark = watermark_result[0] if watermark_result else datetime.utcnow() - timedelta(days=1)
        console.print(f"Processing posts since: {last_watermark}")
        
        # Load new posts from Bronze
        bronze_posts = conn.execute(text("""
            SELECT post_id, company, platform, author_username, content, 
                   posted_at, url, likes, shares, comments
            FROM bronze.social_posts 
            WHERE created_at > :watermark
            ORDER BY posted_at DESC
        """), {"watermark": last_watermark}).fetchall()
        
        if not bronze_posts:
            console.print("[yellow]No new posts to process[/yellow]")
            return {"processed": 0, "errors": []}
        
        console.print(f"[green]Found {len(bronze_posts)} new posts[/green]")
        
        # Convert to dictionaries
        posts_list = []
        for row in bronze_posts:
            posts_list.append({
                "post_id": row[0],
                "company": row[1], 
                "platform": row[2],
                "author_username": row[3],
                "content": row[4],
                "posted_at": row[5],
                "url": row[6],
                "likes": row[7] or 0,
                "shares": row[8] or 0, 
                "comments": row[9] or 0
            })
        
        # Step A1: Load Raw (already done)
        # Step A2: Clean Text
        console.print("[blue]Step A2: Cleaning text...[/blue]")
        for post in posts_list:
            post['post_text_cleaned'] = clean_text(post['content'])
        
        # Step A3: Deduplicate
        console.print("[blue]Step A3: Deduplicating posts...[/blue]")
        original_count = len(posts_list)
        posts_list = deduplicate_posts(posts_list)
        console.print(f"[green]Removed {original_count - len(posts_list)} duplicates[/green]")
        
        # Step A4: Compute Engagement
        console.print("[blue]Step A4: Computing engagement scores...[/blue]")
        competitor_keywords = get_competitor_keywords()
        for post in posts_list:
            post['engagement_score'] = calculate_engagement_score(
                post['likes'], post['shares'], post['comments']
            )
            post['source_type'] = classify_source_type(
                post['author_username'], post['content'], competitor_keywords
            )
        
        # Step A5: Generate Embeddings
        console.print("[blue]Step A5: Generating embeddings...[/blue]")
        contents = [post['post_text_cleaned'] for post in posts_list]
        vectors = model.encode(contents, show_progress_bar=True, batch_size=32)
        
        for post, vec in zip(posts_list, vectors):
            post['post_embedding'] = vec.tolist()
        
        # Data Quality Validation
        console.print("[blue]Running data quality validation...[/blue]")
        validator = DataQualityValidator()
        
        schema_validation = validator.validate_schema_silver(posts_list)
        quality_validation = validator.validate_data_quality(posts_list)
        
        if not schema_validation['passed']:
            console.print("[red]Schema validation failed![/red]")
            for error in schema_validation['errors']:
                console.print(f"  - {error:}")
        
        if not quality_validation['passed']:
            console.print("[red]Data quality validation failed![/red]")
            for error in quality_validation['errors']:
                console.print(f"  - {error}")
        
 # Create silver schema and table if not exists
        console.print("[blue]Creating silver schema and table...[/blue]")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS silver.social_posts_cleaned_features (
                post_id VARCHAR(255) PRIMARY KEY,
                company VARCHAR(100),
                platform VARCHAR(50),
                author_username VARCHAR(100),
                post_text_cleaned TEXT,
                engagement_score FLOAT,
                post_embedding vector(384),
                source_type VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Insert processed data
        console.print("[blue]Inserting cleaned data to silver layer...[/blue]")
        for post in posts_list:
            conn.execute(text("""
                INSERT INTO silver.social_posts_cleaned_features 
                (post_id, company, platform, author_username, post_text_cleaned, 
                 engagement_score, post_embedding, source_type)
                VALUES (:post_id, :company, :platform, :author_username, :post_text_cleaned,
                        :engagement_score, :post_embedding, :source_type)
                ON CONFLICT (post_id) DO UPDATE SET
                    company = EXCLUDED.company,
                    platform = EXCLUDED.platform,
                    author_username = EXCLUDED.author_username,
                    post_text_cleaned = EXCLUDED.post_text_cleaned,
                    engagement_score = EXCLUDED.engagement_score,
                    post_embedding = EXCLUDED.post_embedding,
                    source_type = EXCLUDED.source_type,
                    processed_at = CURRENT_TIMESTAMP
            """), post)
        
        # Update watermark
        conn.execute(text("""
            INSERT INTO metadata.pipeline_watermarks (pipeline_name, last_successful_watermark)
            VALUES ('bronze_to_silver', :watermark)
            ON CONFLICT (pipeline_name) DO UPDATE SET
                last_successful_watermark = EXCLUDED.last_successful_watermark
        """), {"watermark": datetime.utcnow()})
        
        conn.commit()
        
        console.print(f"[bold green]Successfully processed {len(posts_list)} posts to silver layer[/bold green]")
        
        return {
            "processed": len(posts_list),
            "errors": schema_validation['errors'] + quality_validation['errors'],
            "metrics": quality_validation.get('metrics', {})
        }

if __name__ == "__main__":
    result = process_bronze_to_silver()
    console.print(f"\n[bold]Summary:[/bold]")
    console.print(f"Processed: {result['processed']} posts")
    console.print(f"Errors: {len(result['errors'])}")
    if result['metrics']:
        console.print(f"Average Engagement: {result['metrics'].get('avg_engagement', 0):.2f}")
