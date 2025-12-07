#!/usr/bin/env python
from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, text
from rich.console import Console

console = Console()
engine = create_engine("postgresql://postgres:postgres@localhost:5432/social_insights")
model = SentenceTransformer('all-MiniLM-L6-v2')

with engine.connect() as conn:
    rows = conn.execute(text("SELECT post_id, content FROM bronze.social_posts")).fetchall()
    
    if not rows:
        console.print("[red]No posts in bronze![/red]")
    else:
        contents = [row[1] for row in rows]
        vectors = model.encode(contents, show_progress_bar=True, batch_size=32)
        data = [{"post_id": row[0], "embedding": vec.tolist()} for row, vec in zip(rows, vectors)]
        
        # Create silver schema and table if not exists
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS silver.social_posts_clean (
                post_id VARCHAR(255) PRIMARY KEY,
                embedding vector(384),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Insert embeddings one by one to handle vector casting properly
        for item in data:
            conn.execute(text("""
                INSERT INTO silver.social_posts_clean (post_id, embedding)
                VALUES (:post_id, :embedding)
                ON CONFLICT (post_id) DO UPDATE SET embedding = EXCLUDED.embedding
            """), {"post_id": item["post_id"], "embedding": item["embedding"]})
        
        conn.commit()
        
        console.print(f"[bold green]Successfully embedded {len(data)} posts with all-MiniLM-L6-v2[/bold green]")
