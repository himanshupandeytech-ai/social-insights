#!/usr/bin/env python
import logging
from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, text
from rich.console import Console

console = Console()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_engine("postgresql://postgres:postgres@localhost:5432/social_insights")
model = SentenceTransformer('all-MiniLM-L6-v2')

def process():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT post_id, content FROM bronze.social_posts 
            WHERE _ingested_at > NOW() - INTERVAL '7 days'
        """)).fetchall()

        if not rows:
            console.print("[yellow]No new posts to embed[/yellow]")
            return

        contents = [row[1] for row in rows]
        vectors = model.encode(contents, show_progress_bar=True, batch_size=32)

        data = [
            {"post_id": row[0], "embedding": vec.tolist()}
            for row, vec in zip(rows, vectors)
        ]

        conn.execute(text("""
            INSERT INTO silver.social_posts_clean (post_id, embedding)
            VALUES (:post_id, :embedding::vector)
            ON CONFLICT (post_id) DO UPDATE SET embedding = EXCLUDED.embedding
        """), data)

        console.print(f"[green]Embedded {len(data)} posts with all-MiniLM-L6-v2[/green]")

if __name__ == "__main__":
    process()
