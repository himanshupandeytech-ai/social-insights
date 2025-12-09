from sqlalchemy import create_engine, text

def clean_database():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/social_insights')
    with engine.connect() as conn:
        # Truncate tables in all schemas
        conn.execute(text("TRUNCATE TABLE bronze.social_posts CASCADE;"))
        conn.execute(text("TRUNCATE TABLE silver.social_posts_cleaned_features CASCADE;"))
        conn.execute(text("""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.tables 
                          WHERE table_schema = 'gold' AND table_name = 'marketing_insights') THEN
                    TRUNCATE TABLE gold.marketing_insights CASCADE;
                END IF;
            END $$;
        
        """))
        conn.commit()
        print("Database cleanup completed successfully.")

if __name__ == "__main__":
    clean_database()
