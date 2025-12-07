CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS governance;

CREATE ROLE data_engineer;
CREATE ROLE data_analyst;
CREATE ROLE marketing_user;
GRANT USAGE ON SCHEMA bronze, silver, gold, governance TO data_engineer, data_analyst, marketing_user;
GRANT CREATE, INSERT, SELECT, UPDATE ON SCHEMA bronze TO data_engineer;
GRANT CREATE, INSERT, SELECT, UPDATE ON SCHEMA silver TO data_engineer;
GRANT SELECT ON SCHEMA silver, gold TO data_analyst;
GRANT SELECT ON SCHEMA gold TO marketing_user;

-- === Bronze: raw scraped posts ===
CREATE TABLE IF NOT EXISTS bronze.social_posts (
	post_id TEXT PRIMARY KEY,
	company TEXT NOT NULL,
	platform TEXT NOT NULL,
	author_username TEXT,
	content TEXT NOT NULL,
	posted_at TIMESTAMPTZ,
	url TEXT,
	_ingested_at TIMESTAMPTZ DEFAULT NOW(),
	_source_file TEXT
);

-- === Silver: cleaned + embeddings ready ===
CREATE TABLE IF NOT EXISTS silver.social_posts_clean (
	post_id TEXT PRIMARY KEY,
	company TEXT NOT NULL,
	platform TEXT NOT NULL,
	author_username TEXT,
	content TEXT NOT NULL,
	posted_at TIMESTAMPTZ,
	sentiment_score NUMERIC,
	embedding VECTOR(384),           	-- 384-dim for all-MiniLM-L6-v2
	_ingested_at TIMESTAMPTZ DEFAULT NOW(),
	_quality_score NUMERIC DEFAULT 100
);

-- === Gold: aggregated weekly buzz (for dashboards + RAG prompts) ===
CREATE TABLE IF NOT EXISTS gold.weekly_buzz_summary (
	week_start DATE PRIMARY KEY,
	company TEXT NOT NULL,
	total_posts INT,
	positive_posts INT,
	negative_posts INT,
	top_themes TEXT[],
	generated_tweet_ideas TEXT[]
);

-- === Governance tables ===
CREATE TABLE IF NOT EXISTS governance.data_quality_checks (
	check_id SERIAL PRIMARY KEY,
	check_name TEXT,
	table_name TEXT,
	status TEXT,
	success_percent NUMERIC,
	error_message TEXT,
	checked_at TIMESTAMPTZ DEFAULT NOW()
);

-- === Privacy: Row-level policy to mask usernames for non-engineers ===
CREATE POLICY mask_usernames ON bronze.social_posts
	FOR SELECT USING (current_user NOT IN ('data_engineer', 'postgres'));

CREATE POLICY mask_usernames_silver ON silver.social_posts_clean
	FOR SELECT USING (current_user NOT IN ('data_engineer', 'postgres'));

-- Apply masking: non-engineers see 'anonymous' instead of real username
ALTER TABLE bronze.social_posts ENABLE ROW LEVEL SECURITY;
ALTER TABLE silver.social_posts_clean ENABLE ROW LEVEL SECURITY;

-- Replace username with 'anonymous' for masked users
CREATE OR REPLACE FUNCTION mask_username()
RETURNS TRIGGER AS $$
BEGIN
	IF current_user NOT IN ('data_engineer', 'postgres') THEN
    	NEW.author_username := 'anonymous';
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_mask_username_bronze
	BEFORE INSERT OR UPDATE ON bronze.social_posts
	FOR EACH ROW EXECUTE FUNCTION mask_username();

CREATE TRIGGER trigger_mask_username_silver
	BEFORE INSERT OR UPDATE ON silver.social_posts_clean
	FOR EACH ROW EXECUTE FUNCTION mask_username();
