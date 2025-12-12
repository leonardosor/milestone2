-- Database initialization script for Milestone 2 ETL
-- This script runs automatically when the PostgreSQL container starts

-- Create the default schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS test;

-- Grant permissions to the postgres user
GRANT ALL PRIVILEGES ON SCHEMA test TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA test TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA test TO postgres;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT ALL PRIVILEGES ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT ALL PRIVILEGES ON SEQUENCES TO postgres;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'Database initialization completed';
END $$;
