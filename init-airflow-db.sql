-- Create the Airflow metadata database
-- This runs automatically when PostgreSQL container is first initialized
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO garmin;

