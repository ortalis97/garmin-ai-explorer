"""
PostgreSQL database module for Garmin data storage.
Handles connection management, schema creation, and data operations.
"""
import os
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Generator
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def get_connection_params() -> Dict[str, str]:
    """Get PostgreSQL connection parameters from environment variables."""
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "database": os.environ.get("POSTGRES_DB", "garmin_data"),
        "user": os.environ.get("POSTGRES_USER", "garmin"),
        "password": os.environ.get("POSTGRES_PASSWORD", "garmin_secret"),
    }


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager for database connections."""
    conn = psycopg2.connect(**get_connection_params())
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_cursor(dict_cursor: bool = False) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Context manager for database cursors with auto-commit."""
    with get_connection() as conn:
        cursor_factory = RealDictCursor if dict_cursor else None
        cursor = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


# SQL schema definitions
SCHEMA_SQL = """
-- Activities table: one row per workout/activity
CREATE TABLE IF NOT EXISTS activities (
    activity_id VARCHAR(50) PRIMARY KEY,
    source VARCHAR(20) NOT NULL DEFAULT 'garmin',
    start_time_utc TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    activity_type VARCHAR(50),
    activity_name VARCHAR(255),
    distance_km REAL,
    duration_min REAL,
    moving_time_min REAL,
    avg_hr REAL,
    max_hr REAL,
    elevation_gain_m REAL,
    avg_speed_kmh REAL,
    calories REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sleep table: one row per night's sleep
CREATE TABLE IF NOT EXISTS sleep (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    sleep_start TIMESTAMP,
    sleep_end TIMESTAMP,
    sleep_duration_minutes REAL,
    deep_sleep_minutes REAL,
    light_sleep_minutes REAL,
    rem_sleep_minutes REAL,
    awake_minutes REAL,
    sleep_score REAL,
    avg_hr REAL,
    lowest_hr REAL,
    avg_respiration REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily summary table: one row per day with wellness metrics
CREATE TABLE IF NOT EXISTS daily_summary (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    steps INTEGER,
    calories REAL,
    resting_hr REAL,
    min_hr REAL,
    max_hr REAL,
    stress_avg REAL,
    body_battery_charged REAL,
    body_battery_drained REAL,
    body_battery_highest REAL,
    body_battery_lowest REAL,
    floors_climbed INTEGER,
    distance_km REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_activities_date ON activities(date);
CREATE INDEX IF NOT EXISTS idx_activities_type ON activities(activity_type);
CREATE INDEX IF NOT EXISTS idx_sleep_date ON sleep(date);
CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_summary(date);
"""


def init_schema():
    """Initialize the database schema (create tables if not exist)."""
    with get_cursor() as cursor:
        cursor.execute(SCHEMA_SQL)
    print("✓ Database schema initialized")


def insert_activities(activities: List[Dict[str, Any]]) -> int:
    """
    Insert activities into the database using upsert (ON CONFLICT DO NOTHING).
    
    Args:
        activities: List of activity dictionaries
    
    Returns:
        Number of rows inserted
    """
    if not activities:
        return 0
    
    columns = [
        "activity_id", "source", "start_time_utc", "date", "activity_type",
        "activity_name", "distance_km", "duration_min", "moving_time_min",
        "avg_hr", "max_hr", "elevation_gain_m", "avg_speed_kmh", "calories"
    ]
    
    # Prepare values
    values = []
    for act in activities:
        values.append(tuple(act.get(col) for col in columns))
    
    insert_sql = f"""
        INSERT INTO activities ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (activity_id) DO NOTHING
    """
    
    with get_cursor() as cursor:
        execute_values(cursor, insert_sql, values)
        return cursor.rowcount


def insert_sleep(sleep_records: List[Dict[str, Any]]) -> int:
    """
    Insert sleep records into the database using upsert.
    
    Args:
        sleep_records: List of sleep dictionaries
    
    Returns:
        Number of rows inserted/updated
    """
    if not sleep_records:
        return 0
    
    columns = [
        "date", "sleep_start", "sleep_end", "sleep_duration_minutes",
        "deep_sleep_minutes", "light_sleep_minutes", "rem_sleep_minutes",
        "awake_minutes", "sleep_score", "avg_hr", "lowest_hr", "avg_respiration"
    ]
    
    values = []
    for record in sleep_records:
        values.append(tuple(record.get(col) for col in columns))
    
    # Upsert: update if date already exists
    insert_sql = f"""
        INSERT INTO sleep ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (date) DO UPDATE SET
            sleep_start = EXCLUDED.sleep_start,
            sleep_end = EXCLUDED.sleep_end,
            sleep_duration_minutes = EXCLUDED.sleep_duration_minutes,
            deep_sleep_minutes = EXCLUDED.deep_sleep_minutes,
            light_sleep_minutes = EXCLUDED.light_sleep_minutes,
            rem_sleep_minutes = EXCLUDED.rem_sleep_minutes,
            awake_minutes = EXCLUDED.awake_minutes,
            sleep_score = EXCLUDED.sleep_score,
            avg_hr = EXCLUDED.avg_hr,
            lowest_hr = EXCLUDED.lowest_hr,
            avg_respiration = EXCLUDED.avg_respiration
    """
    
    with get_cursor() as cursor:
        execute_values(cursor, insert_sql, values)
        return cursor.rowcount


def insert_daily_summary(summaries: List[Dict[str, Any]]) -> int:
    """
    Insert daily summary records into the database using upsert.
    
    Args:
        summaries: List of daily summary dictionaries
    
    Returns:
        Number of rows inserted/updated
    """
    if not summaries:
        return 0
    
    columns = [
        "date", "steps", "calories", "resting_hr", "min_hr", "max_hr",
        "stress_avg", "body_battery_charged", "body_battery_drained",
        "body_battery_highest", "body_battery_lowest", "floors_climbed", "distance_km"
    ]
    
    values = []
    for summary in summaries:
        values.append(tuple(summary.get(col) for col in columns))
    
    # Upsert: update if date already exists
    insert_sql = f"""
        INSERT INTO daily_summary ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (date) DO UPDATE SET
            steps = EXCLUDED.steps,
            calories = EXCLUDED.calories,
            resting_hr = EXCLUDED.resting_hr,
            min_hr = EXCLUDED.min_hr,
            max_hr = EXCLUDED.max_hr,
            stress_avg = EXCLUDED.stress_avg,
            body_battery_charged = EXCLUDED.body_battery_charged,
            body_battery_drained = EXCLUDED.body_battery_drained,
            body_battery_highest = EXCLUDED.body_battery_highest,
            body_battery_lowest = EXCLUDED.body_battery_lowest,
            floors_climbed = EXCLUDED.floors_climbed,
            distance_km = EXCLUDED.distance_km
    """
    
    with get_cursor() as cursor:
        execute_values(cursor, insert_sql, values)
        return cursor.rowcount


def get_latest_date(table: str) -> Optional[str]:
    """
    Get the most recent date in a table.
    
    Args:
        table: Table name ('activities', 'sleep', or 'daily_summary')
    
    Returns:
        Latest date as ISO string, or None if table is empty
    """
    with get_cursor() as cursor:
        cursor.execute(f"SELECT MAX(date) FROM {table}")
        result = cursor.fetchone()
        if result and result[0]:
            return result[0].isoformat()
        return None


def get_existing_activity_ids() -> set:
    """Get all existing activity IDs from the database."""
    with get_cursor() as cursor:
        cursor.execute("SELECT activity_id FROM activities")
        return {row[0] for row in cursor.fetchall()}


def execute_query(sql: str) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a DataFrame.
    
    Args:
        sql: SQL query string
    
    Returns:
        Query results as pandas DataFrame
    """
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def get_table_stats() -> Dict[str, Dict[str, Any]]:
    """
    Get statistics for all tables (count, date range).
    
    Returns:
        Dictionary with stats for each table
    """
    stats = {}
    tables = ['activities', 'sleep', 'daily_summary']
    
    with get_cursor() as cursor:
        for table in tables:
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as count,
                    MIN(date) as min_date,
                    MAX(date) as max_date
                FROM {table}
            """)
            result = cursor.fetchone()
            if result:
                stats[table] = {
                    'count': result[0],
                    'min_date': result[1],
                    'max_date': result[2]
                }
    
    return stats


def check_connection() -> bool:
    """Check if database connection is working."""
    try:
        with get_cursor() as cursor:
            cursor.execute("SELECT 1")
            return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False

