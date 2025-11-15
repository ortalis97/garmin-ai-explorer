"""
Airflow-callable task functions for Garmin data sync.
These functions are designed to be called from Airflow DAGs without CLI argument parsing.
"""
import sys
from datetime import date
from typing import Optional

from .database import init_schema, check_connection
from .daily_sync import sync_activities, sync_sleep, sync_daily_summary


def run_daily_sync(
    entities: Optional[list] = None,
    activity_lookback_days: int = 30
) -> dict:
    """
    Run the daily Garmin data sync.
    
    This is the main entry point for Airflow tasks. It handles:
    - Database connection check
    - Schema initialization
    - Syncing all entities with smart delta detection
    
    Args:
        entities: List of entities to sync. Defaults to all: 
                  ["activities", "sleep", "daily_summary"]
        activity_lookback_days: Minimum lookback for activities (extended 
                                automatically if last sync was earlier)
    
    Returns:
        Dictionary with sync results:
        {
            "success": bool,
            "total_new_records": int,
            "activities": int,
            "sleep": int,
            "daily_summary": int,
            "error": str or None
        }
    """
    if entities is None:
        entities = ["activities", "sleep", "daily_summary"]
    
    result = {
        "success": False,
        "total_new_records": 0,
        "activities": 0,
        "sleep": 0,
        "daily_summary": 0,
        "error": None
    }
    
    print("=" * 80)
    print("GARMIN DAILY SYNC (Airflow Task)")
    print("=" * 80)
    print(f"Entities: {', '.join(entities)}")
    print(f"Date: {date.today()}")
    
    # Check database connection
    print("\nChecking database connection...")
    if not check_connection():
        error_msg = "Cannot connect to PostgreSQL database"
        print(f"✗ {error_msg}")
        result["error"] = error_msg
        return result
    print("✓ Database connection OK")
    
    # Initialize schema (idempotent)
    init_schema()
    
    try:
        # Sync activities
        if "activities" in entities:
            result["activities"] = sync_activities(lookback_days=activity_lookback_days)
            result["total_new_records"] += result["activities"]
        
        # Sync sleep
        if "sleep" in entities:
            result["sleep"] = sync_sleep()
            result["total_new_records"] += result["sleep"]
        
        # Sync daily summary
        if "daily_summary" in entities:
            result["daily_summary"] = sync_daily_summary()
            result["total_new_records"] += result["daily_summary"]
        
        result["success"] = True
        
        print("\n" + "=" * 80)
        print(f"✓ Sync complete! Added {result['total_new_records']} total new records")
        print("=" * 80)
        
    except Exception as e:
        import traceback
        error_msg = f"Sync failed: {str(e)}"
        print(f"\n✗ {error_msg}")
        traceback.print_exc()
        result["error"] = error_msg
    
    return result


def run_full_backfill(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    entities: Optional[list] = None
) -> dict:
    """
    Run a full backfill of historical Garmin data.
    
    This is useful for initial setup or recovering from data loss.
    Should be triggered manually, not on a schedule.
    
    Args:
        start_date: Start date (YYYY-MM-DD). Defaults to 3 years ago.
        end_date: End date (YYYY-MM-DD). Defaults to today.
        entities: List of entities to backfill.
    
    Returns:
        Dictionary with backfill results
    """
    from datetime import timedelta
    from .backfill import (
        backfill_activities,
        backfill_sleep,
        backfill_daily_summary
    )
    
    if entities is None:
        entities = ["activities", "sleep", "daily_summary"]
    
    # Parse dates
    end_dt = date.fromisoformat(end_date) if end_date else date.today()
    start_dt = date.fromisoformat(start_date) if start_date else (end_dt - timedelta(days=3*365))
    
    result = {
        "success": False,
        "start_date": start_dt.isoformat(),
        "end_date": end_dt.isoformat(),
        "entities": entities,
        "error": None
    }
    
    print("=" * 80)
    print("GARMIN FULL BACKFILL (Airflow Task)")
    print("=" * 80)
    print(f"Date range: {start_dt} to {end_dt}")
    print(f"Entities: {', '.join(entities)}")
    
    # Check database connection
    if not check_connection():
        result["error"] = "Cannot connect to PostgreSQL database"
        return result
    
    init_schema()
    
    try:
        if "activities" in entities:
            backfill_activities()
        
        if "sleep" in entities:
            backfill_sleep(start_dt, end_dt)
        
        if "daily_summary" in entities:
            backfill_daily_summary(start_dt, end_dt)
        
        result["success"] = True
        print("\n✓ Backfill complete!")
        
    except Exception as e:
        import traceback
        result["error"] = str(e)
        traceback.print_exc()
    
    return result

