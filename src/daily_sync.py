"""
Daily sync script for incremental Garmin data updates.
Automatically detects the latest date in the data lake and syncs only new data.
"""
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from .garmin_client import GarminClient
from .backfill import (
    normalize_activities, 
    normalize_sleep, 
    normalize_daily_stats,
    write_partitioned_parquet,
    DATA_LAKE
)


def get_latest_date_in_lake(entity: str) -> Optional[date]:
    """
    Query the data lake to find the most recent date for a given entity.
    
    Args:
        entity: One of 'activities', 'sleep', 'daily_summary'
    
    Returns:
        Latest date found, or None if no data exists
    """
    entity_path = DATA_LAKE / entity / "year=*" / "month=*" / "*.parquet"
    
    # Check if any parquet files exist
    if not list(DATA_LAKE.glob(f"{entity}/year=*/month=*/*.parquet")):
        return None
    
    try:
        con = duckdb.connect(database=":memory:")
        
        # Different date column based on entity
        date_col = "date"
        if entity == "activities":
            date_col = "date"  # activities uses 'date' column
        
        query = f"SELECT MAX({date_col}) as max_date FROM read_parquet('{entity_path}')"
        result = con.execute(query).fetchone()
        
        if result and result[0]:
            # Convert to date object
            if isinstance(result[0], date):
                return result[0]
            elif isinstance(result[0], datetime):
                return result[0].date()
            else:
                return pd.to_datetime(result[0]).date()
        return None
    except Exception as e:
        print(f"Warning: Could not determine latest date for {entity}: {e}")
        return None


def deduplicate_activities(new_activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove activities that already exist in the data lake based on activity_id.
    
    Args:
        new_activities: List of normalized activity dictionaries
    
    Returns:
        Deduplicated list of activities
    """
    if not new_activities:
        return []
    
    activities_path = DATA_LAKE / "activities" / "year=*" / "month=*" / "*.parquet"
    
    # Check if any existing data
    if not list(DATA_LAKE.glob("activities/year=*/month=*/*.parquet")):
        print(f"  No existing activities data, all {len(new_activities)} activities are new")
        return new_activities
    
    try:
        # Get existing activity IDs
        con = duckdb.connect(database=":memory:")
        existing_ids_query = f"SELECT DISTINCT activity_id FROM read_parquet('{activities_path}')"
        existing_ids = set(con.execute(existing_ids_query).df()['activity_id'].tolist())
        
        # Filter out existing activities
        new_activity_ids = {act['activity_id'] for act in new_activities}
        truly_new = [act for act in new_activities if act['activity_id'] not in existing_ids]
        
        duplicates_count = len(new_activities) - len(truly_new)
        if duplicates_count > 0:
            print(f"  Filtered out {duplicates_count} duplicate activities")
        print(f"  {len(truly_new)} new activities to add")
        
        return truly_new
    except Exception as e:
        print(f"Warning: Could not deduplicate activities: {e}")
        print(f"  Proceeding with all {len(new_activities)} activities")
        return new_activities


def sync_activities(lookback_days: int = 30) -> int:
    """
    Sync activities, deduplicating against existing data.
    
    Args:
        lookback_days: How many days back to fetch (to catch any updates)
    
    Returns:
        Number of new activities added
    """
    print(f"\n=== Syncing Activities (lookback: {lookback_days} days) ===")
    
    gc = GarminClient()
    
    # Fetch recent activities
    # Note: Garmin API doesn't support date filtering well, so we fetch a batch
    # and filter by date on our side
    activities = gc.get_activities(start=0, limit=lookback_days * 3)  # Overestimate
    
    if not activities:
        print("No activities found from API")
        return 0
    
    # Normalize activities
    rows = normalize_activities(activities)
    
    if not rows:
        print("No activities to process after normalization")
        return 0
    
    # Filter to only activities within lookback period
    cutoff_date = date.today() - timedelta(days=lookback_days)
    rows = [r for r in rows if r['date'] >= cutoff_date]
    print(f"Found {len(rows)} activities in the last {lookback_days} days")
    
    # Deduplicate against existing data
    rows = deduplicate_activities(rows)
    
    if not rows:
        print("✓ No new activities to add")
        return 0
    
    # Write to data lake
    df = pd.DataFrame(rows)
    write_partitioned_parquet(df, "activities", date_column="date")
    print(f"✓ Added {len(df)} new activities")
    
    return len(df)


def sync_sleep(start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
    """
    Sync sleep data incrementally.
    
    Args:
        start_date: Start date (defaults to latest date in lake + 1 day)
        end_date: End date (defaults to today)
    
    Returns:
        Number of new sleep records added
    """
    # Determine date range
    if end_date is None:
        end_date = date.today()
    
    if start_date is None:
        latest = get_latest_date_in_lake("sleep")
        if latest:
            start_date = latest + timedelta(days=1)
        else:
            # No existing data, fetch last 7 days
            start_date = end_date - timedelta(days=7)
    
    # Skip if no new dates to fetch
    if start_date > end_date:
        print(f"\n=== Sleep: Already up to date (latest: {start_date - timedelta(days=1)}) ===")
        return 0
    
    print(f"\n=== Syncing Sleep ({start_date} to {end_date}) ===")
    
    gc = GarminClient()
    cur = start_date
    all_rows = []
    
    while cur <= end_date:
        try:
            sleep_data = gc.get_sleep_data(cur.isoformat())
            rows = normalize_sleep(sleep_data, cur)
            all_rows.extend(rows)
            
            if rows:
                print(f"  ✓ {cur}")
            else:
                print(f"  - {cur} (no data)")
        except Exception as e:
            print(f"  ✗ {cur}: {e}")
        
        cur += timedelta(days=1)
    
    if not all_rows:
        print("✓ No new sleep data found")
        return 0
    
    df = pd.DataFrame(all_rows)
    write_partitioned_parquet(df, "sleep", date_column="date")
    print(f"✓ Added {len(df)} sleep records")
    
    return len(df)


def sync_daily_summary(start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
    """
    Sync daily summary data incrementally.
    
    Args:
        start_date: Start date (defaults to latest date in lake + 1 day)
        end_date: End date (defaults to yesterday, since today is incomplete)
    
    Returns:
        Number of new daily summary records added
    """
    # Determine date range
    if end_date is None:
        # Use yesterday since today's data might be incomplete
        end_date = date.today() - timedelta(days=1)
    
    if start_date is None:
        latest = get_latest_date_in_lake("daily_summary")
        if latest:
            start_date = latest + timedelta(days=1)
        else:
            # No existing data, fetch last 7 days
            start_date = end_date - timedelta(days=7)
    
    # Skip if no new dates to fetch
    if start_date > end_date:
        print(f"\n=== Daily Summary: Already up to date (latest: {start_date - timedelta(days=1)}) ===")
        return 0
    
    print(f"\n=== Syncing Daily Summary ({start_date} to {end_date}) ===")
    
    gc = GarminClient()
    cur = start_date
    all_rows = []
    
    while cur <= end_date:
        try:
            stats = gc.get_daily_stats(cur.isoformat())
            rows = normalize_daily_stats(stats, cur)
            all_rows.extend(rows)
            
            if rows:
                print(f"  ✓ {cur}")
            else:
                print(f"  - {cur} (no data)")
        except Exception as e:
            print(f"  ✗ {cur}: {e}")
        
        cur += timedelta(days=1)
    
    if not all_rows:
        print("✓ No new daily summary data found")
        return 0
    
    df = pd.DataFrame(all_rows)
    write_partitioned_parquet(df, "daily_summary", date_column="date")
    print(f"✓ Added {len(df)} daily summary records")
    
    return len(df)


def main():
    """Main daily sync orchestration."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Incrementally sync new Garmin data to the data lake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-detect latest dates and sync only new data
  python -m src.daily_sync
  
  # Sync specific entities only
  python -m src.daily_sync --entities sleep daily_summary
  
  # Force sync from a specific date
  python -m src.daily_sync --start-date 2025-11-10
  
  # Sync with custom activity lookback period
  python -m src.daily_sync --activity-lookback 60
        """
    )
    parser.add_argument(
        "--entities",
        nargs="+",
        default=["activities", "sleep", "daily_summary"],
        choices=["activities", "sleep", "daily_summary"],
        help="Which entities to sync (default: all)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD). Overrides auto-detection."
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD). Defaults to today."
    )
    parser.add_argument(
        "--activity-lookback",
        type=int,
        default=30,
        help="Days to look back for activities (default: 30)"
    )
    
    args = parser.parse_args()
    
    # Parse date overrides
    start_date_override = date.fromisoformat(args.start_date) if args.start_date else None
    end_date_override = date.fromisoformat(args.end_date) if args.end_date else None
    
    print("=" * 80)
    print("GARMIN DAILY SYNC")
    print("=" * 80)
    print(f"Entities: {', '.join(args.entities)}")
    print(f"Data lake: {DATA_LAKE}")
    
    # Show what we're about to sync
    if start_date_override:
        print(f"Start date: {start_date_override} (manual override)")
    else:
        print(f"Start date: Auto-detect from data lake")
    
    if end_date_override:
        print(f"End date: {end_date_override}")
    else:
        print(f"End date: Today (or yesterday for daily_summary)")
    
    # Run syncs
    total_new = 0
    
    try:
        if "activities" in args.entities:
            total_new += sync_activities(lookback_days=args.activity_lookback)
        
        if "sleep" in args.entities:
            total_new += sync_sleep(start_date_override, end_date_override)
        
        if "daily_summary" in args.entities:
            total_new += sync_daily_summary(start_date_override, end_date_override)
        
        print("\n" + "=" * 80)
        print(f"✓ Sync complete! Added {total_new} total new records")
        print("=" * 80)
    
    except KeyboardInterrupt:
        print("\n\nSync interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Sync failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

