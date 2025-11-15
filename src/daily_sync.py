"""
Daily sync script for incremental Garmin data updates.
Automatically detects the latest date in the database and syncs only new data.
"""
import sys
from datetime import date, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from .garmin_client import GarminClient
from .backfill import (
    normalize_activities, 
    normalize_sleep, 
    normalize_daily_stats,
)
from .database import (
    init_schema,
    insert_activities,
    insert_sleep,
    insert_daily_summary,
    get_latest_date,
    get_existing_activity_ids,
    check_connection
)


def deduplicate_activities(new_activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove activities that already exist in the database based on activity_id.
    
    Args:
        new_activities: List of normalized activity dictionaries
    
    Returns:
        Deduplicated list of activities
    """
    if not new_activities:
        return []
    
    try:
        # Get existing activity IDs from database
        existing_ids = get_existing_activity_ids()
        
        if not existing_ids:
            print(f"  No existing activities in database, all {len(new_activities)} activities are new")
            return new_activities
        
        # Filter out existing activities
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
    
    Uses smart lookback: if the last sync was more than lookback_days ago,
    extends the window to cover from the last synced activity.
    
    Args:
        lookback_days: Minimum days to look back (default 30, extended if needed)
    
    Returns:
        Number of new activities added
    """
    # Smart lookback: extend if last sync was before the default window
    latest = get_latest_date("activities")
    if latest:
        days_since_last = (date.today() - date.fromisoformat(latest)).days
        if days_since_last > lookback_days:
            print(f"Last activity sync was {days_since_last} days ago, extending lookback")
            lookback_days = days_since_last + 1  # +1 to include the last synced day
    
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
    
    # Write to database
    count = insert_activities(rows)
    print(f"✓ Added {count} new activities")
    
    return count


def sync_sleep(start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
    """
    Sync sleep data incrementally.
    
    Args:
        start_date: Start date (defaults to latest date in database + 1 day)
        end_date: End date (defaults to today)
    
    Returns:
        Number of new sleep records added
    """
    # Determine date range
    if end_date is None:
        end_date = date.today()
    
    if start_date is None:
        latest = get_latest_date("sleep")
        if latest:
            start_date = date.fromisoformat(latest) + timedelta(days=1)
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
    
    count = insert_sleep(all_rows)
    print(f"✓ Added {count} sleep records")
    
    return count


def sync_daily_summary(start_date: Optional[date] = None, end_date: Optional[date] = None) -> int:
    """
    Sync daily summary data incrementally.
    
    Args:
        start_date: Start date (defaults to latest date in database + 1 day)
        end_date: End date (defaults to yesterday, since today is incomplete)
    
    Returns:
        Number of new daily summary records added
    """
    # Determine date range
    if end_date is None:
        # Use yesterday since today's data might be incomplete
        end_date = date.today() - timedelta(days=1)
    
    if start_date is None:
        latest = get_latest_date("daily_summary")
        if latest:
            start_date = date.fromisoformat(latest) + timedelta(days=1)
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
    
    count = insert_daily_summary(all_rows)
    print(f"✓ Added {count} daily summary records")
    
    return count


def main():
    """Main daily sync orchestration."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Incrementally sync new Garmin data to the PostgreSQL database",
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
    
    # Check database connection
    print("\nChecking database connection...")
    if not check_connection():
        print("✗ Cannot connect to PostgreSQL. Make sure Docker is running:")
        print("  docker-compose up -d")
        sys.exit(1)
    print("✓ Database connection OK")
    
    # Initialize schema (idempotent)
    init_schema()
    
    # Show what we're about to sync
    if start_date_override:
        print(f"Start date: {start_date_override} (manual override)")
    else:
        print(f"Start date: Auto-detect from database")
    
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
