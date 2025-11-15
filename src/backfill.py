"""
Backfill script to fetch all historical Garmin data into PostgreSQL.
"""
import sys
from datetime import date, datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from .garmin_client import GarminClient
from .database import (
    init_schema,
    insert_activities,
    insert_sleep,
    insert_daily_summary,
    check_connection
)


def normalize_activities(activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize Garmin activities JSON into flat dictionaries.
    """
    rows = []
    for act in activities:
        try:
            # Parse start time
            start_time_str = act.get("startTimeLocal") or act.get("beginTimestamp")
            if start_time_str:
                if isinstance(start_time_str, str):
                    # Handle various datetime formats
                    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]:
                        try:
                            start_time = datetime.strptime(start_time_str.split('.')[0], fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        # Try pandas parsing as fallback
                        start_time = pd.to_datetime(start_time_str)
                else:
                    start_time = pd.to_datetime(start_time_str)
            else:
                continue  # Skip activities without timestamps
            
            row = {
                "activity_id": str(act.get("activityId")),
                "source": "garmin",
                "start_time_utc": start_time,
                "date": start_time.date(),
                "activity_type": act.get("activityType", {}).get("typeKey") if isinstance(act.get("activityType"), dict) else act.get("activityType"),
                "activity_name": act.get("activityName"),
                "distance_km": act.get("distance", 0) / 1000 if act.get("distance") else None,  # meters to km
                "duration_min": act.get("duration", 0) / 60 if act.get("duration") else None,  # seconds to minutes
                "moving_time_min": act.get("movingDuration", 0) / 60 if act.get("movingDuration") else None,
                "avg_hr": act.get("averageHR") or act.get("avgHr"),
                "max_hr": act.get("maxHR") or act.get("maxHr"),
                "elevation_gain_m": act.get("elevationGain"),
                "avg_speed_kmh": act.get("averageSpeed") * 3.6 if act.get("averageSpeed") else None,  # m/s to km/h
                "calories": act.get("calories"),
            }
            rows.append(row)
        except Exception as e:
            print(f"Warning: Failed to normalize activity {act.get('activityId')}: {e}")
            continue
    
    return rows


def normalize_sleep(sleep_data: Dict[str, Any], query_date: date) -> List[Dict[str, Any]]:
    """
    Normalize Garmin sleep data into flat dictionaries.
    """
    if not sleep_data or "dailySleepDTO" not in sleep_data:
        return []
    
    daily = sleep_data["dailySleepDTO"]
    
    try:
        # Parse timestamps
        sleep_start_str = daily.get("sleepStartTimestampLocal")
        sleep_end_str = daily.get("sleepEndTimestampLocal")
        
        if sleep_start_str and sleep_end_str:
            sleep_start = pd.to_datetime(sleep_start_str)
            sleep_end = pd.to_datetime(sleep_end_str)
        else:
            return []
        
        # Extract sleep stages
        sleep_levels = daily.get("sleepLevels", {})
        deep_sleep_sec = 0
        light_sleep_sec = 0
        rem_sleep_sec = 0
        awake_sec = 0
        
        for level_data in sleep_levels.values():
            if isinstance(level_data, list):
                for entry in level_data:
                    duration = entry.get("seconds", 0)
                    if "deep" in str(entry).lower():
                        deep_sleep_sec += duration
                    elif "light" in str(entry).lower():
                        light_sleep_sec += duration
                    elif "rem" in str(entry).lower():
                        rem_sleep_sec += duration
                    elif "awake" in str(entry).lower():
                        awake_sec += duration
        
        row = {
            "date": query_date,
            "sleep_start": sleep_start,
            "sleep_end": sleep_end,
            "sleep_duration_minutes": daily.get("sleepTimeSeconds", 0) / 60,
            "deep_sleep_minutes": deep_sleep_sec / 60,
            "light_sleep_minutes": light_sleep_sec / 60,
            "rem_sleep_minutes": rem_sleep_sec / 60,
            "awake_minutes": awake_sec / 60,
            "sleep_score": daily.get("sleepScores", {}).get("overall", {}).get("value") if daily.get("sleepScores") else None,
            "avg_hr": daily.get("averageHeartRate"),
            "lowest_hr": daily.get("lowestHeartRate"),
            "avg_respiration": daily.get("averageRespirationValue"),
        }
        return [row]
    except Exception as e:
        print(f"Warning: Failed to normalize sleep data for {query_date}: {e}")
        return []


def normalize_daily_stats(stats: Dict[str, Any], query_date: date) -> List[Dict[str, Any]]:
    """
    Normalize Garmin daily stats/summary into flat dictionaries.
    """
    if not stats:
        return []
    
    try:
        row = {
            "date": query_date,
            "steps": stats.get("totalSteps") or stats.get("steps"),
            "calories": stats.get("activeKilocalories") or stats.get("calories"),
            "resting_hr": stats.get("restingHeartRate"),
            "min_hr": stats.get("minHeartRate"),
            "max_hr": stats.get("maxHeartRate"),
            "stress_avg": stats.get("averageStressLevel"),
            "body_battery_charged": stats.get("bodyBatteryChargedValue"),
            "body_battery_drained": stats.get("bodyBatteryDrainedValue"),
            "body_battery_highest": stats.get("bodyBatteryHighestValue"),
            "body_battery_lowest": stats.get("bodyBatteryLowestValue"),
            "floors_climbed": stats.get("floorsAscended"),
            "distance_km": stats.get("totalDistanceMeters", 0) / 1000 if stats.get("totalDistanceMeters") else None,
        }
        return [row]
    except Exception as e:
        print(f"Warning: Failed to normalize daily stats for {query_date}: {e}")
        return []


def backfill_activities():
    """Fetch all activities and write to database."""
    print("\n=== Backfilling Activities ===")
    gc = GarminClient()
    activities = gc.get_all_activities()
    
    if not activities:
        print("No activities found")
        return
    
    rows = normalize_activities(activities)
    if not rows:
        print("No activities to write after normalization")
        return
    
    count = insert_activities(rows)
    print(f"✓ Inserted {count} activities (total normalized: {len(rows)})")


def backfill_sleep(start_date: date, end_date: date):
    """Fetch sleep data for a date range and write to database."""
    print(f"\n=== Backfilling Sleep ({start_date} to {end_date}) ===")
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
        print("No sleep data found")
        return
    
    count = insert_sleep(all_rows)
    print(f"✓ Inserted {count} sleep records")


def backfill_daily_summary(start_date: date, end_date: date):
    """Fetch daily summary/stats for a date range and write to database."""
    print(f"\n=== Backfilling Daily Summary ({start_date} to {end_date}) ===")
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
        print("No daily summary data found")
        return
    
    count = insert_daily_summary(all_rows)
    print(f"✓ Inserted {count} daily summary records")


def main():
    """Main backfill orchestration."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Backfill Garmin data to PostgreSQL database")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD). Defaults to 3 years ago."
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD). Defaults to today."
    )
    parser.add_argument(
        "--entities",
        nargs="+",
        default=["activities", "sleep", "daily_summary"],
        choices=["activities", "sleep", "daily_summary"],
        help="Which entities to backfill (default: all)"
    )
    
    args = parser.parse_args()
    
    # Parse dates
    end_date = date.fromisoformat(args.end_date) if args.end_date else date.today()
    start_date = date.fromisoformat(args.start_date) if args.start_date else (end_date - timedelta(days=3*365))
    
    print(f"Backfilling from {start_date} to {end_date}")
    print(f"Entities: {', '.join(args.entities)}")
    
    # Check database connection
    print("\nChecking database connection...")
    if not check_connection():
        print("✗ Cannot connect to PostgreSQL. Make sure Docker is running:")
        print("  docker-compose up -d")
        sys.exit(1)
    print("✓ Database connection OK")
    
    # Initialize schema
    init_schema()
    
    # Run backfills
    try:
        if "activities" in args.entities:
            backfill_activities()
        
        if "sleep" in args.entities:
            backfill_sleep(start_date, end_date)
        
        if "daily_summary" in args.entities:
            backfill_daily_summary(start_date, end_date)
        
        print("\n✓ Backfill complete!")
    except KeyboardInterrupt:
        print("\n\nBackfill interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Backfill failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
