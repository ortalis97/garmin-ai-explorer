"""
Wrapper around garminconnect library with session persistence.
"""
import os
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import date, datetime, timedelta
from garminconnect import Garmin
import garth


class GarminClient:
    """
    Thin wrapper around Garmin API with session persistence.
    """
    
    def __init__(
        self, 
        email: Optional[str] = None, 
        password: Optional[str] = None,
        session_dir: Optional[Path] = None
    ):
        self.email = email or os.environ.get("GARMIN_EMAIL")
        self.password = password or os.environ.get("GARMIN_PASSWORD")
        
        if not self.email or not self.password:
            raise ValueError("GARMIN_EMAIL and GARMIN_PASSWORD must be set")
        
        # Session persistence directory
        self.session_dir = session_dir or Path.home() / ".garmin_session"
        self.session_dir.mkdir(exist_ok=True)
        
        self.api: Optional[Garmin] = None
        self._login()
    
    def _login(self):
        """Login to Garmin Connect, using cached session if available."""
        print("Logging into Garmin Connect...")
        
        # Try to load existing session from garth
        try:
            garth.resume(str(self.session_dir))
            # Create Garmin instance (it will use the resumed garth session)
            self.api = Garmin()
            # Test if session is still valid by making a simple API call
            try:
                self.api.get_heart_rates(date.today().isoformat())
                print("✓ Loaded existing session")
                return
            except:
                # Session expired, need fresh login
                print("Cached session expired, logging in again...")
        except Exception as e:
            print(f"No valid cached session, performing fresh login...")
        
        # Fresh login
        try:
            # Create Garmin instance with credentials
            self.api = Garmin(self.email, self.password)
            # Perform login
            self.api.login()
            # Save the garth session for next time
            garth.save(str(self.session_dir))
            print("✓ Logged in successfully")
        except Exception as e:
            print(f"Login failed: {e}")
            raise
    
    def get_activities(self, start: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get activities with pagination.
        
        Args:
            start: Starting index
            limit: Number of activities to fetch
        
        Returns:
            List of activity dictionaries
        """
        return self.api.get_activities(start, limit)
    
    def get_all_activities(self, max_activities: int = 10000) -> List[Dict[str, Any]]:
        """
        Fetch all activities by paginating through the API.
        
        Args:
            max_activities: Safety limit to prevent infinite loops
        
        Returns:
            List of all activity dictionaries
        """
        all_activities = []
        batch_size = 100
        start = 0
        
        print("Fetching activities...")
        while start < max_activities:
            batch = self.get_activities(start, batch_size)
            if not batch:
                break
            all_activities.extend(batch)
            print(f"  Fetched {len(all_activities)} activities...")
            start += batch_size
            
            # Stop if we got fewer than requested (no more data)
            if len(batch) < batch_size:
                break
        
        print(f"✓ Total activities fetched: {len(all_activities)}")
        return all_activities
    
    def get_sleep_data(self, date_str: str) -> Dict[str, Any]:
        """
        Get sleep data for a specific date.
        
        Args:
            date_str: Date in YYYY-MM-DD format
        
        Returns:
            Sleep data dictionary
        """
        return self.api.get_sleep_data(date_str)
    
    def get_daily_stats(self, date_str: str) -> Dict[str, Any]:
        """
        Get daily stats/summary for a specific date.
        
        Args:
            date_str: Date in YYYY-MM-DD format
        
        Returns:
            Daily stats dictionary
        """
        return self.api.get_stats(date_str)
    
    def get_heart_rates(self, date_str: str) -> Dict[str, Any]:
        """
        Get heart rate data for a specific date.
        
        Args:
            date_str: Date in YYYY-MM-DD format
        
        Returns:
            Heart rate data dictionary
        """
        return self.api.get_heart_rates(date_str)
