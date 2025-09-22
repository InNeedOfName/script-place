"""
Small script that will check each NHL Teams schedule
And print out the amount of games that are watchable in different timezones
The window during which you are willing to watch games is defined in the _precompute_time_windows method
"""
import requests
import json
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Tuple, Dict
import concurrent.futures
from functools import lru_cache
import threading


class NHLScheduleAnalyzer:
    """
    NHL Schedule analyzer class
    """
    # Using a dict to speed up lookup
    NHL_TEAMS = {
        1: 'NJD', 2: 'NYI', 3: 'NYR', 4: 'PHI', 5: 'PIT', 6: 'BOS', 7: 'BUF',
        8: 'MTL', 9: 'OTT', 10: 'TOR', 13: 'FLA', 14: 'TBL', 12: 'CAR', 15: 'WSH',
        16: 'CHI', 17: 'DET', 18: 'NSH', 19: 'STL', 20: 'CGY', 21: 'COL', 22: 'EDM',
        23: 'VAN', 24: 'ANA', 25: 'DAL', 26: 'LAK', 28: 'SJS', 29: 'CBJ', 30: 'MIN',
        52: 'WPG', 54: 'VGK', 55: 'SEA', 59: 'UTA'
    }

    # Pre-converted viewing windows
    VIEWING_WINDOWS_TIME = {}

    def __init__(self, max_workers: int = 10):
        self.team_list = list(self.NHL_TEAMS.values())
        self.max_workers = max_workers
        self._session = requests.Session()
        self._cache = {}
        self._cache_lock = threading.Lock()

        # Pre-compute time objects and precache current date to avoid repetition
        self._precompute_time_windows()
        self.current_date = datetime.now().date()

    def _precompute_time_windows(self):
        """Convert time strings to time objects for faster comparisons."""
        viewing_windows_str = {
            "Monday": ("15:00:00", "22:30:00"),
            "Tuesday": ("15:00:00", "22:30:00"),
            "Wednesday": ("15:00:00", "22:30:00"),
            "Thursday": ("15:00:00", "22:30:00"),
            "Friday": ("15:00:00", "23:30:00"),
            "Saturday": ("09:00:00", "23:30:00"),
            "Sunday": ("09:00:00", "22:00:00"),
        }

        for day, (start_str, end_str) in viewing_windows_str.items():
            start_time = datetime.strptime(start_str, "%H:%M:%S").time()
            end_time = datetime.strptime(end_str, "%H:%M:%S").time()
            self.VIEWING_WINDOWS_TIME[day] = (start_time, end_time)

    @lru_cache(maxsize=128)
    def fetch_api_data(self, url: str) -> str:
        """
        Retrieve Data from API
        Returns json string
        """
        try:
            response = self._session.get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"API request failed for {url}: {e}")
            return "{}"

    def get_team_schedule_cached(self, team: str) -> List[Tuple[datetime, str]]:
        """
        Get team schedule with the preset caching
        Returns datetime object
        """
        with self._cache_lock:
            if team in self._cache:
                return self._cache[team]

        url = f"https://api-web.nhle.com/v1/club-schedule-season/{team}/now"
        json_data = self.fetch_api_data(url)
        schedule_data = json.loads(json_data)

        games = schedule_data.get('games', [])
        parsed_games = []

        # Batch process all games at once
        for game in games:
            # Only compute regular season games
            if game.get('gameType') == 2:
                start_time = game.get('startTimeUTC', '')
                if start_time:
                    try:
                        dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        if dt.date() > self.current_date:
                            parsed_games.append((dt, start_time.split('T')[0]))
                    except (ValueError, AttributeError):
                        continue

        # Caching the result
        with self._cache_lock:
            self._cache[team] = parsed_games

        return parsed_games

    def analyze_team_viewing_time(self, team: str, timezone_offset: int) -> Tuple[str, int, List[str]]:
        """
        Analyze the gamedates
        returns a team with the viewable ganes per date
        """
        schedule = self.get_team_schedule_cached(team)

        viewable_games = 0
        game_dates = []

        # Pre-calculate timezone offset
        tz_delta = timedelta(hours=timezone_offset)

        for utc_datetime, date_str in schedule:
            local_time = utc_datetime + tz_delta # apply offset due to timezones
            weekday = local_time.strftime("%A")
            local_time_only = local_time.time()

            # Using the faster look up due to cached data
            if weekday in self.VIEWING_WINDOWS_TIME:
                start_time, end_time = self.VIEWING_WINDOWS_TIME[weekday]
                if start_time <= local_time_only <= end_time:
                    viewable_games += 1
                    game_dates.append(date_str)

        return (team, viewable_games, game_dates)

    def rank_teams_by_viewing_availability_parallel(self, timezone_offset: int) -> List[List]:
        """
        Processing the data in paralell to speed up the process
        returns sorted list of teams
        """
        def process_team(team):
            return self.analyze_team_viewing_time(team, timezone_offset)

        team_rankings = []

        # Use ThreadPoolExecutor for I/O-bound API calls
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_team = {executor.submit(process_team, team): team for team in self.team_list}

            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_team):
                try:
                    result = future.result()
                    team_rankings.append([result[0], result[1], result[2]])
                except Exception as exc:
                    team = future_to_team[future]
                    print(f'Team {team} generated an exception: {exc}')

        # Sort by game count in descending order
        return sorted(team_rankings, key=lambda x: x[1], reverse=True)

    def generate_timezone_analysis_parallel(self, top_n_teams: int = 5) -> Dict[str, Dict[str, int]]:
        """
        parallel processing across all timezones.
        """
        timezone_analysis = defaultdict(lambda: defaultdict(int))
        timezone_range = list(range(-12, 13))  # UTC-12 to UTC+12

        def process_timezone(tz_offset):
            team_rankings = self.rank_teams_by_viewing_availability_parallel(tz_offset)
            result = {}
            for i in range(min(top_n_teams, len(team_rankings))):
                team_name, game_count, _ = team_rankings[i]
                result[team_name] = game_count
            return tz_offset, result

        # Process multiple timezones in parallel (CPU-bound after caching)
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(timezone_range))) as executor:
            future_to_tz = {executor.submit(process_timezone, tz): tz for tz in timezone_range}

            for future in concurrent.futures.as_completed(future_to_tz):
                try:
                    tz_offset, team_data = future.result()
                    timezone_key = f"UTC{tz_offset:+d}"
                    timezone_analysis[timezone_key] = team_data
                except Exception as exc:
                    tz = future_to_tz[future]
                    print(f'Timezone {tz} generated an exception: {exc}')

        return dict(timezone_analysis)


    def print_analysis_summary(self, analysis_data: Dict[str, Dict[str, int]]) -> None:
        """Print a formatted summary"""

        print("NHL Viewing Schedule Summary \n")
        # Sort timezones by their offset instead of alphabetically
        def timezone_sort_key(timezone_str):
            # Extract the numerical offset from strings like "UTC-12", "UTC+5", etc.
            if timezone_str.startswith("UTC"):
                offset_str = timezone_str[3:]  # Remove "UTC" prefix
                return int(offset_str)
            return 0  # Fallback for unexpected format

        sorted_timezones = sorted(analysis_data.keys(), key=timezone_sort_key)

        for timezone in sorted_timezones:
            teams = analysis_data[timezone]
            print(f"\n{timezone}:")
            for team, count in teams.items():
                print(f"  {team}: {count} viewable games")

    def warm_cache(self):
        """Preload all team schedules"""
        def load_team_schedule(team):
            self.get_team_schedule_cached(team)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            list(executor.map(load_team_schedule, self.team_list))


def main():
    """
    Executing the script
    """
    analyzer = NHLScheduleAnalyzer(max_workers=12)
    analyzer.warm_cache()

    analysis_results = analyzer.generate_timezone_analysis_parallel(top_n_teams=5)

    # Display results
    analyzer.print_analysis_summary(analysis_results)

    return analysis_results


if __name__ == "__main__":
    results = main()