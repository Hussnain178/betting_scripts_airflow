import scrapy
import json
from pymongo import MongoClient, UpdateOne
from scrapy.crawler import CrawlerProcess
from helper import (
    setup_scraper_logger, log_scraper_progress,
    execute_bulk_write_operations
)


class FlashscoreLiveResultsSpider(scrapy.Spider):
    name = 'flashscore-live-results-scraper'

    # Configuration
    custom_settings = {
        'CONCURRENT_REQUESTS': 50,
        "LOG_ENABLED": False  # Disable scrapy's default logging since we have our own
    }

    # Match status mapping from Flashscore
    status_mapping = {
        '1': 'sched',
        '45': 'to_finish',
        '42': 'awaiting_updates',
        '2': 'live',
        '12': 'first_half',
        '38': 'half_time',
        '13': 'second_half',
        '6': 'extra_time',
        '7': 'penalties',
        '46': 'break_time',
        '3': 'finished',
        '10': 'after_extra_time',
        '11': 'after_penalties',
        '9': 'walkover',
        '43': 'delayed',
        '36': 'interrupted',
        '4': 'postponed',
        '5': 'cancelled',
        '37': 'abandoned',
        '54': 'awarded'
    }

    BULK_UPDATE_BATCH_SIZE = 100

    request_headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9,ar;q=0.8",
        "origin": "https://www.flashscore.com",
        "priority": "u=1, i",
        "referer": "https://www.flashscore.com/",
        "sec-ch-ua": "\"Chromium\";v=\"124\", \"Google Chrome\";v=\"124\", \"Not-A.Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "x-fsign": "SW9D1eZo"
    }

    def __init__(self):
        super().__init__()

        # Initialize logger
        self.custom_logger = setup_scraper_logger('flashscore_live')
        log_scraper_progress(self.custom_logger, 'INIT', 'Initializing Flashscore Live Results scraper')

        # Database connection
        mongodb_client = MongoClient(
            'mongodb://localhost:27017/',
            maxPoolSize=50,  # Increase pool size
            minPoolSize=10,  # Keep minimum connections
            maxIdleTimeMS=30000,  # Keep connections alive longer
            serverSelectionTimeoutMS=10000,  # 10 second timeout
            connectTimeoutMS=10000,
            socketTimeoutMS=0,  # No socket timeout (important!)
            waitQueueTimeoutMS=10000,
            retryWrites=True,
            heartbeatFrequencyMS=10000  # Check connection every 10s
        )
        betting_database = mongodb_client['betting']
        self.matches_collection = betting_database['matches_data']

        # Collections for bulk operations
        self.all_sports_mapping = {}
        self.bulk_update_operations = []

        # Counters
        self.total_matches_updated = 0
        self.finished_matches_count = 0
        self.live_matches_count = 0
        self.cancelled_postponed_count = 0

        log_scraper_progress(self.custom_logger, 'INIT_COMPLETE', 'Connected to matches database')

    def start_requests(self):
        log_scraper_progress(self.custom_logger, 'START', 'Starting Flashscore Live core data extraction')

        core_data_url = 'https://www.flashscore.com/x/js/core_2_2188000000.js'
        yield scrapy.Request(
            url=core_data_url,
            callback=self.parse_core_data,
            headers=self.request_headers
        )

    def parse_core_data(self, response, **kwargs):
        try:
            # Extract sports mapping from core JavaScript file
            sports_data_string = response.text.split('sport_list":{')[-1].split('},"')[0]
            self.all_sports_mapping = json.loads('{' + sports_data_string + '}')

            log_scraper_progress(
                self.custom_logger, 'SPORTS_MAPPED',
                f'Found {len(self.all_sports_mapping)} sports for live updates'
            )

            # Filter out racing and excluded sports for live updates
            excluded_sport_ids = [14, 16, 23, 28, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41]
            valid_sports_count = 0

            for sport_name, sport_id in self.all_sports_mapping.items():
                if sport_id not in excluded_sport_ids:
                    valid_sports_count += 1
                    # Only scrape today (day 0) for live results
                    live_results_url = f'https://global.flashscore.ninja/2/x/feed/f_{sport_id}_0_5_en_1'

                    yield scrapy.Request(
                        url=live_results_url,
                        callback=self.parse_live_sport_results,
                        headers=self.request_headers,
                        meta={'sport_name': sport_name, 'sport_id': sport_id}
                    )

            log_scraper_progress(
                self.custom_logger, 'LIVE_SPORTS_FILTERED',
                f'Processing {valid_sports_count} sports for live results'
            )

        except Exception as parsing_error:
            log_scraper_progress(
                self.custom_logger, 'CORE_DATA_PARSE_ERROR',
                'Failed to parse core sports data for live results',
                error=parsing_error
            )

    def parse_live_sport_results(self, response):
        try:
            sport_name = response.meta['sport_name']
            sport_id = response.meta['sport_id']
            matches_updated_in_sport = 0
            log_scraper_progress(
                self.custom_logger, 'PROCESSING_LIVE_SPORT',
                f'Processing live results for {sport_name}'
            )

            # Parse live match results from response text
            match_segments = response.text.split('~')

            for segment in match_segments:
                if segment.startswith('AA÷'):
                    # This segment contains live match information
                    match_updated = self._process_live_match_update(segment, sport_name)

                    if match_updated:
                        matches_updated_in_sport += 1
                        self.total_matches_updated += 1

                    # Process bulk updates if batch size reached
                    self._process_bulk_updates_if_needed()

            log_scraper_progress(
                self.custom_logger, 'LIVE_SPORT_COMPLETED',
                f'Sport: {sport_name}, Live matches updated: {matches_updated_in_sport}'
            )

        except Exception as sport_parsing_error:
            log_scraper_progress(
                self.custom_logger, 'LIVE_SPORT_ERROR',
                f'Error parsing live results for sport {response.meta.get("sport_name", "unknown")}',
                error=sport_parsing_error
            )

    def _process_live_match_update(self, match_segment, sport_name):
        """
        Process individual live match update from segment

        :param match_segment: Raw match data segment
        :param sport_name: Name of the sport
        :return: True if match was updated successfully, False otherwise
        """
        try:
            # Extract match ID
            match_id = match_segment.split('¬')[0].split('÷')[-1]

            # Extract match status code
            status_code = match_segment.split('¬AC÷')[-1].split('¬')[0]

            # Map status code to readable status
            readable_match_status = self.status_mapping.get(status_code, 'unknown')

            # Extract scores
            team1_score = match_segment.split('¬AG÷')[-1].split('¬')[0]
            team2_score = match_segment.split('¬AH÷')[-1].split('¬')[0]

            # Determine final status and process accordingly
            if readable_match_status in ['first_half', 'second_half', 'after_extra_time',
                                         'after_penalties', 'awaiting_updates', 'live']:
                final_status = 'live'
                self.live_matches_count += 1
            elif readable_match_status in ['cancelled', 'postponed']:
                final_status = readable_match_status
                team1_score = '-'
                team2_score = '-'
                self.cancelled_postponed_count += 1
            elif readable_match_status == 'finished':
                final_status = 'finished'
                self.finished_matches_count += 1
            else:
                # Skip matches that don't need updating
                return False

            # Prepare bulk update operation
            update_operation = UpdateOne(
                {"match_id": match_id},
                {"$set": {
                    "status": final_status,
                    'currentScore_competitor1': team1_score,
                    'currentScore_competitor2': team2_score
                }}
            )

            self.bulk_update_operations.append(update_operation)

            return True

        except Exception as match_update_error:
            log_scraper_progress(
                self.custom_logger, 'LIVE_MATCH_UPDATE_ERROR',
                f'Error processing live match update for match ID: {match_segment[:20]}...',
                error=match_update_error
            )
            return False

    def _process_bulk_updates_if_needed(self):
        """Process bulk updates if batch size is reached"""
        if len(self.bulk_update_operations) >= self.BULK_UPDATE_BATCH_SIZE:
            self._execute_bulk_updates()

    def _execute_bulk_updates(self):
        """Execute bulk update operations for live match results"""
        if not self.bulk_update_operations:
            return

        try:
            bulk_result = execute_bulk_write_operations(
                self.matches_collection,
                self.bulk_update_operations,
                "flashscore_live_updates",
                self.custom_logger
            )

            self.bulk_update_operations = []  # Clear operations list

        except Exception as bulk_error:
            log_scraper_progress(
                self.custom_logger, 'BULK_LIVE_UPDATE_ERROR',
                'Error executing bulk live updates',
                error=bulk_error
            )
            self.bulk_update_operations = []  # Clear operations list even on error

    def close(self, reason):
        """Final cleanup and bulk update execution for live results scraper"""
        try:
            # Execute any remaining bulk operations
            if self.bulk_update_operations:
                log_scraper_progress(
                    self.custom_logger, 'FINAL_LIVE_BULK_UPDATE',
                    f'Executing final {len(self.bulk_update_operations)} live bulk operations'
                )
                self._execute_bulk_updates()

            log_scraper_progress(
                self.custom_logger, 'LIVE_RESULTS_SCRAPER_COMPLETED',
                'Flashscore Live Results scraper completed successfully',
                match_count=self.total_matches_updated
            )

            log_scraper_progress(
                self.custom_logger, 'FINAL_LIVE_RESULTS_STATS',
                f'Total updated: {self.total_matches_updated}, '
                f'Live: {self.live_matches_count}, '
                f'Finished: {self.finished_matches_count}, '
                f'Cancelled/Postponed: {self.cancelled_postponed_count}'
            )

        except Exception as cleanup_error:
            log_scraper_progress(
                self.custom_logger, 'LIVE_RESULTS_CLEANUP_ERROR',
                'Error during live results cleanup',
                error=cleanup_error
            )


if __name__ == '__main__':
    crawler_process = CrawlerProcess()
    crawler_process.crawl(FlashscoreLiveResultsSpider)
    crawler_process.start()