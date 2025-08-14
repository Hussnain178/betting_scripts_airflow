import scrapy
from scrapy.crawler import CrawlerProcess
import json
from pymongo import MongoClient
from datetime import datetime
import pytz
from helper import (
    setup_scraper_logger, log_scraper_progress,
    execute_bulk_write_operations, store_data_into_mongodb
)


class FlashscoreFixturesSpider(scrapy.Spider):
    name = 'flashscore-fixtures-scraper'

    # Configuration
    custom_settings = {
        'CONCURRENT_REQUESTS': 32,
    }

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

    def __init__(self, proxy=0, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Initialize logger
        self.custom_logger = setup_scraper_logger('flashscore')
        log_scraper_progress(self.custom_logger, 'INIT', 'Initializing Flashscore fixtures scraper')

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

        # Load country data
        country_collection = betting_database['cos']
        self.country_data = list(country_collection.find())

        # Collections for storing processed data
        self.all_sports_mapping = {}
        self.all_countries_mapping = {}
        self.processed_matches = []

        # Proxy settings
        self.enable_proxy = int(proxy)

        # Counters
        self.total_matches_found = 0
        self.valid_matches = 0
        self.invalid_matches = 0

        log_scraper_progress(
            self.custom_logger, 'INIT_COMPLETE',
            f'Loaded {len(self.country_data)} countries data'
        )

    def start_requests(self):
        log_scraper_progress(self.custom_logger, 'START', 'Starting Flashscore core data extraction')

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
                f'Found {len(self.all_sports_mapping)} sports in mapping'
            )

            # Filter out racing and excluded sports
            excluded_sport_ids = [14, 16, 23, 28, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41]
            valid_sports_count = 0

            for sport_name, sport_id in self.all_sports_mapping.items():
                if sport_id not in excluded_sport_ids:
                    valid_sports_count += 1
                    # Scrape multiple days for each sport (8 days total: today + 7 future days)
                    for day_offset in [0, 1, 2, 3, 4, 5, 6, 7]:
                        fixtures_url = f'https://global.flashscore.ninja/2/x/feed/f_{sport_id}_{day_offset}_5_en_1'

                        yield scrapy.Request(
                            url=fixtures_url,
                            callback=self.parse_sport_fixtures,
                            headers=self.request_headers,
                            meta={'sport_name': sport_name, 'sport_id': sport_id, 'day_offset': day_offset}
                        )

            log_scraper_progress(
                self.custom_logger, 'VALID_SPORTS_FILTERED',
                f'Processing {valid_sports_count} valid sports (excluding racing and other excluded sports)'
            )

        except Exception as parsing_error:
            log_scraper_progress(
                self.custom_logger, 'CORE_DATA_PARSE_ERROR',
                'Failed to parse core sports data',
                error=parsing_error
            )

    def get_standardized_datetime_from_unix(self, unix_timestamp):
        """
        Convert Unix timestamp to standardized UTC datetime object

        :param unix_timestamp: Unix timestamp in seconds
        :return: timezone-aware UTC datetime object
        """
        try:
            # Create timezone-aware UTC datetime from Unix timestamp
            utc_datetime = datetime.fromtimestamp(unix_timestamp, tz=pytz.UTC)
            return utc_datetime
        except Exception as conversion_error:
            log_scraper_progress(
                self.custom_logger, 'TIMESTAMP_CONVERSION_ERROR',
                f'Error converting timestamp {unix_timestamp}',
                error=conversion_error
            )
            # Return current UTC time as fallback
            return datetime.now(pytz.UTC)

    def parse_sport_fixtures(self, response):
        try:
            sport_name = response.meta['sport_name']
            sport_id = response.meta['sport_id']
            day_offset = response.meta['day_offset']

            current_country = ''
            current_league = ''
            matches_in_sport = 0

            # Parse matches from response text
            match_segments = response.text.split('~')

            for segment in match_segments:
                if segment.startswith('ZA÷'):
                    # This segment contains country and league information
                    if '¬ZY÷' in segment:
                        current_country = segment.split('¬ZY÷')[-1].split('¬')[0]
                    else:
                        current_country = segment.split('ZA÷')[-1].split('¬')[0].split(':')[0]

                    current_league = ':'.join(segment.split('ZA÷')[-1].split('¬')[0].split(':')[1:]).strip()

                elif segment.startswith('AA÷'):
                    # This segment contains match information
                    match_processed = self._process_individual_match(
                        segment, sport_name, current_country, current_league
                    )

                    if match_processed:
                        matches_in_sport += 1
                        self.total_matches_found += 1

            log_scraper_progress(
                self.custom_logger, 'SPORT_FIXTURES_PROCESSED',
                f'Sport: {sport_name} (Day +{day_offset}), Matches found: {matches_in_sport}'
            )

        except Exception as sport_parsing_error:
            log_scraper_progress(
                self.custom_logger, 'SPORT_FIXTURES_ERROR',
                f'Error parsing fixtures for sport {response.meta.get("sport_name", "unknown")}',
                error=sport_parsing_error
            )

    def _process_individual_match(self, match_segment, sport_name, country_name, league_name):
        """
        Process individual match data from segment

        :param match_segment: Raw match data segment
        :param sport_name: Name of the sport
        :param country_name: Name of the country
        :param league_name: Name of the league
        :return: True if match was processed successfully, False otherwise
        """
        try:
            # Extract match ID
            match_id = match_segment.split('¬')[0].split('÷')[-1]

            # Extract match status
            status_code = match_segment.split('¬AC÷')[-1].split('¬')[0]

            # Only process scheduled matches (status code '1')
            if status_code not in ['1']:
                return False

            # Extract match timestamp
            unix_timestamp = int(match_segment.split('¬AD÷')[-1].split('¬')[0])

            # Check if match is in the future
            current_utc_time = datetime.now(pytz.UTC)
            match_utc_time = datetime.fromtimestamp(unix_timestamp, tz=pytz.UTC)

            if current_utc_time > match_utc_time:
                # Skip past matches
                return False

            # Get standardized datetime object
            standardized_match_datetime = self.get_standardized_datetime_from_unix(unix_timestamp)

            # Extract competitor names
            competitor1_name = match_segment.split('¬AE÷')[-1].split('¬')[0]
            competitor2_name = match_segment.split('AF÷')[-1].split('¬')[0]

            # Build match information dictionary
            match_information = {
                "match_id": match_id,
                "sport": sport_name,
                "country": country_name,
                "group": league_name,
                "timestamp": standardized_match_datetime,
                "competitor1": competitor1_name,
                "competitor2": competitor2_name,
                "status": "sched",
                "is_country": self._check_if_valid_country(country_name),
                "prices": {},
            }

            # Generate match link
            if sport_name == 'soccer':
                sport_url_name = 'football'
            else:
                sport_url_name = sport_name

            match_link = f'https://www.flashscore.com/match/{sport_url_name}/{match_id}/#/match-summary'
            match_information['match_link'] = match_link

            self.processed_matches.append(match_information)
            self.valid_matches += 1

            # Log progress every 100 matches
            if self.valid_matches % 100 == 0:
                log_scraper_progress(
                    self.custom_logger, 'MATCHES_PROGRESS',
                    f'Processed {self.valid_matches} valid matches so far'
                )

            return True

        except Exception as match_processing_error:
            self.invalid_matches += 1
            log_scraper_progress(
                self.custom_logger, 'MATCH_PROCESSING_ERROR',
                f'Error processing match segment',
                error=match_processing_error
            )
            return False

    def _check_if_valid_country(self, country_name):
        """
        Check if the country exists in our country data

        :param country_name: Name of the country to check
        :return: True if country is valid, False otherwise
        """
        for country_info in self.country_data:
            if country_info['id'] == country_name.lower():
                return True
        return False

    def close(self, reason):
        """Final cleanup and data storage"""
        try:
            log_scraper_progress(
                self.custom_logger, 'STORING_MATCHES',
                f'Storing {len(self.processed_matches)} matches to database'
            )

            # Store all processed matches to MongoDB
            store_data_into_mongodb(self.processed_matches, 'matches_data', self.custom_logger)

            log_scraper_progress(
                self.custom_logger, 'SCRAPER_COMPLETED',
                'Flashscore fixtures scraper completed successfully',
                match_count=len(self.processed_matches)
            )

            log_scraper_progress(
                self.custom_logger, 'FINAL_STATS',
                f'Total found: {self.total_matches_found}, Valid: {self.valid_matches}, '
                f'Invalid: {self.invalid_matches}, Sports: {len(self.all_sports_mapping)}'
            )

        except Exception as cleanup_error:
            log_scraper_progress(
                self.custom_logger, 'CLEANUP_ERROR',
                'Error during cleanup and data storage',
                error=cleanup_error
            )


if __name__ == '__main__':
    crawler_process = CrawlerProcess()
    crawler_process.crawl(FlashscoreFixturesSpider)
    crawler_process.start()