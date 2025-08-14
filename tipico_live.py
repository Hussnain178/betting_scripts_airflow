import json
import scrapy
from scrapy.crawler import CrawlerProcess
from pymongo import MongoClient, UpdateOne
from helper import (remove_empty_dicts,
    check_key, compare_matchups, check_header_name, setup_scraper_logger,
    log_scraper_progress, execute_bulk_write_operations,parse_tipico_date,normalize_timestamp_for_comparison
)


class TipicoLiveOddsSpider(scrapy.Spider):
    name = "tipico-live-odds-scraper"

    # Configuration
    custom_settings = {
        'CONCURRENT_REQUESTS': 64,
    }

    BULK_UPDATE_BATCH_SIZE = 50  # Smaller batches for live updates

    request_headers = {
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9,ar;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "priority": "u=0, i",
        "sec-ch-ua": "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    }

    def __init__(self):
        super().__init__()

        # Initialize logger
        self.custom_logger = setup_scraper_logger('tipico_live')
        log_scraper_progress(self.custom_logger, 'INIT', 'Initializing Tipico Live scraper')

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

        # Load mapping data
        odds_mapping_collection = betting_database['ots']
        self.odds_type_mappings = list(odds_mapping_collection.find())

        country_collection = betting_database['cos']
        self.country_data = list(country_collection.find())

        # Matches collection for updates
        self.matches_collection = betting_database['matches_data']

        # Collections for storing processed data
        self.unique_odds_keys = set()
        self.processed_live_matches = []
        self.bulk_update_operations = []

        # Proxy configuration
        self.proxy_url = "http://hafiz123-AT-rotate:pucit123@p.webshare.io:80/"

        # Counters
        self.total_live_matches_processed = 0
        self.successful_live_matches = 0
        self.failed_live_matches = 0

        log_scraper_progress(
            self.custom_logger, 'INIT_COMPLETE',
            f'Loaded {len(self.odds_type_mappings)} odds mappings for live matches'
        )

    def start_requests(self):
        log_scraper_progress(self.custom_logger, 'START', 'Starting Tipico Live navigation scraping')

        navigation_tree_url = 'https://sports.tipico.com/json/program/navigationTree/all'
        yield scrapy.Request(
            url=navigation_tree_url,
            headers=self.request_headers,
            callback=self.parse_navigation,
            meta={'proxy': self.proxy_url}
        )

    def parse_navigation(self, response, **kwargs):
        try:
            navigation_data = json.loads(response.text)
            sports_categories = navigation_data['children']

            log_scraper_progress(
                self.custom_logger, 'LIVE_NAVIGATION_PARSED',
                f'Found {len(sports_categories)} sports categories for live matches'
            )

            for sport_category in sports_categories:
                sport_id = sport_category['sportId']

                # Get live matches for each sport
                live_matches_url = (
                    f'https://sports.tipico.com/v1/tpapi/programgateway/program/events/live'
                    f'?oneSectionResult=true&selectedSports={sport_id}&isLoggedIn=0'
                    f'&licenseRegion=COM&language=de&maxMarkets=5'
                )

                live_request_headers = {
                    "accept": "application/json",
                    "accept-language": "en-US,en;q=0.9",
                    "mobileproductclienttype": "DESKTOP_WEB_RETH",
                    "priority": "u=1, i",
                    "referer": "https://sports.tipico.com/",
                    "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": "\"Windows\"",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "tipico-label": "tipico",
                    "tipico-product": "sports",
                    "tpapi-client-type": "DESKTOP_WEB_RETH",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
                    "x-instana-l": "1,correlationType=web;correlationId=c199741e769c542",
                    "x-instana-s": "c199741e769c542",
                    "x-instana-t": "c199741e769c542",
                    "x-xsrf-token": "Ws4EUWp1-TQUE9_kgvDKReB1Le6qOMNg_fL0"
                }

                yield scrapy.Request(
                    url=live_matches_url,
                    method='GET',
                    dont_filter=True,
                    callback=self.extract_live_matches,
                    headers=live_request_headers,
                    meta={'proxy': self.proxy_url}
                )

            log_scraper_progress(
                self.custom_logger, 'LIVE_SPORTS_QUEUED',
                f'Queued live match requests for {len(sports_categories)} sports'
            )

        except Exception as parsing_error:
            log_scraper_progress(
                self.custom_logger, 'LIVE_NAVIGATION_ERROR',
                'Failed to parse live navigation',
                error=parsing_error
            )

    def extract_live_matches(self, response):
        try:
            request_cookies = {"language": 'en'}

            live_response_data = json.loads(response.text)
            live_matches_data = live_response_data['LIVE']

            if not live_matches_data or 'events' not in live_matches_data:
                log_scraper_progress(self.custom_logger, 'NO_LIVE_MATCHES', 'No live matches found')
                return

            live_matches_count = len(live_matches_data['events'])
            log_scraper_progress(
                self.custom_logger, 'LIVE_MATCHES_EXTRACTED',
                f'Found {live_matches_count} live matches'
            )

            for match_info in live_matches_data['events'].values():
                match_id = match_info['id']

                # Only process matches that have team IDs (valid matches)
                if match_info['team1Id']:
                    match_details_url = f'https://sports.tipico.com/json/services/event/{match_id}'

                    yield scrapy.Request(
                        url=match_details_url,
                        callback=self.extract_live_match_odds_data,
                        cookies=request_cookies,
                        headers=self.request_headers,
                        meta={'proxy': self.proxy_url}
                    )

        except Exception as extraction_error:
            log_scraper_progress(
                self.custom_logger, 'LIVE_MATCHES_EXTRACTION_ERROR',
                'Error extracting live matches',
                error=extraction_error
            )

    def extract_live_match_odds_data(self, response):
        try:
            match_data = json.loads(response.text)
            event_info = match_data['event']

            # Only process live matches
            if not event_info['live']:
                return

            self.total_live_matches_processed += 1
            match_start_date = event_info.get('startDate', '')
            if not match_start_date:
                self.failed_live_matches += 1
                return

            try:
                parsed_match_datetime = parse_tipico_date(match_start_date)
            except Exception as date_error:
                log_scraper_progress(
                    self.custom_logger, 'DATE_PARSE_ERROR',
                    f'Error parsing date {match_start_date}',
                    error=date_error
                )
                self.failed_live_matches += 1
                return

            # Determine sport category
            sport_group_key = 'group' if 'group' in event_info else 'groups'
            sport_categories = event_info[sport_group_key]

            # Normalize sport name
            normalized_sport = self._normalize_sport_name(sport_categories[-1])

            # Build live match information dictionary
            live_match_information = {
                'website': 'tipico',
                'sport': normalized_sport,
                'country': sport_categories[-2],
                'group': sport_categories[0],
                'tipico_match_id': str(event_info['id']),
                'timestamp': parsed_match_datetime,
                'competitor1': event_info['team1'],
                'competitor2': event_info['team2'],
                'status': 'live',
                'prices': {},
                "is_country": self._check_if_valid_country(sport_categories[-2]),
            }

            # Extract live odds data
            self._extract_live_odds_information(match_data, live_match_information)


            live_match_information['prices']=remove_empty_dicts(live_match_information['prices'])

            # Try to match with existing flashscore data and prepare bulk update
            self._match_live_data_with_flashscore(live_match_information)

            # Process bulk updates if batch size reached
            self._process_bulk_updates_if_needed()

            self.processed_live_matches.append(live_match_information)
            self.successful_live_matches += 1

            # Log progress every 25 matches
            if self.total_live_matches_processed % 25 == 0:
                log_scraper_progress(
                    self.custom_logger, 'LIVE_PROGRESS_UPDATE',
                    f'Processed {self.total_live_matches_processed} LIVE matches, '
                    f'Success: {self.successful_live_matches}, Failed: {self.failed_live_matches}'
                )

        except Exception as live_match_error:
            self.failed_live_matches += 1
            log_scraper_progress(
                self.custom_logger, 'LIVE_MATCH_PROCESSING_ERROR',
                'Error processing live match odds data',
                error=live_match_error
            )

    def _normalize_sport_name(self, sport_name):
        """Normalize sport names to standard format"""
        normalized_name = sport_name.strip().lower()

        if normalized_name in ['football', 'esports']:
            return 'soccer'
        else:
            return sport_name.strip()

    def _check_if_valid_country(self, country_name):
        """Check if the country exists in our country data"""
        for country_info in self.country_data:
            if country_info['id'] == country_name.lower():
                return True
        return False

    def _extract_live_odds_information(self, match_data, match_information):
        """Extract and process live odds data from match"""
        event_info = match_data['event']

        # Build category dictionary for faster lookup
        category_mapping = {
            str(category['id']): category['name']
            for category in match_data.get('categories', {})
            if category['id'] < 100
        }

        odds_group_data = match_data.get('categoryOddGroupMapSectioned', {})

        for category_id in odds_group_data.keys():
            if category_id not in category_mapping:
                continue

            for odds_group in odds_group_data[category_id]:
                # Normalize odds group title by replacing team names
                normalized_odds_key = (
                    odds_group['oddGroupTitle']
                    .replace(event_info['team1'], 'home')
                    .replace(event_info['team2'], 'away')
                )
                if ' - extra time' in normalized_odds_key.lower() and ' including extra time' in normalized_odds_key.lower():
                    normalized_odds_key = normalized_odds_key.replace(' - extra time', '').replace(' including extra time', '')

                if not check_key(normalized_odds_key):
                    continue

                # Get mapping data for this odds type
                mapping_result = self._get_odds_mapping_data(normalized_odds_key)
                odds_value_mappings = mapping_result[0]
                standardized_odds_key = mapping_result[1]

                # Process each odds group ID
                for odds_group_id in odds_group['oddGroupIds']:
                    self._process_live_odds_group(
                        match_data,
                        odds_group_id,
                        standardized_odds_key,
                        odds_value_mappings,
                        match_information
                    )

    def _process_live_odds_group(self, match_data, odds_group_id, odds_key, value_mappings, match_info):
        """Process individual live odds group and extract betting lines"""
        odds_group_id_str = str(odds_group_id)
        odds_results = match_data['oddGroupResultsMap'][odds_group_id_str]

        # Adjust odds key based on number of outcomes
        if odds_key.lower() == '3-way' and len(odds_results) == 2:
            odds_key = '2-way'
            mapping_result = self._get_odds_mapping_data(odds_key)
            value_mappings = mapping_result[0]
            odds_key = mapping_result[1]
        elif odds_key.lower() == '2-way' and len(odds_results) == 3:
            odds_key = '3-way'
            mapping_result = self._get_odds_mapping_data(odds_key)
            value_mappings = mapping_result[0]
            odds_key = mapping_result[1]

        # Get subtitle/handicap information
        odds_subtitle = self._extract_live_odds_subtitle(match_data, odds_group_id_str)

        # Determine header categorization
        header_result = check_header_name(odds_key)
        odds_header_category = header_result[0]
        final_odds_key = header_result[1]

        self.unique_odds_keys.add(final_odds_key)

        # Initialize nested dictionary structure
        prices_dict = match_info['prices']
        if odds_header_category not in prices_dict:
            prices_dict[odds_header_category] = {}
        if final_odds_key not in prices_dict[odds_header_category]:
            prices_dict[odds_header_category][final_odds_key] = {}
        if odds_subtitle not in prices_dict[odds_header_category][final_odds_key]:
            prices_dict[odds_header_category][final_odds_key][odds_subtitle] = {}

        # Extract individual odds values for live match
        for result_id in odds_results:
            result_info = match_data['results'][str(result_id)]

            odds_value =str(round(float(result_info['quoteFloatValue']), 1))

            if odds_value == '' or odds_value == ' ' or odds_value is None:
                break

            outcome_name = result_info['caption']

            # Map outcome name if mappings exist
            if value_mappings:
                mapped_outcome = self._map_outcome_name(outcome_name, value_mappings)
                outcome_name = mapped_outcome

            prices_dict[odds_header_category][final_odds_key][odds_subtitle][outcome_name] = odds_value

    def _extract_live_odds_subtitle(self, match_data, odds_group_id):
        """Extract subtitle/handicap information from live odds group"""
        odds_group_info = match_data['oddGroups'][odds_group_id]
        short_caption = odds_group_info.get('shortCaption', '')

        if not short_caption:
            return 'null'

        return short_caption.split(' ')[0].replace(',', '.')

    def _map_outcome_name(self, outcome_name, value_mappings):
        """Map outcome name using provided mappings"""
        outcome_lower = outcome_name.lower()

        for mapping in value_mappings:
            if mapping['id'] == outcome_lower:
                return mapping['id']
            elif 'maps' in mapping and outcome_lower in mapping['maps']:
                return mapping['id']

        return outcome_name

    def _match_live_data_with_flashscore(self, tipico_live_match_info):
        """Match tipico LIVE data with flashscore data and prepare bulk update"""
        # normalized_tipico_timestamp = normalize_timestamp_for_comparison(tipico_live_match_info['timestamp'])
        # tipico_sport_normalized = (tipico_live_match_info['sport'].lower().replace('-', '').replace(' ', ''))
        #
        # # Query database for potential matches (no timestamp needed for live matches)
        # potential_matches_cursor = self.matches_collection.find({"timestamp": normalized_tipico_timestamp,})
        #
        # for flashscore_match in potential_matches_cursor:
        #     # Check sport compatibility
        #     flashscore_sport_normalized = (flashscore_match['sport']
        #                                    .replace('-', '').replace(' ', ''))
        #
        #     sport_matches = tipico_sport_normalized == flashscore_sport_normalized
        #
        #     if sport_matches:
        #         matchup_compatibility = compare_matchups(
        #             flashscore_match['competitor1'].lower(),
        #             flashscore_match['competitor2'].lower(),
        #             tipico_live_match_info['competitor1'].lower(),
        #             tipico_live_match_info['competitor2'].lower()
        #         )
        #
        #         if matchup_compatibility:
        #             # Prepare bulk update operation for LIVE match
        #             update_operation = UpdateOne(
        #                 {"match_id": flashscore_match["match_id"]},
        #                 {"$set": {
        #                     "prices.tipico": tipico_live_match_info['prices'],
        #                     # Optionally update status to live if needed
        #                     # "status": "live"
        #                 }}
        #             )
        #             self.bulk_update_operations.append(update_operation)
        #
        #             log_scraper_progress(
        #                 self.custom_logger, 'LIVE_MATCH_FOUND',
        #                 f'Matched LIVE {tipico_live_match_info["competitor1"]} vs {tipico_live_match_info["competitor2"]}'
        #             )
        #             break

        flashscore_match = self.matches_collection.find_one({
            "tipico_match_id": tipico_live_match_info["tipico_match_id"],
        })
        if flashscore_match:
            # Prepare bulk update operation for LIVE match
            update_operation = UpdateOne(
                {"match_id": flashscore_match["match_id"]},
                {"$set": {
                    "prices.bovada": tipico_live_match_info['odds'],
                    # Optionally update status to live if needed
                    # "status": "live"
                }}
            )
            self.bulk_update_operations.append(update_operation)

            log_scraper_progress(
                self.custom_logger, 'LIVE_MATCH_FOUND',
                f'Matched LIVE {tipico_live_match_info["competitor1"]} vs {tipico_live_match_info["competitor2"]}'
            )

    def _get_odds_mapping_data(self, odds_key):
        """Get mapping data for odds key from MongoDB"""
        odds_value_mappings = None
        standardized_key = odds_key

        for mapping_entry in self.odds_type_mappings:
            try:
                if odds_key.lower() == mapping_entry['id']:
                    standardized_key = mapping_entry['id']
                    odds_value_mappings = mapping_entry.get('ovs')
                    break
                elif 'maps' in mapping_entry and odds_key.lower() in mapping_entry['maps']:
                    standardized_key = mapping_entry['id']
                    odds_value_mappings = mapping_entry.get('ovs')
                    break
            except Exception as mapping_error:
                log_scraper_progress(
                    self.custom_logger, 'MAPPING_ERROR',
                    f'Error in live odds mapping: {mapping_error}',
                    error=mapping_error
                )

        return [odds_value_mappings, standardized_key]

    def _process_bulk_updates_if_needed(self):
        """Process bulk updates if batch size is reached"""
        if len(self.bulk_update_operations) >= self.BULK_UPDATE_BATCH_SIZE:
            self._execute_bulk_updates()

    def _execute_bulk_updates(self):
        """Execute bulk update operations for live matches"""
        if not self.bulk_update_operations:
            return

        try:
            bulk_result = execute_bulk_write_operations(
                self.matches_collection,
                self.bulk_update_operations,
                "tipico_live_updates",
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
        """Final cleanup and bulk update execution for live scraper"""
        try:
            # Execute any remaining bulk operations
            if self.bulk_update_operations:
                log_scraper_progress(
                    self.custom_logger, 'FINAL_LIVE_BULK_UPDATE',
                    f'Executing final {len(self.bulk_update_operations)} live bulk operations'
                )
                self._execute_bulk_updates()

            log_scraper_progress(
                self.custom_logger, 'LIVE_SCRAPER_COMPLETED',
                'Tipico Live scraper completed successfully',
                match_count=len(self.processed_live_matches)
            )

            log_scraper_progress(
                self.custom_logger, 'FINAL_LIVE_STATS',
                f'Total LIVE: {self.total_live_matches_processed}, Success: {self.successful_live_matches}, '
                f'Failed: {self.failed_live_matches}, Unique odds: {len(self.unique_odds_keys)}'
            )

        except Exception as cleanup_error:
            log_scraper_progress(
                self.custom_logger, 'LIVE_CLEANUP_ERROR',
                'Error during live cleanup',
                error=cleanup_error
            )


if __name__ == '__main__':
    crawler_process = CrawlerProcess()
    crawler_process.crawl(TipicoLiveOddsSpider)
    crawler_process.start()