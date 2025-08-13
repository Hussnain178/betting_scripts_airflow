import json
from pymongo import MongoClient, UpdateOne
import scrapy
from scrapy.crawler import CrawlerProcess
from helper import (remove_empty_dicts,
    check_sport_name, parse_tipico_date, normalize_timestamp_for_comparison,
    check_key, check_header_name, compare_matchups, setup_scraper_logger,
    log_scraper_progress, execute_bulk_write_operations
)


class UnibetOddsSpider(scrapy.Spider):
    name = "unibet-odds-scraper"

    # Configuration
    custom_settings = {
        'CONCURRENT_REQUESTS': 64
    }

    BULK_UPDATE_BATCH_SIZE = 100

    request_headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "referer": "https://www.unibet.mt/betting/sports/home",
        "sec-ch-ua": "\"Not;A=Brand\";v=\"99\", \"Google Chrome\";v=\"139\", \"Chromium\";v=\"139\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }

    def __init__(self):
        super().__init__()

        # Initialize logger
        self.custom_logger = setup_scraper_logger('unibet')
        log_scraper_progress(self.custom_logger, 'INIT', 'Initializing Unibet scraper')

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
        self.valid_sports = set()
        self.processed_matches = []
        self.unique_odds_keys = set()
        self.bulk_update_operations = []

        # Proxy configuration
        self.proxy_url = "http://hafiz123-MT-rotate:pucit123@p.webshare.io:80/"

        # Counters
        self.total_matches_processed = 0
        self.successful_matches = 0
        self.failed_matches = 0

        log_scraper_progress(
            self.custom_logger, 'INIT_COMPLETE',
            f'Loaded {len(self.odds_type_mappings)} odds mappings, {len(self.country_data)} countries'
        )

    def start_requests(self):
        log_scraper_progress(self.custom_logger, 'START', 'Starting Unibet sports navigation')

        sports_navigation_url = 'https://www.unibet.mt/sportsbook-feeds/views/sports/a-z?ncid=1754720826'
        yield scrapy.Request(
            url=sports_navigation_url,
            headers=self.request_headers,
            method='GET',
            dont_filter=True,
            callback=self.parse_sports_navigation,
            meta={'proxy': self.proxy_url}
        )

    def parse_sports_navigation(self, response, **kwargs):
        try:
            sports_data = json.loads(response.text)
            available_sports = sports_data['layout']['sections'][1]['widgets'][0]['sports']

            log_scraper_progress(
                self.custom_logger, 'SPORTS_NAVIGATION_PARSED',
                f'Found {len(available_sports)} sports in navigation'
            )

            valid_sports_count = 0
            for sport_data in available_sports:
                sport_name = sport_data['name']
                # if sport_name != 'Basketball':
                #     continue
                if check_sport_name(sport_name):
                    valid_sports_count += 1
                    normalized_sport_name = sport_name.replace(' ', '_').lower()
                    self.valid_sports.add(normalized_sport_name)

                    sport_matches_url = (
                        f'https://www.unibet.mt/sportsbook-feeds/views/filter/{normalized_sport_name}/all/matches'
                        f'?includeParticipants=true&useCombined=true'
                    )

                    yield scrapy.Request(
                        url=sport_matches_url,
                        headers=self.request_headers,
                        callback=self.extract_sport_data,
                        method='GET',
                        dont_filter=True,
                        meta={'proxy': self.proxy_url, 'sport_name': sport_name}
                    )

            log_scraper_progress(
                self.custom_logger, 'VALID_SPORTS_FILTERED',
                f'Processing {valid_sports_count} valid sports'
            )

        except Exception as navigation_error:
            log_scraper_progress(
                self.custom_logger, 'NAVIGATION_ERROR',
                'Failed to parse sports navigation',
                error=navigation_error
            )

    def extract_sport_data(self, response):
        try:
            sport_data = json.loads(response.text)
            sport_name = response.meta.get('sport_name', 'unknown')

            try:
                match_groups = sport_data['layout']['sections'][1]['widgets'][0]['matches']['groups']
            except (KeyError, IndexError):
                log_scraper_progress(
                    self.custom_logger, 'NO_MATCH_GROUPS',
                    f'No match groups found for {sport_name}'
                )
                return

            if not match_groups:
                return

            # Get sport name from first group
            actual_sport_name = match_groups[0]['sport']
            if actual_sport_name.lower() == 'football':
                actual_sport_name = 'soccer'
            log_scraper_progress(
                self.custom_logger, 'SPORT_DATA_EXTRACTED',
                f'Processing matches for {actual_sport_name} ({len(match_groups)} groups)'
            )

            matches_in_sport = 0
            for match_group in match_groups:
                # Process match group and yield requests
                for request in self._process_match_group(match_group, actual_sport_name):
                    matches_in_sport += 1
                    yield request

            log_scraper_progress(
                self.custom_logger, 'SPORT_COMPLETED',
                f'Sport: {actual_sport_name}, Matches processed: {matches_in_sport}'
            )

        except Exception as extraction_error:
            log_scraper_progress(
                self.custom_logger, 'SPORT_EXTRACTION_ERROR',
                f'Error extracting sport data for {response.meta.get("sport_name", "unknown")}',
                error=extraction_error
            )

    def _process_match_group(self, match_group, sport_name):
        """Process matches within a group (league/country) and yield requests"""
        try:
            if match_group.get('subGroups'):
                # Group has subgroups (leagues within countries)
                for subgroup in match_group['subGroups']:
                    for match_event in subgroup['events']:
                        if self._should_process_match(match_event):
                            # Yield the request from _process_individual_match
                            for request in self._process_individual_match(
                                match_event, sport_name,
                                match_group['name'], subgroup['name']
                            ):
                                yield request
            else:
                # Group has direct events (no subgroups)
                league_name = match_group['name']
                for match_event in match_group['events']:
                    if self._should_process_match(match_event):
                        # Yield the request from _process_individual_match
                        for request in self._process_individual_match(
                            match_event, sport_name,
                            league_name, league_name
                        ):
                            yield request

        except Exception as group_error:
            log_scraper_progress(
                self.custom_logger, 'MATCH_GROUP_ERROR',
                f'Error processing match group',
                error=group_error
            )

    def _should_process_match(self, match_event):
        """Check if match should be processed (not live)"""
        return 'liveData' not in match_event

    def _process_individual_match(self, match_event, sport_name, country_name, group_name):
        """Process individual match and prepare for odds extraction"""
        try:
            self.total_matches_processed += 1

            # Parse match timestamp
            match_timestamp = parse_tipico_date(match_event['event']['start'])

            match_information = {
                'website': 'unibet',
                'sport': sport_name,
                'country': country_name,
                'group': group_name,
                'competitor1': match_event['event']['homeName'],
                'competitor2': match_event['event']['awayName'],
                'timestamp': match_timestamp,
                'match_id': match_event['event']['id'],
                'status': 'sched',
                'prices': {}
            }

            # Request detailed odds data for this match
            detailed_odds_url = (
                f"https://eu-offering-api.kambicdn.com/offering/v2018/ubca/betoffer/event/"
                f"{match_event['event']['id']}.json?lang=en_GB&market=ZZ&client_id=2&channel_id=1&includeParticipants=true"
            )

            yield scrapy.Request(
                url=detailed_odds_url,
                method='GET',
                dont_filter=True,
                headers=self.request_headers,
                callback=self.extract_match_odds_data,
                meta={
                    'match_info': match_information,
                    'proxy': self.proxy_url
                }
            )

        except Exception as match_error:
            self.failed_matches += 1
            log_scraper_progress(
                self.custom_logger, 'INDIVIDUAL_MATCH_ERROR',
                'Error processing individual match',
                error=match_error
            )

    def extract_match_odds_data(self, response):
        try:
            odds_data = json.loads(response.text)
            match_information = response.meta['match_info']

            if not odds_data.get('betOffers'):
                log_scraper_progress(
                    self.custom_logger, 'NO_ODDS_DATA',
                    f'No odds data for match {match_information["competitor1"]} vs {match_information["competitor2"]}'
                )
                return

            # Track ignored category IDs to handle duplicates
            # ignored_category_id = 0

            # Process each betting offer
            for betting_offer in odds_data['betOffers']:
                # category_id = betting_offer['criterion']['id']

                # Skip duplicate categories
                # if ignored_category_id == category_id:
                #     continue

                # Process market name
                market_name = self._process_market_name(betting_offer, match_information)

                if not market_name or not self._is_valid_market(market_name):
                    continue

                # Get mapping data
                mapping_result = self._get_odds_mapping_data(market_name)
                odds_value_mappings = mapping_result[0]
                standardized_market_name = mapping_result[1]

                # Get header categorization
                header_result = check_header_name(standardized_market_name)
                market_header_category = header_result[0]
                final_market_name = header_result[1]

                if market_header_category not in match_information['prices']:
                    match_information['prices'][market_header_category] = {}

                self.unique_odds_keys.add(final_market_name.lower())

                # Process outcomes and check for duplicates
                duplicate_handling_result = self._handle_duplicate_markets(
                    betting_offer, odds_data, match_information,
                    market_header_category, final_market_name, odds_value_mappings
                )

                # if duplicate_handling_result:
                #     ignored_category_id = category_id
            match_information['prices']=remove_empty_dicts(match_information['prices'])

            # Try to match with flashscore data
            self._match_with_flashscore_data(match_information)

            # Process bulk updates if needed
            self._process_bulk_updates_if_needed()

            self.processed_matches.append(match_information)
            self.successful_matches += 1

            if self.total_matches_processed % 50 == 0:
                log_scraper_progress(
                    self.custom_logger, 'PROGRESS_UPDATE',
                    f'Processed {self.total_matches_processed} matches, '
                    f'Success: {self.successful_matches}, Failed: {self.failed_matches}'
                )

        except Exception as odds_extraction_error:
            self.failed_matches += 1
            log_scraper_progress(
                self.custom_logger, 'ODDS_EXTRACTION_ERROR',
                'Error extracting odds data',
                error=odds_extraction_error
            )

    def _process_market_name(self, betting_offer, match_info):
        """Process and normalize market name"""
        market_name = (betting_offer['criterion']['label']
                       .replace(match_info['competitor1'], 'home')
                       .replace(match_info['competitor2'], 'away')
                       .replace('(3-way)', '3-way'))
        return market_name

    def _is_valid_market(self, market_name):
        """Check if market should be processed"""
        # Skip certain market types
        if 'point' in market_name.lower() and 'game' in market_name.lower() and '-' in market_name:
            return False

        if 'set' in market_name.lower() and 'game' in market_name.lower():
            return False
        if ' - extra time' in market_name.lower() and ' including extra time' in market_name.lower():
            market_name=market_name.replace(' - extra time','').replace(' including extra time','')

        # Check for game-specific exclusions
        for number in range(1, 10):
            excluded_value = f'game {number}'
            if excluded_value in market_name.lower():
                return False

        return check_key(market_name)

    def _handle_duplicate_markets(self, betting_offer, all_odds_data, match_info,
                                  header_category, market_name, value_mappings):
        """Handle markets with potential duplicates"""
        try:
            # Extract handicap
            try:
                handicap_value = str(betting_offer['outcomes'][0]['line'] / 1000) if betting_offer['outcomes'][0][
                    'line'] else 'null'
            except (KeyError, TypeError):
                handicap_value = 'null'

            if market_name not in match_info['prices'][header_category]:
                match_info['prices'][header_category][market_name] = {}

            # Check if this market already has data (indicating duplicate)
            # try:
            #     if match_info['prices'][header_category][market_name][handicap_value]:
            #         # Handle as duplicate market
            #         return self._process_duplicate_market(
            #             betting_offer, all_odds_data, match_info,
            #             header_category, market_name, value_mappings
            #         )
            # except KeyError:
            #     pass

            # Process as regular market
            match_info['prices'][header_category][market_name][handicap_value] = {}

            for outcome in betting_offer['outcomes']:
                outcome_key = self._map_outcome_name(outcome['label'], match_info, value_mappings)
                odds_value = outcome.get('oddsFractional')

                if odds_value is not None:
                    odds_str = str(odds_value).strip().lower()

                    if odds_str == "evens":
                        odds_value = "2.0"  # Evens = 1/1 fractional = decimal 2.0
                    elif "/" in odds_str:  # Fractional odds
                        num, den = map(int, odds_str.split("/"))
                        odds_value = str(round((num / den) + 1, 1))
                    else:  # Whole number or decimal-like string
                        odds_value = str(round(float(odds_str), 1))
                else:
                    odds_value = None
                match_info['prices'][header_category][market_name][handicap_value][outcome_key] = odds_value

            return False  # Not a duplicate

        except Exception as handling_error:
            log_scraper_progress(
                self.custom_logger, 'MARKET_HANDLING_ERROR',
                f'Error handling market: {market_name}',
                error=handling_error
            )
            return False

    # def _process_duplicate_market(self, betting_offer, all_odds_data, match_info,
    #                               header_category, market_name, value_mappings):
    #     """Process market with duplicates by participant"""
    #     try:
    #         # Clear existing market data
    #         match_info['prices'][header_category][market_name] = {}
    #
    #         # Find all betting offers with same criterion ID
    #         criterion_id = betting_offer['criterion']['id']
    #         related_offers = [offer for offer in all_odds_data['betOffers']
    #                           if offer['criterion']['id'] == criterion_id]
    #
    #         # Process each related offer by participant
    #         for offer in related_offers:
    #             try:
    #                 handicap_value = str(offer['outcomes'][0]['line'] / 1000) if offer['outcomes'][0][
    #                     'line'] else 'null'
    #             except (KeyError, TypeError):
    #                 handicap_value = 'null'
    #
    #             participant_key = offer['outcomes'][0]['participant']
    #
    #
    #
    #
    #
    #             if participant_key not in match_info['prices'][header_category][market_name]:
    #                 match_info['prices'][header_category][market_name][participant_key] = {}
    #
    #             if handicap_value not in match_info['prices'][header_category][market_name][participant_key]:
    #                 match_info['prices'][header_category][market_name][participant_key][handicap_value] = {}
    #
    #             for outcome in offer['outcomes']:
    #                 outcome_key = self._map_outcome_name(outcome['label'], match_info, value_mappings)
    #                 odds_value = outcome.get('oddsAmerican')
    #                 match_info['prices'][header_category][market_name][participant_key][handicap_value][
    #                     outcome_key] = odds_value
    #
    #         return True  # Was a duplicate
    #
    #     except Exception as duplicate_error:
    #         log_scraper_progress(
    #             self.custom_logger, 'DUPLICATE_MARKET_ERROR',
    #             f'Error processing duplicate market: {market_name}',
    #             error=duplicate_error
    #         )
    #         return False

    def _map_outcome_name(self, outcome_label, match_info, value_mappings):
        """Map outcome label to standard format"""
        if outcome_label == match_info['competitor1']:
            return '1'
        elif outcome_label == 'Draw':
            return 'x'
        elif outcome_label == match_info['competitor2']:
            return '2'

        # Apply value mappings if available
        if value_mappings:
            for mapping_entry in value_mappings:
                try:
                    if mapping_entry['id'] == outcome_label.lower():
                        return mapping_entry['id']
                    elif 'maps' in mapping_entry and outcome_label.lower() in mapping_entry['maps']:
                        return mapping_entry['id']
                except (KeyError, AttributeError):
                    continue

        return outcome_label.lower()

    def _match_with_flashscore_data(self, unibet_match_info):
        """Match unibet data with flashscore data and prepare bulk update"""
        normalized_unibet_timestamp = normalize_timestamp_for_comparison(unibet_match_info['timestamp'])
        unibet_sport_normalized = (unibet_match_info['sport'].lower()
                                   .replace('-', '').replace(' ', ''))

        # Query database for potential matches
        potential_matches_cursor = self.matches_collection.find({
            "timestamp": normalized_unibet_timestamp,
        })

        for flashscore_match in potential_matches_cursor:
            # Check sport compatibility
            flashscore_sport_normalized = (flashscore_match['sport']
                                           .replace('-', '').replace(' ', ''))

            sport_matches = unibet_sport_normalized == flashscore_sport_normalized

            if sport_matches:
                matchup_compatibility = compare_matchups(
                    flashscore_match['competitor1'].lower(),
                    flashscore_match['competitor2'].lower(),
                    unibet_match_info['competitor1'].lower(),
                    unibet_match_info['competitor2'].lower()
                )

                if matchup_compatibility:
                    # Prepare bulk update operation
                    update_operation = UpdateOne(
                        {"match_id": flashscore_match["match_id"]},
                        {"$set": {"prices.unibet": unibet_match_info['prices']}}
                    )
                    self.bulk_update_operations.append(update_operation)

                    log_scraper_progress(
                        self.custom_logger, 'MATCH_FOUND',
                        f'Matched {unibet_match_info["competitor1"]} vs {unibet_match_info["competitor2"]}'
                    )
                    break

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
                    f'Error in odds mapping: {mapping_error}',
                    error=mapping_error
                )

        return [odds_value_mappings, standardized_key]

    def _process_bulk_updates_if_needed(self):
        """Process bulk updates if batch size is reached"""
        if len(self.bulk_update_operations) >= self.BULK_UPDATE_BATCH_SIZE:
            self._execute_bulk_updates()

    def _execute_bulk_updates(self):
        """Execute bulk update operations"""
        if not self.bulk_update_operations:
            return

        try:
            bulk_result = execute_bulk_write_operations(
                self.matches_collection,
                self.bulk_update_operations,
                "unibet_updates",
                self.custom_logger
            )

            self.bulk_update_operations = []  # Clear operations list

        except Exception as bulk_error:
            log_scraper_progress(
                self.custom_logger, 'BULK_UPDATE_ERROR',
                'Error executing bulk updates',
                error=bulk_error
            )
            self.bulk_update_operations = []  # Clear operations list even on error

    def close(self, reason):
        """Final cleanup and bulk update execution"""
        try:
            # Execute any remaining bulk operations
            if self.bulk_update_operations:
                log_scraper_progress(
                    self.custom_logger, 'FINAL_BULK_UPDATE',
                    f'Executing final {len(self.bulk_update_operations)} bulk operations'
                )
                self._execute_bulk_updates()

            log_scraper_progress(
                self.custom_logger, 'SCRAPER_COMPLETED',
                'Unibet scraper completed successfully',
                match_count=len(self.processed_matches)
            )

            log_scraper_progress(
                self.custom_logger, 'FINAL_STATS',
                f'Total: {self.total_matches_processed}, Success: {self.successful_matches}, '
                f'Failed: {self.failed_matches}, Unique odds: {len(self.unique_odds_keys)}'
            )

        except Exception as cleanup_error:
            log_scraper_progress(
                self.custom_logger, 'CLEANUP_ERROR',
                'Error during cleanup',
                error=cleanup_error
            )


if __name__ == '__main__':
    crawler_process = CrawlerProcess()
    crawler_process.crawl(UnibetOddsSpider)
    crawler_process.start()