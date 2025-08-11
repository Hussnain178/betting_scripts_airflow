import json
from pymongo import MongoClient, UpdateOne
import scrapy
from scrapy.crawler import CrawlerProcess
from helper import (
    check_sport_name, check_key, check_header_name, compare_matchups,
    setup_scraper_logger, log_scraper_progress, execute_bulk_write_operations
)


class UnibetLiveOddsSpider(scrapy.Spider):
    name = "unibet-live-odds-scraper"

    # Configuration
    custom_settings = {
        'CONCURRENT_REQUESTS': 32,  # Lower for live data
    }

    BULK_UPDATE_BATCH_SIZE = 50  # Smaller batches for live updates

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
        self.custom_logger = setup_scraper_logger('unibet_live')
        log_scraper_progress(self.custom_logger, 'INIT', 'Initializing Unibet Live scraper')

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
        self.processed_live_matches = []
        self.unique_odds_keys = set()
        self.bulk_update_operations = []

        # Proxy configuration
        self.proxy_url = "http://hafiz123-MT-rotate:pucit123@p.webshare.io:80/"

        # Counters
        self.total_live_matches_processed = 0
        self.successful_live_matches = 0
        self.failed_live_matches = 0

        log_scraper_progress(
            self.custom_logger, 'INIT_COMPLETE',
            f'Loaded {len(self.odds_type_mappings)} odds mappings for live matches'
        )

    def start_requests(self):
        log_scraper_progress(self.custom_logger, 'START', 'Starting Unibet Live sports navigation')

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
                self.custom_logger, 'LIVE_SPORTS_NAVIGATION_PARSED',
                f'Found {len(available_sports)} sports in navigation for live matches'
            )

            valid_live_sports_count = 0
            for sport_data in available_sports:
                sport_name = sport_data['name']
                if check_sport_name(sport_name):
                    valid_live_sports_count += 1
                    normalized_sport_name = sport_name.replace(' ', '_').lower()
                    self.valid_sports.add(normalized_sport_name)

                    sport_live_matches_url = (
                        f'https://www.unibet.mt/sportsbook-feeds/views/filter/{normalized_sport_name}/all/matches'
                        f'?includeParticipants=true&useCombined=true'
                    )

                    yield scrapy.Request(
                        url=sport_live_matches_url,
                        headers=self.request_headers,
                        callback=self.extract_sport_live_data,
                        method='GET',
                        dont_filter=True,
                        meta={'proxy': self.proxy_url, 'sport_name': sport_name}
                    )

            log_scraper_progress(
                self.custom_logger, 'VALID_LIVE_SPORTS_FILTERED',
                f'Processing {valid_live_sports_count} valid sports for live matches'
            )

        except Exception as navigation_error:
            log_scraper_progress(
                self.custom_logger, 'LIVE_NAVIGATION_ERROR',
                'Failed to parse live sports navigation',
                error=navigation_error
            )

    def extract_sport_live_data(self, response):
        try:
            sport_data = json.loads(response.text)
            sport_name = response.meta.get('sport_name', 'unknown')

            try:
                match_groups = sport_data['layout']['sections'][1]['widgets'][0]['matches']['groups']
            except (KeyError, IndexError):
                log_scraper_progress(
                    self.custom_logger, 'NO_LIVE_MATCH_GROUPS',
                    f'No live match groups found for {sport_name}'
                )
                return

            if not match_groups:
                return

            # Get sport name from first group
            actual_sport_name = match_groups[0]['sport']

            log_scraper_progress(
                self.custom_logger, 'LIVE_SPORT_DATA_EXTRACTED',
                f'Processing LIVE matches for {actual_sport_name} ({len(match_groups)} groups)'
            )

            live_matches_in_sport = 0
            for match_group in match_groups:
                live_matches_in_sport += self._process_live_match_group(match_group, actual_sport_name)

            log_scraper_progress(
                self.custom_logger, 'LIVE_SPORT_COMPLETED',
                f'Sport: {actual_sport_name}, LIVE matches processed: {live_matches_in_sport}'
            )

        except Exception as extraction_error:
            log_scraper_progress(
                self.custom_logger, 'LIVE_SPORT_EXTRACTION_ERROR',
                f'Error extracting live sport data for {response.meta.get("sport_name", "unknown")}',
                error=extraction_error
            )

    def _process_live_match_group(self, match_group, sport_name):
        """Process LIVE matches within a group (league/country)"""
        live_matches_processed = 0

        try:
            if match_group.get('subGroups'):
                # Group has subgroups (leagues within countries)
                for subgroup in match_group['subGroups']:
                    for match_event in subgroup['events']:
                        if self._should_process_live_match(match_event):
                            self._process_individual_live_match(
                                match_event, sport_name,
                                match_group['name'], subgroup['name']
                            )
                            live_matches_processed += 1
            else:
                # Group has direct events (no subgroups)
                league_name = match_group['name']
                for match_event in match_group['events']:
                    if self._should_process_live_match(match_event):
                        self._process_individual_live_match(
                            match_event, sport_name,
                            league_name, league_name
                        )
                        live_matches_processed += 1

        except Exception as group_error:
            log_scraper_progress(
                self.custom_logger, 'LIVE_MATCH_GROUP_ERROR',
                f'Error processing live match group',
                error=group_error
            )

        return live_matches_processed

    def _should_process_live_match(self, match_event):
        """Check if match should be processed (LIVE matches only)"""
        return 'liveData' in match_event

    def _process_individual_live_match(self, match_event, sport_name, country_name, group_name):
        """Process individual LIVE match and prepare for odds extraction"""
        try:
            self.total_live_matches_processed += 1

            live_match_information = {
                'website': 'unibet',
                'sport': sport_name,
                'country': country_name,
                'group': group_name,
                'competitor1': match_event['event']['homeName'],
                'competitor2': match_event['event']['awayName'],
                'match_id': match_event['event']['id'],
                'status': 'live',
                'prices': {}
            }

            # Request detailed odds data for this LIVE match
            detailed_live_odds_url = (
                f"https://eu-offering-api.kambicdn.com/offering/v2018/ubca/betoffer/event/"
                f"{match_event['event']['id']}.json?lang=en_GB&market=ZZ&client_id=2&channel_id=1&includeParticipants=true"
            )

            yield scrapy.Request(
                url=detailed_live_odds_url,
                method='GET',
                dont_filter=True,
                headers=self.request_headers,
                callback=self.extract_live_match_odds_data,
                meta={
                    'live_match_info': live_match_information,
                    'proxy': self.proxy_url
                }
            )

        except Exception as live_match_error:
            self.failed_live_matches += 1
            log_scraper_progress(
                self.custom_logger, 'INDIVIDUAL_LIVE_MATCH_ERROR',
                'Error processing individual live match',
                error=live_match_error
            )

    def extract_live_match_odds_data(self, response):
        try:
            live_odds_data = json.loads(response.text)
            live_match_information = response.meta['live_match_info']

            if not live_odds_data.get('betOffers'):
                log_scraper_progress(
                    self.custom_logger, 'NO_LIVE_ODDS_DATA',
                    f'No live odds data for match {live_match_information["competitor1"]} vs {live_match_information["competitor2"]}'
                )
                return

            # Track ignored category IDs to handle duplicates
            ignored_category_id = 0

            # Process each betting offer for live match
            for betting_offer in live_odds_data['betOffers']:
                category_id = betting_offer['criterion']['id']

                # Skip duplicate categories
                if ignored_category_id == category_id:
                    continue

                # Process market name
                market_name = self._process_live_market_name(betting_offer, live_match_information)

                if not market_name or not self._is_valid_live_market(market_name):
                    continue

                # Get mapping data
                mapping_result = self._get_odds_mapping_data(market_name)
                odds_value_mappings = mapping_result[0]
                standardized_market_name = mapping_result[1]

                # Get header categorization
                header_result = check_header_name(standardized_market_name)
                market_header_category = header_result[0]
                final_market_name = header_result[1]

                if market_header_category not in live_match_information['prices']:
                    live_match_information['prices'][market_header_category] = {}

                self.unique_odds_keys.add(final_market_name.lower())

                # Process outcomes and check for duplicates
                duplicate_handling_result = self._handle_duplicate_live_markets(
                    betting_offer, live_odds_data, live_match_information,
                    market_header_category, final_market_name, odds_value_mappings
                )

                if duplicate_handling_result:
                    ignored_category_id = category_id

            # Try to match with flashscore data (no timestamp needed for live matches)
            self._match_live_data_with_flashscore(live_match_information)

            # Process bulk updates if needed
            self._process_bulk_updates_if_needed()

            self.processed_live_matches.append(live_match_information)
            self.successful_live_matches += 1

            if self.total_live_matches_processed % 25 == 0:
                log_scraper_progress(
                    self.custom_logger, 'LIVE_PROGRESS_UPDATE',
                    f'Processed {self.total_live_matches_processed} LIVE matches, '
                    f'Success: {self.successful_live_matches}, Failed: {self.failed_live_matches}'
                )

        except Exception as live_odds_extraction_error:
            self.failed_live_matches += 1
            log_scraper_progress(
                self.custom_logger, 'LIVE_ODDS_EXTRACTION_ERROR',
                'Error extracting live odds data',
                error=live_odds_extraction_error
            )

    def _process_live_market_name(self, betting_offer, match_info):
        """Process and normalize live market name"""
        market_name = (betting_offer['criterion']['label']
                       .replace(match_info['competitor1'], 'home')
                       .replace(match_info['competitor2'], 'away')
                       .replace('(3-way)', '3-way'))
        return market_name

    def _is_valid_live_market(self, market_name):
        """Check if live market should be processed"""
        # Skip certain market types for live matches
        if 'point' in market_name.lower() and 'game' in market_name.lower() and '-' in market_name:
            return False

        if 'set' in market_name.lower() and 'game' in market_name.lower():
            return False

        # Check for game-specific exclusions
        for number in range(1, 6):
            excluded_value = f'game {number}'
            if excluded_value in market_name.lower():
                return False

        return check_key(market_name)

    def _handle_duplicate_live_markets(self, betting_offer, all_live_odds_data, match_info,
                                       header_category, market_name, value_mappings):
        """Handle live markets with potential duplicates"""
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
            try:
                if match_info['prices'][header_category][market_name][handicap_value]:
                    # Handle as duplicate market
                    return self._process_duplicate_live_market(
                        betting_offer, all_live_odds_data, match_info,
                        header_category, market_name, value_mappings
                    )
            except KeyError:
                pass

            # Process as regular live market
            match_info['prices'][header_category][market_name][handicap_value] = {}

            for outcome in betting_offer['outcomes']:
                outcome_key = self._map_live_outcome_name(outcome['label'], match_info, value_mappings)
                odds_value = outcome.get('oddsAmerican')
                match_info['prices'][header_category][market_name][handicap_value][outcome_key] = odds_value

            return False  # Not a duplicate

        except Exception as handling_error:
            log_scraper_progress(
                self.custom_logger, 'LIVE_MARKET_HANDLING_ERROR',
                f'Error handling live market: {market_name}',
                error=handling_error
            )
            return False

    def _process_duplicate_live_market(self, betting_offer, all_live_odds_data, match_info,
                                       header_category, market_name, value_mappings):
        """Process live market with duplicates by participant"""
        try:
            # Clear existing market data
            match_info['prices'][header_category][market_name] = {}

            # Find all betting offers with same criterion ID
            criterion_id = betting_offer['criterion']['id']
            related_offers = [offer for offer in all_live_odds_data['betOffers']
                              if offer['criterion']['id'] == criterion_id]

            # Process each related offer by participant
            for offer in related_offers:
                try:
                    handicap_value = str(offer['outcomes'][0]['line'] / 1000) if offer['outcomes'][0][
                        'line'] else 'null'
                except (KeyError, TypeError):
                    handicap_value = 'null'

                participant_key = offer['outcomes'][0]['participant']

                if participant_key not in match_info['prices'][header_category][market_name]:
                    match_info['prices'][header_category][market_name][participant_key] = {}

                if handicap_value not in match_info['prices'][header_category][market_name][participant_key]:
                    match_info['prices'][header_category][market_name][participant_key][handicap_value] = {}

                for outcome in offer['outcomes']:
                    outcome_key = self._map_live_outcome_name(outcome['label'], match_info, value_mappings)
                    odds_value = outcome.get('oddsAmerican')
                    match_info['prices'][header_category][market_name][participant_key][handicap_value][
                        outcome_key] = odds_value

            return True  # Was a duplicate

        except Exception as duplicate_error:
            log_scraper_progress(
                self.custom_logger, 'DUPLICATE_LIVE_MARKET_ERROR',
                f'Error processing duplicate live market: {market_name}',
                error=duplicate_error
            )
            return False

    def _map_live_outcome_name(self, outcome_label, match_info, value_mappings):
        """Map live outcome label to standard format"""
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

    def _match_live_data_with_flashscore(self, unibet_live_match_info):
        """Match unibet LIVE data with flashscore data and prepare bulk update"""
        unibet_sport_normalized = (unibet_live_match_info['sport'].lower()
                                   .replace('-', '').replace(' ', ''))

        # Query database for potential matches (no timestamp needed for live matches)
        potential_matches_cursor = self.matches_collection.find({})

        for flashscore_match in potential_matches_cursor:
            # Check sport compatibility
            flashscore_sport_normalized = (flashscore_match['sport']
                                           .replace('-', '').replace(' ', ''))

            sport_matches = unibet_sport_normalized == flashscore_sport_normalized

            if sport_matches:
                matchup_compatibility = compare_matchups(
                    flashscore_match['competitor1'].lower(),
                    flashscore_match['competitor2'].lower(),
                    unibet_live_match_info['competitor1'].lower(),
                    unibet_live_match_info['competitor2'].lower()
                )

                if matchup_compatibility:
                    # Prepare bulk update operation for LIVE match
                    update_operation = UpdateOne(
                        {"match_id": flashscore_match["match_id"]},
                        {"$set": {
                            "prices.unibet": unibet_live_match_info['prices'],
                            # Optionally update status to live if needed
                            # "status": "live"
                        }}
                    )
                    self.bulk_update_operations.append(update_operation)

                    log_scraper_progress(
                        self.custom_logger, 'LIVE_MATCH_FOUND',
                        f'Matched LIVE {unibet_live_match_info["competitor1"]} vs {unibet_live_match_info["competitor2"]}'
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
                "unibet_live_updates",
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
                'Unibet Live scraper completed successfully',
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
    crawler_process.crawl(UnibetLiveOddsSpider)
    crawler_process.start()