import json
import scrapy
import re
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from scrapy.crawler import CrawlerProcess
from helper import (
    check_sport_name, parse_tipico_date, normalize_timestamp_for_comparison,
    compare_matchups, check_key, check_header_name, setup_scraper_logger,
    log_scraper_progress, execute_bulk_write_operations
)


class BovadaOddsSpider(scrapy.Spider):
    name = 'bovada-odds-scraper'

    # Configuration
    custom_settings = {
        'CONCURRENT_REQUESTS': 64,
    }

    BULK_UPDATE_BATCH_SIZE = 100

    request_headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "priority": "u=1, i",
        "referer": "https://services.bovada.lv/",
        "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "same-origin",
        "sec-fetch-site": "same-origin",
        "traceid": "8c607131-c846-4771-9bca-d3c3eca98641",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "x-channel": "desktop"
    }

    def __init__(self):
        super().__init__()

        # Initialize logger
        self.custom_logger = setup_scraper_logger('bovada')
        log_scraper_progress(self.custom_logger, 'INIT', 'Initializing Bovada scraper')

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
        self.processed_matches = []
        self.unique_odds_keys = set()
        self.bulk_update_operations = []
        self.current_sport_name = ''

        # Proxy configuration
        self.proxy_url = "http://hafiz123-US-rotate:pucit123@p.webshare.io:80/"

        # Counters
        self.total_matches_processed = 0
        self.successful_matches = 0
        self.failed_matches = 0

        log_scraper_progress(
            self.custom_logger, 'INIT_COMPLETE',
            f'Loaded {len(self.odds_type_mappings)} odds mappings, {len(self.country_data)} countries'
        )

    def start_requests(self):
        log_scraper_progress(self.custom_logger, 'START', 'Starting Bovada navigation scraping')

        navigation_url = 'https://services.bovada.lv/services/sports/event/v2/nav/A/description?lang=en'
        yield scrapy.Request(
            url=navigation_url,
            headers=self.request_headers,
            callback=self.parse_navigation,
            meta={'proxy': self.proxy_url},
        )

    def parse_navigation(self, response, **kwargs):
        try:
            all_category_data = json.loads(response.text)['children']
            log_scraper_progress(
                self.custom_logger, 'NAVIGATION_PARSED',
                f'Found {len(all_category_data)} sport categories'
            )

            sport_count = 0
            for category_data in all_category_data:
                if check_sport_name(category_data['description']):
                    sport_count += 1
                    self.current_sport_name = category_data['description']

                    subcategory_url = (
                        f'https://services.bovada.lv/services/sports/event/v2/nav/A/description'
                        f'{category_data["link"]}?azSorting=true&lang=en'
                    )

                    yield scrapy.Request(
                        url=subcategory_url,
                        headers=self.request_headers,
                        callback=self.parse_subcategories,
                        meta={'proxy': self.proxy_url},
                    )

            log_scraper_progress(
                self.custom_logger, 'SPORTS_FILTERED',
                f'Processing {sport_count} valid sports out of {len(all_category_data)}'
            )

        except Exception as parsing_error:
            log_scraper_progress(
                self.custom_logger, 'NAVIGATION_ERROR',
                'Failed to parse navigation',
                error=parsing_error
            )

    def parse_subcategories(self, response):
        try:
            subcategory_data = json.loads(response.text)['children']
            log_scraper_progress(
                self.custom_logger, 'SUBCATEGORY_PARSED',
                f'Found {len(subcategory_data)} subcategories for {self.current_sport_name}'
            )

            for subcategory in subcategory_data:
                yield from self._process_hierarchy_node(subcategory)

        except Exception as parsing_error:
            log_scraper_progress(
                self.custom_logger, 'SUBCATEGORY_ERROR',
                f'Failed to parse subcategories for {self.current_sport_name}',
                error=parsing_error
            )

    def _process_hierarchy_node(self, hierarchy_node):
        """Process hierarchy nodes recursively to find leaf matches"""
        if 'leaf' not in hierarchy_node:
            return

        if hierarchy_node['leaf']:
            # This is a leaf node - get matches
            if 'link' in hierarchy_node:
                try:
                    league_hierarchy_data = [
                        self.current_sport_name,
                        hierarchy_node['description'],
                        hierarchy_node['link']
                    ]

                    matches_url = (
                        f'https://www.bovada.lv/services/sports/event/coupon/events/A/description/'
                        f'{league_hierarchy_data[-1]}?marketFilterId=preMatchOnly=true&eventsLimit=5000&lang=en'
                    )

                    updated_headers = self.request_headers.copy()
                    updated_headers["x-channel"] = league_hierarchy_data[0][:4].upper()

                    yield scrapy.Request(
                        url=matches_url,
                        headers=updated_headers,
                        callback=self.extract_match_data,
                        meta={
                            'league_hierarchy': league_hierarchy_data,
                            'proxy': self.proxy_url
                        }
                    )

                except Exception as hierarchy_error:
                    log_scraper_progress(
                        self.custom_logger, 'HIERARCHY_ERROR',
                        f'Error processing leaf: {hierarchy_error}',
                        error=hierarchy_error
                    )
        else:
            # This is not a leaf node - need to go deeper
            deeper_url = (
                f'https://services.bovada.lv/services/sports/event/v2/nav/A/description'
                f'{hierarchy_node["link"]}?azSorting=true&lang=en'
            )

            updated_headers = self.request_headers.copy()
            updated_headers["x-channel"] = "desktop"

            yield scrapy.Request(
                url=deeper_url,
                headers=updated_headers,
                dont_filter=True,
                callback=self.parse_deeper_hierarchy,
                meta={'proxy': self.proxy_url}
            )

    def parse_deeper_hierarchy(self, response):
        """Parse deeper hierarchy levels"""
        try:
            hierarchy_data = json.loads(response.text)

            for node in hierarchy_data['children']:
                if 'leaf' in node:
                    try:
                        if node['leaf']:
                            if 'link' in node:
                                league_name_hierarchy = []

                                # Add parent descriptions
                                if 'parents' in hierarchy_data:
                                    for parent_data in hierarchy_data['parents']:
                                        league_name_hierarchy.append(parent_data['description'])

                                # Add current level description
                                if 'current' in hierarchy_data:
                                    league_name_hierarchy.append(hierarchy_data['current']['description'])

                                # Add leaf description and link
                                league_name_hierarchy.append(node['description'])
                                league_name_hierarchy.append(node['link'])

                                matches_url = (
                                    f'https://www.bovada.lv/services/sports/event/coupon/events/A/description/'
                                    f'{league_name_hierarchy[-1]}?marketFilterId=preMatchOnly=true&eventsLimit=5000&lang=en'
                                )

                                updated_headers = self.request_headers.copy()
                                updated_headers["x-channel"] = league_name_hierarchy[0][:4].upper()

                                yield scrapy.Request(
                                    url=matches_url,
                                    headers=updated_headers,
                                    callback=self.extract_match_data,
                                    meta={
                                        'league_hierarchy': league_name_hierarchy,
                                        'proxy': self.proxy_url
                                    }
                                )
                        else:
                            # Recursively process non-leaf nodes
                            yield from self._process_hierarchy_node(node)

                    except Exception as node_error:
                        log_scraper_progress(
                            self.custom_logger, 'DEEP_HIERARCHY_ERROR',
                            f'Error processing hierarchy node: {node_error}',
                            error=node_error
                        )
                        continue

        except json.JSONDecodeError as json_error:
            log_scraper_progress(
                self.custom_logger, 'DEEP_HIERARCHY_JSON_ERROR',
                'JSON decode error in deeper hierarchy',
                error=json_error
            )
        except Exception as general_error:
            log_scraper_progress(
                self.custom_logger, 'DEEP_HIERARCHY_GENERAL_ERROR',
                'General error in deeper hierarchy parsing',
                error=general_error
            )

    def extract_match_data(self, response):
        try:
            match_data_response = json.loads(response.text)

            if not match_data_response or not match_data_response[0].get('events'):
                log_scraper_progress(self.custom_logger, 'NO_MATCH_DATA', 'No match data found in response')
                return

            matches_in_league = match_data_response[0]['events']
            log_scraper_progress(
                self.custom_logger, 'MATCH_DATA_EXTRACTED',
                f'Processing {len(matches_in_league)} matches from league'
            )

            for individual_match in matches_in_league:
                if individual_match['competitors'] and not individual_match['live']:
                    self._process_single_match(individual_match, match_data_response[0])

        except Exception as extraction_error:
            log_scraper_progress(
                self.custom_logger, 'MATCH_EXTRACTION_ERROR',
                'Error extracting match data',
                error=extraction_error
            )

    def _process_single_match(self, match_info, league_data):
        """Process individual match and extract odds"""
        try:
            self.total_matches_processed += 1

            # Convert timestamp
            gmt_timestamp = self._convert_timestamp(match_info['startTime'])
            try:
                parsed_match_datetime = parse_tipico_date(gmt_timestamp)
            except Exception as date_error:
                log_scraper_progress(
                    self.custom_logger, 'DATE_PARSE_ERROR',
                    f'Error parsing date {gmt_timestamp}',
                    error=date_error
                )
                return

            # Determine sport name
            sport_name = self._normalize_sport_name(league_data['path'][-1]['description'])

            # Determine country and group
            if len(league_data['path']) == 2:
                country_name = league_data['path'][0]['description']
                group_name = league_data['path'][0]['description']
            else:
                country_name = league_data['path'][1]['description']
                group_name = league_data['path'][0]['description']

            # Determine competitor order
            if match_info['awayTeamFirst']:
                competitor1_name = match_info['competitors'][1]['name']
                competitor2_name = match_info['competitors'][0]['name']
            else:
                competitor1_name = match_info['competitors'][0]['name']
                competitor2_name = match_info['competitors'][1]['name']

            match_information = {
                'competitor1': competitor1_name,
                'competitor2': competitor2_name,
                'sport': sport_name,
                "timestamp": parsed_match_datetime,
                'country': country_name,
                'group': group_name,
                'odds': {}
            }

            # Extract various team name formats for replacement
            description_components = re.split(r'vs|@', match_info['description'])
            description_team1 = description_components[0].strip()
            description_team2 = description_components[1].strip()

            short_name_team1 = match_info['competitors'][0].get('shortName', '').strip()
            short_name_team2 = match_info['competitors'][1].get('shortName', '').strip()

            # Process odds data
            self._extract_odds_from_match(
                match_info, match_information,
                description_team1, description_team2,
                short_name_team1, short_name_team2
            )

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

        except Exception as match_error:
            self.failed_matches += 1
            log_scraper_progress(
                self.custom_logger, 'MATCH_PROCESS_ERROR',
                'Error processing individual match',
                error=match_error
            )

    def _normalize_sport_name(self, sport_name):
        """Normalize sport names to standard format"""
        if sport_name.lower() == 'football':
            return 'handball'
        else:
            return sport_name

    def _convert_timestamp(self, timestamp_milliseconds):
        """Convert timestamp from milliseconds to GMT string format"""
        datetime_object = datetime.fromtimestamp(timestamp_milliseconds / 1000, tz=timezone.utc)
        gmt_string = datetime_object.strftime('%d %b %Y %H:%M:%S GMT')
        return gmt_string

    def _extract_odds_from_match(self, match_info, match_data, desc_team1, desc_team2, short_team1, short_team2):
        """Extract odds information from match display groups"""
        for display_group in match_info['displayGroups']:
            if 'props' in display_group['description'].lower():
                continue

            for market_info in display_group['markets']:
                if not market_info['outcomes']:
                    continue

                # Process market name
                market_name = self._process_market_name(
                    market_info, match_data, desc_team1, desc_team2, short_team1, short_team2
                )

                if not market_name:
                    continue

                # Check if market is valid
                if not self._is_valid_market(market_name):
                    continue

                # Get mapping data
                mapping_result = self._get_odds_mapping_data(market_name)
                odds_value_mappings = mapping_result[0]
                standardized_market_name = mapping_result[1]

                # Get header categorization
                header_result = check_header_name(standardized_market_name)
                market_header_category = header_result[0]
                final_market_name = header_result[1]

                self.unique_odds_keys.add(final_market_name)

                # Initialize nested structure
                if market_header_category not in match_data['odds']:
                    match_data['odds'][market_header_category] = {}

                # Process outcomes based on count
                self._process_market_outcomes(
                    market_info, match_data, market_header_category, final_market_name,
                    odds_value_mappings, desc_team1, desc_team2, short_team1, short_team2
                )

    def _process_market_name(self, market_info, match_data, desc_team1, desc_team2, short_team1, short_team2):
        """Process and normalize market name"""
        market_name = market_info["description"].replace(
            match_data['competitor1'], 'home'
        ).replace(match_data['competitor2'], 'away')

        # Skip certain market types
        if 'o/u' in market_name.lower() and '-' in market_name.lower():
            return None

        # Handle period-specific markets
        if 'period' in market_info:
            if market_info['period']['description'] != 'Regulation Time':
                market_name = market_name + ' - ' + market_info['period']["description"]

        # Replace various team name formats
        market_name = market_name.replace(match_data['competitor1'], 'home').replace(match_data['competitor2'], 'away')
        market_name = market_name.replace(short_team1, 'home').replace(short_team2, 'away')
        market_name = market_name.replace(desc_team1, 'home').replace(desc_team2, 'away')

        return market_name

    def _is_valid_market(self, market_name):
        """Check if market should be processed"""
        if 'point' in market_name.lower() and 'game' in market_name.lower():
            return False

        # Check for game-specific exclusions
        for number in ['1st', '2nd', '3rd', '4th', '5th']:
            excluded_value = f'{number} game'
            if excluded_value in market_name.lower():
                return False

        return check_key(market_name)

    def _process_market_outcomes(self, market_info, match_data, header_category, market_name,
                                 value_mappings, desc_team1, desc_team2, short_team1, short_team2):
        """Process market outcomes based on number of outcomes"""
        outcomes_list = market_info['outcomes']

        if market_name not in match_data['odds'][header_category]:
            match_data['odds'][header_category][market_name] = {}

        if len(outcomes_list) == 2:
            self._process_two_outcome_market(
                outcomes_list, match_data, header_category, market_name,
                value_mappings, desc_team1, desc_team2, short_team1, short_team2
            )
        elif len(outcomes_list) == 3:
            self._process_three_outcome_market(
                outcomes_list, match_data, header_category, market_name,
                value_mappings, desc_team1, desc_team2, short_team1, short_team2
            )
        else:
            self._process_multiple_outcome_market(
                outcomes_list, market_info, match_data, header_category, market_name,
                value_mappings, desc_team1, desc_team2, short_team1, short_team2
            )

    def _process_two_outcome_market(self, outcomes, match_data, header_category, market_name,
                                    value_mappings, desc_team1, desc_team2, short_team1, short_team2):
        """Process market with exactly 2 outcomes"""
        outcome1_description = outcomes[0]['description'].replace(
            match_data['competitor1'], 'home'
        ).replace(match_data['competitor2'], 'away').strip()

        outcome2_description = outcomes[1]['description'].replace(
            match_data['competitor1'], 'home'
        ).replace(match_data['competitor2'], 'away').strip()

        # Map team names to standard format
        outcome1_mapped = self._map_competitor_name(
            outcome1_description, desc_team1, short_team1, '1'
        )
        outcome2_mapped = self._map_competitor_name(
            outcome2_description, desc_team2, short_team2, '2'
        )

        outcome1_mapped = self._apply_value_mapping(outcome1_mapped, value_mappings)
        outcome2_mapped = self._apply_value_mapping(outcome2_mapped, value_mappings)

        # Get handicap value
        handicap_value = self._extract_handicap_value(outcomes[0])

        match_data['odds'][header_category][market_name][handicap_value] = {
            outcome1_mapped: round(float(outcomes[0]['price']['decimal']), 1),
            outcome2_mapped: round(float(outcomes[1]['price']['decimal']), 1)
        }

    def _process_three_outcome_market(self, outcomes, match_data, header_category, market_name,
                                      value_mappings, desc_team1, desc_team2, short_team1, short_team2):
        """Process market with exactly 3 outcomes"""
        # Get handicap value
        handicap_value = self._extract_handicap_value(outcomes[0])

        outcome1_description = outcomes[0]['description'].replace(
            match_data['competitor1'], 'home'
        ).replace(match_data['competitor2'], 'away').strip()

        outcome2_description = outcomes[1]['description'].replace(
            match_data['competitor1'], 'home'
        ).replace(match_data['competitor2'], 'away').strip()

        outcome3_description = outcomes[2]['description'].replace(
            match_data['competitor1'], 'home'
        ).replace(match_data['competitor2'], 'away').strip()

        # Map team names
        outcome1_mapped = self._map_competitor_name(
            outcome1_description, desc_team1, short_team1, '1'
        )
        outcome2_mapped = self._map_competitor_name(
            outcome2_description, desc_team2, short_team2, '2'
        )

        outcome1_mapped = self._apply_value_mapping(outcome1_mapped, value_mappings)
        outcome2_mapped = self._apply_value_mapping(outcome2_mapped, value_mappings)
        outcome3_mapped = self._apply_value_mapping(outcome3_description, value_mappings)

        match_data['odds'][header_category][market_name][handicap_value] = {
            outcome1_mapped: round(float(outcomes[0]['price']['decimal']), 1),
            outcome2_mapped: round(float(outcomes[1]['price']['decimal']), 1),
            outcome3_mapped: round(float(outcomes[2]['price']['decimal']), 1)
        }

    def _process_multiple_outcome_market(self, outcomes, market_info, match_data, header_category,
                                         market_name, value_mappings, desc_team1, desc_team2,
                                         short_team1, short_team2):
        """Process market with multiple outcomes"""
        for individual_outcome in outcomes:
            competitor_name = individual_outcome["description"].replace(
                match_data['competitor1'], 'home'
            ).replace(match_data['competitor2'], 'away')

            competitor_name = competitor_name.replace(short_team1, 'home').replace(short_team2, 'away')
            competitor_name = competitor_name.replace(desc_team1, 'home').replace(desc_team2, 'away')
            competitor_name = self._apply_value_mapping(competitor_name, value_mappings)

            # Handle period-specific outcomes
            if 'period' in individual_outcome:
                if individual_outcome['period']['description'] == 'Regulation Time':
                    competitor_name = individual_outcome["description"]
                else:
                    competitor_name = (individual_outcome['description'] + ' - ' +
                                       individual_outcome['period']['description'])

            # Extract and process handicap
            handicap_value = self._extract_handicap_value(individual_outcome)

            # Handle handicap direction based on outcome type
            if handicap_value != 'null':
                if individual_outcome['type'] == 'A':
                    handicap_value = handicap_value
                elif individual_outcome['type'] == 'H':
                    handicap_value = self._negate_handicap_string(handicap_value)

            # Initialize nested structure
            if handicap_value not in match_data['odds'][header_category][market_name]:
                match_data['odds'][header_category][market_name][handicap_value] = {}

            match_data['odds'][header_category][market_name][handicap_value][competitor_name] = round(
                float(individual_outcome['price']['decimal']), 1
            )

    def _map_competitor_name(self, competitor_description, desc_team, short_team, standard_number):
        """Map competitor description to standard format"""
        desc_lower = competitor_description.lower()
        desc_team_lower = desc_team.lower()
        short_team_lower = short_team.lower()

        if (desc_team_lower in desc_lower or desc_lower in desc_team_lower or
                short_team_lower in desc_lower or desc_lower in short_team_lower):
            return standard_number

        return competitor_description

    def _apply_value_mapping(self, competitor_name, value_mappings):
        """Apply value mappings if available"""
        if 'home' in competitor_name.lower():
            return '1'
        elif 'away' in competitor_name.lower():
            return '2'
        elif 'tie' in competitor_name.lower():
            return 'x'
        elif 'under' in competitor_name.lower():
            return '-'
        elif 'over' in competitor_name.lower():
            return '+'

        if value_mappings:
            for mapping_entry in value_mappings:
                if mapping_entry['id'] == competitor_name:
                    return mapping_entry['id']
                elif ('maps' in mapping_entry and
                      (any(competitor_name.lower() in word.strip('[]') for word in mapping_entry['maps']) or
                       any(word.strip('[]') in competitor_name.lower() for word in mapping_entry['maps']))):
                    return mapping_entry['id']

        return competitor_name

    def _extract_handicap_value(self, outcome_info):
        """Extract handicap value from outcome"""
        if 'handicap' in outcome_info['price']:
            handicap = outcome_info['price']['handicap']
            if 'handicap2' in outcome_info['price']:
                handicap = str((float(handicap) + float(outcome_info['price']['handicap2'])) / 2)
            return handicap
        else:
            return 'null'

    def _negate_handicap_string(self, handicap_str):
        """Negate handicap string values"""
        try:
            parts = handicap_str.split(',')
            negated_parts = []
            for part in parts:
                val = float(part.strip()) * -1
                # Normalize -0.0 to 0.0
                if val == 0.0:
                    val = 0.0
                negated_parts.append(str(val))
            return ','.join(negated_parts)
        except ValueError:
            return '0'

    def _match_with_flashscore_data(self, bovada_match_info):
        """Match bovada data with flashscore data and prepare bulk update"""
        normalized_bovada_timestamp = normalize_timestamp_for_comparison(bovada_match_info['timestamp'])
        bovada_sport_normalized = (bovada_match_info['sport'].lower()
                                   .replace('-', '').replace(' ', ''))

        # Query database for potential matches instead of loading all into memory
        potential_matches_cursor = self.matches_collection.find({
            "timestamp": normalized_bovada_timestamp,
        })

        for flashscore_match in potential_matches_cursor:
            # Check sport compatibility
            flashscore_sport_normalized = (flashscore_match['sport']
                                           .replace('-', '').replace(' ', ''))

            sport_matches = bovada_sport_normalized == flashscore_sport_normalized

            if sport_matches:
                matchup_compatibility = compare_matchups(
                    flashscore_match['competitor1'].lower(),
                    flashscore_match['competitor2'].lower(),
                    bovada_match_info['competitor1'].lower(),
                    bovada_match_info['competitor2'].lower()
                )

                if matchup_compatibility:
                    # Prepare bulk update operation
                    update_operation = UpdateOne(
                        {"match_id": flashscore_match["match_id"]},
                        {"$set": {"prices.bovada": bovada_match_info['odds']}}
                    )
                    self.bulk_update_operations.append(update_operation)

                    log_scraper_progress(
                        self.custom_logger, 'MATCH_FOUND',
                        f'Matched {bovada_match_info["competitor1"]} vs {bovada_match_info["competitor2"]}'
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
                "bovada_updates",
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
                f'Bovada scraper completed successfully',
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
    crawler_process.crawl(BovadaOddsSpider)
    crawler_process.start()