import json
from helper import parse_tipico_date, normalize_timestamp_for_comparison
from rapidfuzz import fuzz
import scrapy
import re
from datetime import datetime, timezone
from pymongo import MongoClient
from scrapy.crawler import CrawlerProcess


class BOVADA(scrapy.Spider):
    name = 'bovada_data'
    key_dict = set()
    all_sports = dict()

    custom_settings = {
        "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter"
    }
    main_hierarchy = []
    sport_name = ''
    all_matches = []
    headers = {
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
        client = MongoClient('mongodb://localhost:27017')
        db = client['betting']
        get_collection = db['ots']
        self.all_mapping_data = list(get_collection.find())
        get_country_collection = db['cos']
        self.all_country_data = list(get_country_collection.find())
        self.get_matches_data = db['matches_data']
        self.matches_data_collection = list(self.get_matches_data.find())

    def start_requests(self):
        url = 'https://services.bovada.lv/services/sports/event/v2/nav/A/description?lang=en'
        yield scrapy.Request(
            url=url,
            headers=self.headers,
            callback=self.parse
        )

    def parse(self, response, **kwargs):
        all_category_data = json.loads(response.text)['children']

        self.all_sports = ['/football', '/cricket', '/baseball', '/basketball', '/soccer', '/volleyball',
                           '/table-tennis', '/hockey', '/aussie-rules', '/rugby-union', '/rugby-league', '/snooker',
                           '/tennis']
        for category_data in all_category_data:
            if category_data['link'] in self.all_sports:
                self.sport_name = category_data['description']
                url = f'https://services.bovada.lv/services/sports/event/v2/nav/A/description{category_data["link"]}?azSorting=true&lang=en'
                yield scrapy.Request(
                    url=url,
                    headers=self.headers,

                    callback=self.scrap_subcategory,
                )

    def scrap_subcategory(self, response):
        all_subcategory_data = json.loads(response.text)['children']
        for subcategory_data in all_subcategory_data:
            yield from self.check_hierarchy(subcategory_data)

    def check_hierarchy(self, subcategory_data):
        """Fixed version that properly yields requests"""
        if 'leaf' in subcategory_data:
            if subcategory_data['leaf']:
                if 'link' in subcategory_data:
                    try:
                        match_data = [self.sport_name, subcategory_data['description'], subcategory_data['link']]
                        if subcategory_data['link'] not in self.main_hierarchy:
                            self.main_hierarchy.append(subcategory_data['link'])

                            url = f'https://www.bovada.lv/services/sports/event/coupon/events/A/description/{match_data[-1]}?marketFilterId=preMatchOnly=true&eventsLimit=5000&lang=en'
                            self.headers["x-channel"] = match_data[0][:4].upper()
                            yield scrapy.Request(
                                url=url,
                                headers=self.headers,
                                callback=self.scrap_match_data,
                                meta={'league_hierarchy': match_data}
                            )

                    except Exception as e:
                        print(f"Error adding leaf: {e}")
                        pass
            else:
                url = f'https://services.bovada.lv/services/sports/event/v2/nav/A/description{subcategory_data["link"]}?azSorting=true&lang=en'
                self.headers["x-channel"] = "desktop"
                yield scrapy.Request(
                    url=url,
                    headers=self.headers,
                    dont_filter=True,
                    callback=self.scrapy_hierarchy_category
                )

    def scrapy_hierarchy_category(self, response):
        """Fixed callback for processing category hierarchy"""
        try:
            all_hierarchy_data = json.loads(response.text)

            for data in all_hierarchy_data['children']:
                if 'leaf' in data:
                    try:
                        if data['leaf']:
                            if 'link' in data:

                                league_name_list = []

                                # Add parent descriptions
                                if 'parents' in all_hierarchy_data:
                                    for parent_data in all_hierarchy_data['parents']:
                                        league_name_list.append(parent_data['description'])

                                # Add current level description
                                if 'current' in all_hierarchy_data:
                                    league_name_list.append(all_hierarchy_data['current']['description'])

                                # Add leaf description and link
                                league_name_list.append(data['description'])

                                league_name_list.append(data['link'])
                                if data['link'] not in self.main_hierarchy:
                                    self.main_hierarchy.append(data['link'])
                                    url = f'https://www.bovada.lv/services/sports/event/coupon/events/A/description/{league_name_list[-1]}?marketFilterId=preMatchOnly=true&eventsLimit=5000&lang=en'
                                    self.headers["x-channel"] = league_name_list[0][:4].upper()
                                    yield scrapy.Request(
                                        url=url,
                                        headers=self.headers,
                                        callback=self.scrap_match_data,
                                        meta={'league_hierarchy': league_name_list}
                                    )




                        else:
                            # Recursively process non-leaf nodes
                            yield from self.check_hierarchy(data)

                    except Exception as e:
                        print(f"Error processing data item: {e}")
                        continue


        except json.JSONDecodeError as e:
            print(f"JSON decode error in scrapy_category: {e}")
        except Exception as e:
            print(f"General error in scrapy_category: {e}")

    def scrap_match_data(self, response):

        try:
            all_match_data = json.loads(response.text)

            if not all_match_data or not all_match_data[0].get('events'):
                print("No match data found")
                return

            for match_data in all_match_data[0]['events']:
                if match_data['competitors'] and not match_data['live']:
                    gmt_time = self.convert_time(match_data['startTime'])
                    try:
                        match_date = parse_tipico_date(gmt_time)
                    except Exception as e:
                        print(f"Error parsing date '{gmt_time}': {e}")
                        # Skip this match if date parsing fails
                        return

                    if all_match_data[0]['path'][-1]['description'].lower() == 'football':
                        sport = 'handball'
                    else:
                        sport = all_match_data[0]['path'][-1]['description']
                    if len(all_match_data[0]['path']) == 2:
                        country = all_match_data[0]['path'][0]['description']
                        group = all_match_data[0]['path'][0]['description']
                    else:
                        country = all_match_data[0]['path'][1]['description']
                        group = all_match_data[0]['path'][0]['description']

                    if match_data['awayTeamFirst']:
                        competitor1 = match_data['competitors'][1]['name']
                        competitor2 = match_data['competitors'][0]['name']
                    else:
                        competitor1 = match_data['competitors'][0]['name']
                        competitor2 = match_data['competitors'][1]['name']

                    temp_dict = {
                        'competitor1': competitor1,
                        'competitor2': competitor2,
                        'sport': sport,
                        "timestamp": match_date,
                        'country': country,
                        'group': group,
                        'odds': {}

                    }
                    # types of name
                    descrition_type_comp1 = re.split(r'vs|@', match_data['description'])[0].strip(' ')
                    descrition_type_comp2 = re.split(r'vs|@', match_data['description'])[1].strip(' ')
                    short_name_com_1 = match_data['competitors'][0].get('shortName', '').strip(' ')
                    short_name_com_2 = match_data['competitors'][1].get('shortName', '').strip(' ')
                    for group_data in match_data['displayGroups']:
                        if 'props' not in group_data['description'].lower():
                            for market_data in group_data['markets']:
                                if market_data['outcomes']:
                                    name = market_data["description"].replace(temp_dict['competitor1'], 'home').replace(
                                        temp_dict['competitor2'], 'away')
                                    if 'o/u' in name.lower() and '-' in name.lower():
                                        continue
                                    if 'period' in market_data:
                                        if market_data['period']['description'] != 'Regulation Time':
                                            name = name + ' - ' + market_data['period']["description"]
                                    check_key_name = self.check_key(name)
                                    if check_key_name:

                                        sub_key_name = self.check_header_name(name)
                                        if sub_key_name not in temp_dict['odds']:
                                            temp_dict['odds'][sub_key_name] = {}
                                        list_of_mapping = self.check_mapping_data_into_mongodb(name)
                                        all_key_value = list_of_mapping[0]
                                        name = list_of_mapping[1]
                                        if name not in temp_dict['odds'][sub_key_name]:
                                            temp_dict['odds'][sub_key_name][name] = {}
                                        if len(market_data['outcomes']) == 2:
                                            cp1 = market_data['outcomes'][0]['description'].replace(
                                                temp_dict['competitor1'], 'home').replace(temp_dict['competitor2'],
                                                                                          'away').strip(' ')
                                            cp2 = market_data['outcomes'][1]['description'].replace(
                                                temp_dict['competitor1'], 'home').replace(temp_dict['competitor2'],
                                                                                          'away').strip(' ')

                                            if descrition_type_comp1.lower() in cp1.lower() or cp1.lower() in descrition_type_comp1.lower() or short_name_com_1.lower() in cp1.lower() or cp1.lower() in short_name_com_1.lower():
                                                cp1 = '1'
                                            if descrition_type_comp2.lower() in cp2.lower() or cp2.lower() in descrition_type_comp2.lower() or short_name_com_2.lower() in cp2.lower() or cp2.lower() in short_name_com_2.lower():
                                                cp2 = '2'
                                            cp1 = self.check_competitor_mapping(cp1, all_key_value)
                                            cp2 = self.check_competitor_mapping(cp2, all_key_value)
                                            if 'over' in market_data['outcomes'][0]['description'].lower():
                                                if 'handicap' in market_data['outcomes'][0]['price']:
                                                    handicap = 'O ' + market_data['outcomes'][0]['price']['handicap']
                                                    if 'handicap2' in market_data['outcomes'][0]['price']:
                                                        handicap = 'O ' + market_data['outcomes'][0]['price'][
                                                            'handicap'] + ',' + market_data['outcomes'][0]['price'][
                                                                       'handicap2']
                                                else:
                                                    handicap = 'null'
                                                temp_dict['odds'][sub_key_name][name][handicap] = {
                                                    cp1: market_data['outcomes'][0]['price']['american'].replace('EVEN',
                                                                                                                 '+100'),
                                                    cp2: market_data['outcomes'][1]['price']['american'].replace('EVEN',
                                                                                                                 '+100')}

                                            else:
                                                if 'handicap' in market_data['outcomes'][0]['price']:
                                                    handicap = market_data['outcomes'][0]['price']['handicap']
                                                    if 'handicap2' in market_data['outcomes'][0]['price']:
                                                        handicap = market_data['outcomes'][0]['price'][
                                                                       'handicap'] + ',' + \
                                                                   market_data['outcomes'][0]['price']['handicap2']
                                                else:
                                                    handicap = 'null'

                                                temp_dict['odds'][sub_key_name][name][handicap] = {
                                                    cp1: market_data['outcomes'][0]['price']['american'].replace('EVEN',
                                                                                                                 '+100'),
                                                    cp2: market_data['outcomes'][1]['price']['american'].replace('EVEN',
                                                                                                                 '+100')}

                                        elif len(market_data['outcomes']) == 3:
                                            if 'handicap' in market_data['outcomes'][0]['price']:
                                                handicap = market_data['outcomes'][0]['price']['handicap']
                                                if 'handicap2' in market_data['outcomes'][0]['price']:
                                                    handicap = market_data['outcomes'][0]['price']['handicap'] + ',' + \
                                                               market_data['outcomes'][0]['price']['handicap2']
                                            else:
                                                handicap = 'null'

                                            cp1 = market_data['outcomes'][0]['description'].replace(
                                                temp_dict['competitor1'], 'home').replace(temp_dict['competitor2'],
                                                                                          'away').strip(' ')
                                            cp2 = market_data['outcomes'][1]['description'].replace(
                                                temp_dict['competitor1'], 'home').replace(temp_dict['competitor2'],
                                                                                          'away').strip(' ')
                                            cp3 = market_data['outcomes'][2]['description'].replace(
                                                temp_dict['competitor1'], 'home').replace(temp_dict['competitor2'],
                                                                                          'away').strip(' ')

                                            if descrition_type_comp1.lower() in cp1.lower() or cp1.lower() in descrition_type_comp1.lower() or short_name_com_1.lower() in cp1.lower() or cp1.lower() in short_name_com_1.lower():
                                                cp1 = '1'
                                            if descrition_type_comp2.lower() in cp2.lower() or cp2.lower() in descrition_type_comp2.lower() or short_name_com_2.lower() in cp2.lower() or cp2.lower() in short_name_com_2.lower():
                                                cp2 = '2'

                                            cp1 = self.check_competitor_mapping(cp1, all_key_value)
                                            cp2 = self.check_competitor_mapping(cp2, all_key_value)
                                            cp3 = self.check_competitor_mapping(cp3, all_key_value)
                                            temp_dict['odds'][sub_key_name][name][handicap] = {
                                                cp1: market_data['outcomes'][0]['price']['american'].replace('EVEN',
                                                                                                             '+100'),
                                                cp2: market_data['outcomes'][1]['price']['american'].replace('EVEN',
                                                                                                             '+100'),
                                                cp3: market_data['outcomes'][2]['price']['american'].replace('EVEN',
                                                                                                             '+100')}

                                        else:
                                            for outcome_data in market_data['outcomes']:
                                                check_competitor_name = outcome_data["description"].replace(
                                                    temp_dict['competitor1'], 'home').replace(temp_dict['competitor2'],
                                                                                              'away')
                                                check_competitor_name = check_competitor_name.replace(short_name_com_1,
                                                                                                      'home').replace(
                                                    short_name_com_2, 'away')
                                                check_competitor_name = check_competitor_name.replace(
                                                    descrition_type_comp1, 'home').replace(descrition_type_comp2,
                                                                                           'away')
                                                check_competitor_name = self.check_competitor_mapping(
                                                    check_competitor_name, all_key_value)

                                                if 'period' in outcome_data:
                                                    if outcome_data['period']['description'] == 'Regulation Time':
                                                        check_competitor_name = outcome_data["description"]
                                                    else:
                                                        check_competitor_name = outcome_data['description'] + ' - ' + \
                                                                                outcome_data['period']['description']
                                                if 'handicap' in outcome_data['price']:
                                                    handicap = outcome_data['price']['handicap']
                                                    if 'handicap2' in outcome_data['price']:
                                                        handicap = handicap + ',' + outcome_data['price']['handicap2']
                                                else:
                                                    handicap = 'null'

                                                if outcome_data['type'] == 'A':
                                                    handicap = handicap
                                                elif outcome_data['type'] == 'H':
                                                    handicap = self.negate_handicap_string(handicap)
                                                if outcome_data['type'] == 'O':
                                                    handicap = 'O ' + handicap
                                                elif outcome_data['type'] == 'U':
                                                    handicap = 'O ' + handicap
                                                if handicap not in temp_dict['odds'][sub_key_name][name]:
                                                    temp_dict['odds'][sub_key_name][name][handicap] = {}
                                                temp_dict['odds'][sub_key_name][name][handicap][check_competitor_name] = \
                                                outcome_data['price']['american'].replace('EVEN', '+100')

                    for matches_data in self.matches_data_collection:
                        bovada_timestamp = normalize_timestamp_for_comparison(temp_dict['timestamp'])
                        flashscore_timestamp = normalize_timestamp_for_comparison(matches_data['timestamp'])
                        sport_match = (temp_dict['sport'].lower().replace('-', '').replace(' ', '') ==
                                       matches_data['sport'].replace('-', '').replace(' ', ''))
                        timestamp_match = bovada_timestamp == flashscore_timestamp
                        if sport_match and timestamp_match:
                            result_dict = self.compare_matchups(
                                matches_data['competitor1'].lower(),
                                matches_data['competitor2'].lower(),
                                temp_dict['competitor1'].lower(),
                                temp_dict['competitor2'].lower()
                            )
                            if result_dict:
                                bovada_prices = temp_dict['odds']
                                # Update the matched flashscore entry with tipico prices
                                self.get_matches_data.update_one(
                                    {"match_id": matches_data["match_id"]},
                                    {"$set": {"prices.bovada": bovada_prices}}
                                )
                                break

                    self.all_matches.append(temp_dict)
        except Exception as e:
            print('error', e)

    def negate_handicap_string(self, handicap_str):
        try:
            parts = handicap_str.split(',')
            negated_parts = []
            for p in parts:
                val = float(p.strip()) * -1
                # Normalize -0.0 to 0.0
                if val == 0.0:
                    val = 0.0
                negated_parts.append(str(val))
            return ','.join(negated_parts)
        except ValueError:
            return '0'

    def check_mapping_data_into_mongodb(self, key):
        all_key_value = ''
        for mapping_data in self.all_mapping_data:
            try:
                if key.lower() == mapping_data['id']:
                    key = mapping_data['id']
                    all_key_value = mapping_data['ovs']
                    break
                elif 'maps' in mapping_data.keys() and key.lower() in mapping_data['maps']:
                    key = mapping_data['id']
                    all_key_value = mapping_data['ovs']
                    break
            except Exception as e:
                print('error ', e)
        return [all_key_value, key]

    def check_competitor_mapping(self, name, all_key_value):
        if 'home' in name.lower():
            name = '1'
        elif 'away' in name.lower():
            name = '2'
        elif 'tie' in name.lower():
            name = 'x'
        elif 'under' in name.lower():
            name = '-'
        elif 'over' in name.lower():
            name = '+'
        if all_key_value:
            for key_value in all_key_value:
                if key_value['id'] == name:
                    name = key_value['id']
                    break


                elif 'maps' in key_value.keys() and (
                        any(name.lower() in word.strip('[]') for word in key_value['maps']) or any(
                    word.strip('[]') in name.lower() for word in key_value['maps'])):
                    name = key_value['id']
                    break

        return name

    def convert_time(self, timestamp_ms):
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        gmt_str = dt.strftime('%d %b %Y %H:%M:%S GMT')
        return gmt_str

    def check_key(self, name):
        used_key = ['point', 'moneyline', 'spread', 'goal', 'total', '3 way', '3-way', 'correct score', 'over', 'under',
                    'asian', 'handicap', 'both team to score', 'double chance', 'draw no bet', 'half', 'set', 'inning',
                    'quarter', 'period']
        not_used_key = ['?', 'will', 'who', 'touchdowns', 'range', 'did', 'does', 'hour', 'minutes', 'halves', 'scorer',
                        'betting', '1 .ht', '1. ht', '1.ht', 'how', 'which', 'who', 'result', 'win', 'halftime', 'legs',
                        'tackles', 'attempts', 'final', 'frame', 'side', ')', '(', 'wides', 'highest', 'run', 'four',
                        'sixes', 'assists', 'made', 'home', 'away', 'rebounds', 'milestones', 'qualify', 'exact',
                        'bottom', 'top', 'wicket', 'at least', 'at end', 'at the end', 'before', 'after', 'fulltime',
                        'lead']
        if any(word in name.lower() for word in used_key) and not any(word in name.lower() for word in not_used_key):
            if 'half' in name.lower():
                if 'point spread' in name.lower() and 'o/u' in name.lower():
                    return False
                if 'winner' in name.lower():
                    return False
                return True
            elif 'first' not in name.lower():
                if 'point spread' in name.lower() and 'o/u' in name.lower():
                    return False
                if 'winner' in name.lower():
                    return False
                return True
        return False

    def check_header_name(self, key):

        # half_data = ['half']
        if 'half' in key.lower():
            h1 = ['1st half', 'first half', 'half no. 1',
                  'half number 1', 'half no.1']
            h2 = ['2nd half', 'second half', 'half no. 2',
                  'half number 2', 'half no.2']
            if any(word in key.lower() for word in h1) and not '2nd &' in key.lower():
                if 'first half' in key.lower():
                    if key.lower().count('first') == 1:
                        key_name = '1st Half'
                    else:
                        key_name = 'Full Match'
                elif 'first' in key.lower():
                    key_name = 'Full Match'
                else:
                    key_name = '1st Half'


            elif any(word in key.lower() for word in
                     h2) and 'first' not in key.lower() and not '1st &' in key.lower():
                key_name = '2nd Half'
            else:
                key_name = 'Full Match'

        elif 'quarter' in key.lower():
            q1 = ['1st quarter', 'quarter 1', 'quarter one', 'first quarter', '1.quarter',
                  '1. quarter', '1 .quarter', 'quarter no. 1', 'quarter number 1',
                  'quarter no.1']
            q2 = ['2nd quarter', 'quarter 2', 'quarter two', 'second quarter', '2.quarter',
                  '2. quarter', '2 .quarter', 'quarter no. 2', 'quarter number 2',
                  'quarter no.2']
            q3 = ['3rd quarter', 'quarter 3', 'quarter three', 'third quarter', '3.quarter',
                  '3. quarter', '3 .quarter', 'quarter no. 3', 'quarter number 3',
                  'quarter no.3']
            q4 = ['4th quarter', 'quarter 4', 'quarter four', 'fourth quarter', '4.quarter',
                  '4. quarter', '4 .quarter', 'quarter no. 4', 'quarter number 4',
                  'quarter no.4']
            if any(word in key.lower() for word in q1):
                if 'first quarter' in key.lower():
                    if key.lower().count('first') == 1:
                        key_name = '1st Quarter'
                    else:
                        key_name = 'Full Match'
                elif 'first' in key.lower():
                    key_name = 'Full Match'

                else:
                    key_name = '1st Quarter'


            elif any(word in key.lower() for word in q2) and 'first' not in key.lower():
                key_name = '2nd Quarter'
            elif any(word in key.lower() for word in q3) and 'first' not in key.lower():
                key_name = '3rd Quarter'
            elif any(word in key.lower() for word in q4) and 'first' not in key.lower():
                key_name = '4th Quarter'
            else:
                key_name = 'Full Match'
        elif 'set' in key.lower():
            s1 = ['1st set', 'set 1', 'set one', 'first set', 'set no. 1', 'set number 1',
                  '1.set', '1 .set', '1. set', 'set no.1']
            s2 = ['2nd set', 'set 2', 'set two', 'second set', 'set no. 2', 'set number 2',
                  '2.set', '2 .set', '2. set', 'set no.2']
            s3 = ['3rd set', 'set 3', 'set three', 'third set', 'set no. 3', 'set number 3',
                  '3.set', '3 .set', '3. set', 'set no.3']
            s4 = ['4th set', 'set 4', 'set four', 'fourth set', 'set no. 4', 'set number 4',
                  '4.set', '4 .set', '4. set', 'set no.4']
            s5 = ['5th set', 'set 5', 'set five', 'fifth set', 'set no. 5', 'set number 5',
                  '5.set', '5 .set', '5. set', 'set no.5']
            if any(word in key.lower() for word in s1):
                if 'first set' in key.lower():
                    if key.lower().count('first') == 1:
                        key_name = '1st Set'
                    else:
                        key_name = 'Full Match'
                elif 'first' in key.lower():
                    key_name = 'Full Match'
                else:
                    key_name = '1st Set'

            elif any(word in key.lower() for word in s2) and 'first' not in key.lower():
                key_name = '2nd Set'
            elif any(word in key.lower() for word in s3) and 'first' not in key.lower():
                key_name = '3rd Set'
            elif any(word in key.lower() for word in s4) and 'first' not in key.lower():
                key_name = '4th Set'
            elif any(word in key.lower() for word in s5) and 'first' not in key.lower():
                key_name = '5th Set'
            else:
                key_name = 'Full Match'


        elif 'inning' in key.lower():

            i1 = ['1st inning', 'first inning', 'one inning', 'inning 1', 'inning one',
                  'inning no. 1', 'inning number 1', 'inning no.1']
            i2 = ['2nd inning', 'second inning', 'two inning', 'inning 2', 'inning two',
                  'inning no. 2', 'inning number 2', 'inning no.2']
            i3 = ['3rd inning', 'third inning', 'three inning', 'inning 3', 'inning third',
                  'inning no. 3', 'inning number 3', 'inning no.3']
            i4 = ['4th inning', 'fourth inning', 'four inning', 'inning 4', 'inning fourth',
                  'inning no. 4', 'inning number 4', 'inning no.4']
            i5 = ['5th inning', 'fifth inning', 'five inning', 'inning 5', 'inning fifth',
                  'inning no. 5', 'inning number 5', 'inning no.5']
            i6 = ['6th inning', 'sixth inning', 'six inning', 'inning 6', 'inning sixth',
                  'inning no. 6', 'inning number 6', 'inning no.6']
            i7 = ['7th inning', 'seventh inning', 'seven inning', 'inning 7',
                  'inning seventh', 'inning no. 7', 'inning number 7', 'inning no.7']
            i8 = ['8th inning', 'eighth inning', 'eight inning', 'inning 8',
                  'inning eighth', 'inning no. 8', 'inning number 8', 'inning no.8']
            i9 = ['9th inning', 'ninth inning', 'nine inning', 'inning 9', 'inning ninth',
                  'inning no. 9', 'inning number 9', 'inning no.9']
            if any(word in key.lower() for word in i1):
                if 'first inning' in key.lower():
                    if key.lower().count('first') == 1:
                        key_name = '1st Innings'
                    else:
                        key_name = 'Full Match'
                elif 'first' in key.lower():
                    key_name = 'Full Match'
                else:
                    key_name = '1st Innings'
            elif any(word in key.lower() for word in i2) and 'first' not in key.lower():
                key_name = '2nd Innings'

            elif any(word in key.lower() for word in i3) and 'first' not in key.lower():
                key_name = '3rd Innings'

            elif any(word in key.lower() for word in i4) and 'first' not in key.lower():
                key_name = '4th Innings'

            elif any(word in key.lower() for word in i5) and 'first' not in key.lower():
                key_name = '5th Innings'

            elif any(word in key.lower() for word in i6) and 'first' not in key.lower():
                key_name = '6th Innings'

            elif any(word in key.lower() for word in i7) and 'first' not in key.lower():
                key_name = '7th Innings'

            elif any(word in key.lower() for word in i8) and 'first' not in key.lower():
                key_name = '8th Innings'

            elif any(word in key.lower() for word in i9) and 'first' not in key.lower():
                key_name = '9th Innings'

            else:
                key_name = 'Full Match'
        elif 'period' in key.lower():

            i1 = ['1st period', 'first period', 'one period', 'period 1', 'period one',
                  'period no. 1', 'period number 1', 'period no.1']
            i2 = ['2nd period', 'second period', 'two period', 'period 2', 'period two',
                  'period no. 2', 'period number 2', 'period no.2']
            i3 = ['3rd period', 'third period', 'three period', 'period 3', 'period third',
                  'period no. 3', 'period number 3', 'period no.3']

            if any(word in key.lower() for word in i1):
                if 'first period' in key.lower():
                    if key.lower().count('first') == 1:
                        key_name = '1st period'
                    else:
                        key_name = 'Full Match'
                elif 'first' in key.lower():
                    key_name = 'Full Match'
                else:
                    key_name = '1st period'
            elif any(word in key.lower() for word in i2) and 'first' not in key.lower():
                key_name = '2nd period'

            elif any(word in key.lower() for word in i3) and 'first' not in key.lower():
                key_name = '3rd period'

            else:
                key_name = 'Full Match'

        else:
            key_name = 'Full Match'

        return key_name

    def compare_matchups(self,
                         team1_a: str,
                         team2_a: str,
                         team1_b: str,
                         team2_b: str,
                         threshold: float = 80.0,
                         ) -> bool:
        sim_team1_sort = fuzz.token_sort_ratio(team1_a, team1_b)
        sim_team2_sort = fuzz.token_sort_ratio(team2_a, team2_b)
        sim_team1_set = fuzz.token_set_ratio(team1_a, team1_b)
        sim_team2_set = fuzz.token_set_ratio(team2_a, team2_b)
        combined_a = f"{team1_a} {team2_a}"
        combined_b = f"{team1_b} {team2_b}"
        sim_combined_sort = fuzz.token_sort_ratio(combined_a, combined_b)
        sim_combined_set = fuzz.token_set_ratio(combined_a, combined_b)

        result = {
            'flashcore_team1': team1_a,
            'tipico_team1': team1_b,
            'flashcore_team2': team2_a,
            'tipico_team2': team2_b,
            'is_match': '',
            'team1_score_sort': sim_team1_sort,
            'team2_score_sort': sim_team2_sort,
            'is_match_sort': sim_team1_sort >= threshold and sim_team2_sort >= threshold,
            'combined_score_sort': sim_combined_sort,
            'is_match_combined_sort': True if sim_combined_sort >= 80 else False,
            'team1_score_set': sim_team1_set,
            'team2_score_set': sim_team2_set,
            'is_match_set': sim_team1_set >= threshold and sim_team2_set >= threshold,
            'combined_score_set': sim_combined_set,
            'is_match_combined_set': True if sim_combined_set >= 80 else False
        }

        # return result
        check = True if sim_combined_set >= 80 else False
        return check

    def close(self, reason):
        print('hello')
        pass


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(BOVADA)
    process.start()
