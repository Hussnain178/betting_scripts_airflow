import json
from rapidfuzz import fuzz
import scrapy
from scrapy.crawler import CrawlerProcess
from fixed_helper import local_to_utc, date_conversion, store_data_into_mongodb, parse_tipico_date, normalize_timestamp_for_comparison
from pymongo import MongoClient
import pytz
from datetime import datetime


def compare_matchups(
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


class tipico(scrapy.Spider):
    competitor_data = list()
    s = set()
    name = "odds-tipico"
    comparsion = list()
    key_dict = set()
    all_matches = list()
    headers = {
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
        client = MongoClient('mongodb://localhost:27017')
        db = client['betting']
        get_collection = db['ots']
        self.all_mapping_data = list(get_collection.find())
        get_country_collection = db['cos']
        self.all_country_data = list(get_country_collection.find())
        self.get_matches_data = db['matches_data']
        self.matches_data_collection = list(self.get_matches_data.find())
        get_competitor_data = db['competitor_mapping']
        self.competitor_mapping = list(get_competitor_data.find())

    def start_requests(self):
        url = 'https://sports.tipico.com/json/program/navigationTree/all'
        yield scrapy.Request(url=url, headers=self.headers)

    def parse(self, response, **kwargs):
        json_data = json.loads(response.text)
        sports = json_data['children']
        for sport in sports:
            countries = sport['children']

            for country in countries:
                groups = country['children']
                for group in groups:
                    group_id = group['groupId']

                    league_matches = 'https://sports.tipico.com/json/program/selectedEvents/all/{}?oneSectionResult=true&maxMarkets=2&language=de'.format(
                        group_id)
                    yield scrapy.Request(
                        url=league_matches,
                        callback=self.extract_matches,
                        headers=self.headers
                    )

    def extract_matches(self, response):
        cookies = {
            "language": 'en',
        }

        all_matches = json.loads(response.text)['SELECTION']['events'].values()
        for match in all_matches:
            match_id = match['id']

            if match['team1Id']:
                match_details_url = 'https://sports.tipico.com/json/services/event/{}'.format(match_id)
                yield scrapy.Request(
                    url=match_details_url,
                    callback=self.extract_details_data,
                    cookies=cookies,
                    headers=self.headers
                )

    def extract_details_data(self, response):
        match_data = json.loads(response.text)

        if not match_data['event']['live']:
            match_date_str = match_data['event'].get('startDate', '')
            if match_date_str:
                # Use the new parse_tipico_date function for consistent parsing
                try:
                    match_date = parse_tipico_date(match_date_str)
                except Exception as e:
                    print(f"Error parsing date '{match_date_str}': {e}")
                    # Skip this match if date parsing fails
                    return

                if match_data['event']['group'][-1].strip(' ').lower() == 'football':
                    sport = 'soccer'
                else:
                    sport = match_data['event']['group'][-1].strip(' ')

                temp_dic = {
                    'website': 'tipico',
                    'sport': sport,
                    'country': match_data['event']['group'][-2],
                    'group': match_data['event']['group'][0],
                    'timestamp': match_date,  # This is now a timezone-aware UTC datetime
                    'match_id': match_data['event']['id'],
                    'competitor1': match_data['event']['team1'],
                    'competitor2': match_data['event']['team2'],
                    'status': 'sched',
                    'prices': {},
                    "is_country": False,
                }
                
                # Check if country exists
                for country_data in self.all_country_data:
                    if country_data['id'] == temp_dic["country"].lower():
                        temp_dic['is_country'] = True
                        break
                cat_dict = {
                    str(v['id']): v['name'] for v in match_data.get('categories', {}) if v['id'] < 100
                }
                for odds_group_key in match_data.get('categoryOddGroupMapSectioned', {}).keys():
                    if odds_group_key in cat_dict.keys():

                        for sub_keys in match_data['categoryOddGroupMapSectioned'][odds_group_key]:

                            key = sub_keys['oddGroupTitle'].replace(match_data['event']['team1'], 'home').replace(
                                match_data['event']['team2'], 'away')

                            try:
                                self.key_dict.add(key)
                            except Exception as e:
                                print(e)
                                l = 1

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
                                except:
                                    l = 1

                            for ids in sub_keys['oddGroupIds']:
                                if match_data['oddGroups'][str(ids)]['shortCaption']:
                                    sub_key = match_data['oddGroups'][str(ids)]['shortCaption']
                                    sub_key = sub_key.split(' ')[0].replace(',', '.')
                                else:
                                    sub_key = 'null'

                                #   HALF

                                checked = ['top', 'wicket', 'halftime', 'score', 'at least', 'at end', 'at the end',
                                           'before', 'after', 'total', 'fulltime', 'both', 'lead', ]
                                if not any(word in key.lower() for word in checked):
                                    half_data = ['half', '.h', '. h']
                                    if any(word in key.lower() for word in half_data):
                                        h1 = ['1st half', 'first half', '1.h', '1. h', '1 .h', 'half no. 1',
                                              'half number 1', 'half no.1']
                                        h2 = ['2nd half', 'second half', '2.h', '2. h', '2 .h', 'half no. 2',
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

                                    else:
                                        key_name = 'Full Match'
                                else:
                                    key_name = 'Full Match'
                                if key_name not in temp_dic['prices'].keys():
                                    temp_dic['prices'][key_name] = {}
                                if key not in temp_dic['prices'][key_name].keys():
                                    temp_dic['prices'][key_name][key] = {}
                                if sub_key not in temp_dic['prices'][key_name][key].keys():
                                    temp_dic['prices'][key_name][key][sub_key] = {}
                                for data_id in match_data['oddGroupResultsMap'][str(ids)]:
                                    odds_handicap = match_data['results'][str(data_id)]['caption']
                                    if all_key_value:
                                        for key_value in all_key_value:
                                            try:
                                                if key_value['id'] == odds_handicap.lower():
                                                    # odds_handicap = key_value['id']
                                                    break

                                                elif 'maps' in key_value.keys() and odds_handicap.lower() in key_value[
                                                    'maps']:
                                                    odds_handicap = key_value['id']
                                                    break
                                            except:
                                                k = 1
                                    else:
                                        k = 1
                                    odds_price = match_data['results'][str(data_id)]['quoteFloatValue']
                                    temp_dic['prices'][key_name][key][sub_key][odds_handicap] = odds_price


                # Match with flashscore data using normalized timestamps
                for matches_data in self.matches_data_collection:
                    # Normalize both timestamps for comparison
                    tipico_timestamp = normalize_timestamp_for_comparison(temp_dic['timestamp'])
                    flashscore_timestamp = normalize_timestamp_for_comparison(matches_data['timestamp'])
                    
                    # Compare sports and timestamps
                    sport_match = (temp_dic['sport'].lower().replace('-', '').replace(' ', '') == 
                                 matches_data['sport'].replace('-', '').replace(' ', ''))
                    
                    # Use exact timestamp comparison for timezone-aware datetimes
                    timestamp_match = tipico_timestamp == flashscore_timestamp
                    
                    if sport_match and timestamp_match :
                        result_dict = compare_matchups(
                            matches_data['competitor1'].lower(),
                            matches_data['competitor2'].lower(),
                            temp_dic['competitor1'].lower(), 
                            temp_dic['competitor2'].lower()
                        )
                        
                        if result_dict:
                            tipico_prices = temp_dic['prices']
                            self.s.add(matches_data["match_id"])
                            # Update the matched flashscore entry with tipico prices
                            self.get_matches_data.update_one(
                                {"match_id": matches_data["match_id"]},
                                {"$set": {"prices.tipico": tipico_prices}}
                            )
                            break
                
                self.all_matches.append(temp_dic)

    def close(self, reason):
        try:
            store_data_into_mongodb(self.all_matches, 'website_data')
            print('ok')
        except Exception as e:
            print('error', e)


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(tipico)
    process.start()