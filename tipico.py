import json
import scrapy
from scrapy.crawler import CrawlerProcess
from helper import parse_tipico_date, normalize_timestamp_for_comparison,check_key,check_header_name,compare_matchups
from pymongo import MongoClient





class tipico(scrapy.Spider):
    name = "odds-tipico"
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
        self.proxy = "http://lXe53W9wSpWgBb2W:9rHVoP8UgHUmFoD0_country-at@geo.iproyal.com:12321/"

    def start_requests(self):
        url = 'https://sports.tipico.com/json/program/navigationTree/all'
        yield scrapy.Request(url=url, headers=self.headers, meta={'proxy': self.proxy})

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
                        headers=self.headers,
                        meta={'proxy': self.proxy}
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
                    callback=self.extract_single_match_details_data,
                    cookies=cookies,
                    headers=self.headers,
                    meta={'proxy': self.proxy}
                )

    def extract_single_match_details_data(self, response):
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
                if 'group' in match_data['event'].keys():
                    name_group='group'
                else:
                    name_group = 'groups'

                if match_data['event'][name_group][-1].strip(' ').lower() == 'football':
                    sport = 'soccer'
                else:
                    sport = match_data['event'][name_group][-1].strip(' ')

                temp_dic = {
                    'website': 'tipico',
                    'sport': sport,
                    'country': match_data['event'][name_group][-2],
                    'group': match_data['event'][name_group][0],
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
                            check_key_name = check_key(key)
                            if check_key_name:


                                # check key name in mongodb mapping collection
                                list_of_mapping = self.check_mapping_data_into_mongodb(key)
                                all_key_value = list_of_mapping[0]
                                key = list_of_mapping[1]

                                for ids in sub_keys['oddGroupIds']:

                                    if match_data['oddGroups'][str(ids)]['shortCaption']:
                                        if "over" in sub_keys['oddGroupTitle'].lower() or 'under' in sub_keys['oddGroupTitle'].lower() or 'total' in sub_keys['oddGroupTitle'].lower():
                                            sub_key = match_data['oddGroups'][str(ids)]['shortCaption']
                                            sub_key = 'O '+sub_key.split(' ')[0].replace(',', '.')
                                        else:
                                            sub_key = match_data['oddGroups'][str(ids)]['shortCaption']
                                            sub_key = sub_key.split(' ')[0].replace(',', '.')
                                    else:
                                        sub_key = 'null'

                                    key_name = check_header_name(key)
                                    self.key_dict.add(key)

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
                                                if key_value['id'] == odds_handicap.lower():
                                                    odds_handicap = key_value['id']
                                                    break

                                                elif 'maps' in key_value.keys() and odds_handicap.lower() in key_value[
                                                    'maps']:
                                                    odds_handicap = key_value['id']
                                                    break


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


                    if sport_match and timestamp_match:
                        result_dict = compare_matchups(
                            matches_data['competitor1'].lower(),
                            matches_data['competitor2'].lower(),
                            temp_dic['competitor1'].lower(),
                            temp_dic['competitor2'].lower()
                        )

                        if result_dict:
                            if matches_data['competitor1'].lower()=='fc tulsa':
                                l=1
                            tipico_prices = temp_dic['prices']
                            # Update the matched flashscore entry with tipico prices
                            self.get_matches_data.update_one(
                                {"match_id": matches_data["match_id"]},
                                {"$set": {"prices.tipico": tipico_prices}}
                            )
                            break

                self.all_matches.append(temp_dic)

    def check_mapping_data_into_mongodb(self,key):
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
    def close(self, reason):
        try:
            # store_data_into_mongodb(self.all_matches, 'website_data')
            print('ok')
        except Exception as e:
            print('error', e)


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(tipico)
    process.start()