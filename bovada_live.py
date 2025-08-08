import json
from  helper import check_sport_name, parse_tipico_date,normalize_timestamp_for_comparison,compare_matchups,check_key,check_header_name
import scrapy
import re
from datetime import datetime, timezone
from pymongo import MongoClient
from scrapy.crawler import CrawlerProcess




class BOVADA(scrapy.Spider):
    name = 'bovada_data'
    key_dict = set()
    all_sports=dict()


    custom_settings = {
        # 'DOWNLOAD_DELAY': 1,
        # 'CONCURRENT_REQUESTS': 1,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        # 'CONCURRENT_REQUESTS_PER_IP': 1,
        "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter"
    }

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
        self.proxy = "http://hafiz123-US-rotate:pucit123@p.webshare.io:80/"

    def start_requests(self):
        url = 'https://services.bovada.lv/services/sports/event/v2/nav/A/description?lang=en'
        yield scrapy.Request(
            url=url,
            headers=self.headers,
            callback=self.parse,
            meta={'proxy': self.proxy},
        )

    def parse(self, response, **kwargs):
        all_category_data = json.loads(response.text)['children']

        for category_data in all_category_data:
            if check_sport_name(category_data['description']):

                    self.sport_name = category_data['description']
                    url = f'https://services.bovada.lv/services/sports/event/v2/nav/A/description{category_data["link"]}?azSorting=true&lang=en'
                    yield scrapy.Request(
                        url=url,
                        headers=self.headers,

                        callback=self.scrap_subcategory,
                        meta={'proxy': self.proxy},
                    )

    def scrap_subcategory(self, response):
        all_subcategory_data = json.loads(response.text)['children']
        for subcategory_data in all_subcategory_data:
            yield from self.check_hierarchy(subcategory_data)

    def check_hierarchy(self, subcategory_data):
        """Fixed version that properly yields requests"""
        if 'leaf' in subcategory_data:
            if subcategory_data['leaf']:
                if 'link'in subcategory_data:
                    try:
                        match_data = [self.sport_name, subcategory_data['description'], subcategory_data['link']]


                        url = f'https://www.bovada.lv/services/sports/event/coupon/events/A/description/{match_data[-1]}?marketFilterId=def&liveOnly=true&eventsLimit=5000&lang=en'
                        self.headers["x-channel"] = match_data[0][:4].upper()
                        yield scrapy.Request(
                            url=url,
                            headers=self.headers,
                            callback=self.scrap_match_data,
                            meta={'league_hierarchy': match_data ,'proxy': self.proxy}
                        )

                    except Exception as e:
                        print(f"Error adding leaf: {e}")
                        pass
            else:
                url = f'https://services.bovada.lv/services/sports/event/v2/nav/A/description{subcategory_data["link"]}?azSorting=true&lang=en'
                self.headers["x-channel"]= "desktop"
                yield scrapy.Request(
                    url=url,
                    headers=self.headers,
                    dont_filter=True,
                    callback=self.scrapy_hierarchy_category,
                    meta = {'proxy': self.proxy},
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
                                url = f'https://www.bovada.lv/services/sports/event/coupon/events/A/description/{league_name_list[-1]}?marketFilterId=def&liveOnly=true&eventsLimit=5000&lang=en'
                                self.headers["x-channel"] = league_name_list[0][:4].upper()
                                yield scrapy.Request(
                                    url=url,
                                    headers=self.headers,
                                    callback=self.scrap_match_data,
                                    meta={'league_hierarchy': league_name_list,'proxy': self.proxy}
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
                if match_data['competitors'] and  match_data['live']:

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
                        'country': country,
                        'group': group,
                        'odds': {},
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
                                        if   market_data['period']['description'] == 'Live Regulation Time':
                                            name=name

                                        else:
                                            name = name + ' - ' + market_data['period']["description"].replace('Live ','')

                                    # name = name.replace(' - Live Match','').replace(' - Live Game','').replace(' - Live Regulation Time','')
                                    name = name.replace(competitor1, 'home').replace(competitor2, 'away')
                                    name = name.replace(short_name_com_1, 'home').replace(short_name_com_2, 'away')
                                    name = name.replace(descrition_type_comp1, 'home').replace(descrition_type_comp2,'away')

                                    check_key_name = check_key(name)
                                    if check_key_name and '(' not in name:

                                        list_of_mapping = self.check_mapping_data_into_mongodb(name)
                                        all_key_value = list_of_mapping[0]
                                        name = list_of_mapping[1]
                                        key_name_list = check_header_name(name)
                                        sub_key_name = key_name_list[0]
                                        name = key_name_list[1]
                                        if sub_key_name not in temp_dict['odds']:
                                            temp_dict['odds'][sub_key_name] = {}
                                        self.key_dict.add(name)
                                        if name not in temp_dict['odds'][sub_key_name]:
                                            temp_dict['odds'][sub_key_name][name] = {}
                                        if len(market_data['outcomes']) == 2:
                                            cp1 = market_data['outcomes'][0]['description'].replace(temp_dict['competitor1'],
                                                                                                    'home').replace(
                                                temp_dict['competitor2'], 'away').strip(' ')
                                            cp2 = market_data['outcomes'][1]['description'].replace(temp_dict['competitor1'],
                                                                                                    'home').replace(
                                                temp_dict['competitor2'], 'away').strip(' ')

                                            if descrition_type_comp1.lower() in cp1.lower() or cp1.lower() in descrition_type_comp1.lower() or short_name_com_1.lower() in cp1.lower() or cp1.lower() in short_name_com_1.lower():
                                                cp1 = '1'
                                            if descrition_type_comp2.lower() in cp2.lower() or cp2.lower() in descrition_type_comp2.lower() or short_name_com_2.lower() in cp2.lower() or cp2.lower() in short_name_com_2.lower():
                                                cp2 = '2'
                                            cp1 = self.check_competitor_mapping(cp1, all_key_value)
                                            cp2 = self.check_competitor_mapping(cp2, all_key_value)
                                            if 'over' in market_data['outcomes'][0]['description'].lower():
                                                if 'handicap' in market_data['outcomes'][0]['price']:
                                                    handicap =market_data['outcomes'][0]['price']['handicap']
                                                    if 'handicap2' in market_data['outcomes'][0]['price']:
                                                        handicap = str((float( market_data['outcomes'][0]['price']['handicap']) + float( market_data['outcomes'][0]['price']['handicap2'])) / 2)

                                                else:
                                                    handicap = 'null'
                                                temp_dict['odds'][sub_key_name][name][handicap] = {
                                                    cp1: round(float(market_data['outcomes'][0]['price']['decimal']), 1),
                                                    cp2: round(float(market_data['outcomes'][1]['price']['decimal']), 1)}
                                            else:
                                                if 'handicap' in market_data['outcomes'][0]['price']:
                                                    handicap = market_data['outcomes'][0]['price']['handicap']
                                                    if 'handicap2' in market_data['outcomes'][0]['price']:
                                                        handicap = str((float(market_data['outcomes'][0]['price']['handicap']) + float(market_data['outcomes'][0]['price']['handicap2'])) / 2)

                                                else:
                                                    handicap = 'null'

                                                temp_dict['odds'][sub_key_name][name][handicap] = {
                                                    cp1: round(float(market_data['outcomes'][0]['price']['decimal']), 1),
                                                    cp2: round(float(market_data['outcomes'][1]['price']['decimal']), 1)}

                                        elif len(market_data['outcomes']) == 3:
                                            if 'handicap' in market_data['outcomes'][0]['price']:
                                                handicap = market_data['outcomes'][0]['price']['handicap']
                                                if 'handicap2' in market_data['outcomes'][0]['price']:
                                                    handicap = str((float(market_data['outcomes'][0]['price']['handicap']) + float(market_data['outcomes'][0]['price']['handicap2'])) / 2)

                                            else:
                                                handicap = 'null'

                                            cp1 = market_data['outcomes'][0]['description'].replace(temp_dict['competitor1'],
                                                                                                    'home').replace(
                                                temp_dict['competitor2'], 'away').strip(' ')
                                            cp2 = market_data['outcomes'][1]['description'].replace(temp_dict['competitor1'],
                                                                                                    'home').replace(
                                                temp_dict['competitor2'], 'away').strip(' ')
                                            cp3 = market_data['outcomes'][2]['description'].replace(temp_dict['competitor1'],
                                                                                                    'home').replace(
                                                temp_dict['competitor2'], 'away').strip(' ')

                                            if descrition_type_comp1.lower() in cp1.lower() or cp1.lower() in descrition_type_comp1.lower() or short_name_com_1.lower() in cp1.lower() or cp1.lower() in short_name_com_1.lower():
                                                cp1 = '1'
                                            if descrition_type_comp2.lower() in cp2.lower() or cp2.lower() in descrition_type_comp2.lower() or short_name_com_2.lower() in cp2.lower() or cp2.lower() in short_name_com_2.lower():
                                                cp2 = '2'
                                            cp1 = self.check_competitor_mapping(cp1, all_key_value)
                                            cp2 = self.check_competitor_mapping(cp2, all_key_value)
                                            cp3 = self.check_competitor_mapping(cp3, all_key_value)
                                            temp_dict['odds'][sub_key_name][name][handicap] = {
                                                cp1: round(float(market_data['outcomes'][0]['price']['decimal']), 1),
                                                cp2: round(float(market_data['outcomes'][1]['price']['decimal']), 1),
                                                cp3: round(float(market_data['outcomes'][2]['price']['decimal']), 1)}

                                        else:
                                            for outcome_data in market_data['outcomes']:
                                                check_competitor_name = outcome_data["description"].replace(
                                                    temp_dict['competitor1'], 'home').replace(temp_dict['competitor2'], 'away')
                                                check_competitor_name = check_competitor_name.replace(short_name_com_1,
                                                                                                      'home').replace(
                                                    short_name_com_2, 'away')
                                                check_competitor_name = check_competitor_name.replace(descrition_type_comp1,
                                                                                                      'home').replace(
                                                    descrition_type_comp2, 'away')
                                                check_competitor_name = self.check_competitor_mapping(check_competitor_name,
                                                                                                      all_key_value)

                                                if 'period' in outcome_data:
                                                    if outcome_data['period']['description'] == 'Regulation Time':
                                                        check_competitor_name = outcome_data["description"]
                                                    else:
                                                        check_competitor_name = outcome_data['description'] + ' - ' + \
                                                                                outcome_data['period']['description']
                                                if 'handicap' in outcome_data['price']:
                                                    handicap = outcome_data['price']['handicap']
                                                    if 'handicap2' in outcome_data['price']:
                                                        handicap = str((float(handicap) + float(outcome_data['price']['handicap2']))/2)
                                                else:
                                                    handicap = 'null'
                                                if handicap !='null':
                                                    if outcome_data['type'] == 'A':
                                                        handicap = handicap
                                                    elif outcome_data['type'] == 'H':
                                                        handicap = self.negate_handicap_string(handicap)
                                                    if outcome_data['type'] == 'O':
                                                        handicap = handicap
                                                    elif outcome_data['type'] == 'U':
                                                        handicap = handicap
                                                if handicap not in temp_dict['odds'][sub_key_name][name]:
                                                    temp_dict['odds'][sub_key_name][name][handicap] = {}

                                                temp_dict['odds'][sub_key_name][name][handicap][check_competitor_name] = round(
                                                    float(outcome_data['price']['decimal']), 1)
                    #
                    for matches_data in self.matches_data_collection:
                        sport_match = (temp_dict['sport'].lower().replace('-', '').replace(' ', '') ==
                                       matches_data['sport'].replace('-', '').replace(' ', ''))
                        if sport_match :
                            result_dict = compare_matchups(
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
                                    {"$set": {
                                        "prices.bovada": bovada_prices,
                                        "status": "live",
                                              }}
                                )
                                break

                    self.all_matches.append(temp_dict)
        except Exception as e:
            print(e)


    def negate_handicap_string(self,handicap_str):
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






    def check_competitor_mapping(self,name,all_key_value):
        if 'home' in name.lower():
            name='1'
        elif 'away' in name.lower():
            name='2'
        elif 'tie' in name.lower():
            name='x'
        elif 'under' in name.lower():
            name='-'
        elif 'over' in name.lower():
            name='+'
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

    def close(self,reason):
        print('hello')
        pass


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(BOVADA)
    process.start()