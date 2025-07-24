import scrapy
from scrapy.crawler import CrawlerProcess
import json
from fixed_helper import local_to_utc, date_conversion, store_data_into_mongodb
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import pytz


class FlashscoreResults(scrapy.Spider):
    name = 'fixture-flashscore'
    all_sports = dict()
    all_countries = dict()

    vd_name = 'flashscore.com'
    vd_idn = None

    all_matches = list()

    def __init__(self, proxy=0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        client = MongoClient('mongodb://localhost:27017')
        db = client['betting']
        get_country_collection = db['cos']
        self.all_country_data = list(get_country_collection.find())

        self.enable_proxy = int(proxy)
        self.headers = {
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

    def start_requests(self):
        url = 'https://www.flashscore.com/x/js/core_2_2188000000.js'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        self.all_sports = json.loads('{' + response.text.split('sport_list":{')[-1].split('},"')[0] + '}')

        for sport in self.all_sports.items():
            if sport[1] not in [14, 16, 23, 28, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
                                41]:  # ids of racing sports, right now we are also ignoring darts, boxing, golf and mma
                for day in [0, 1, 2, 3, 4, 5, 6, 7]:
                    url = 'https://global.flashscore.ninja/2/x/feed/f_{}_{}_5_en_1'.format(sport[1], day)
                    yield scrapy.Request(url=url, callback=self.parse_season, headers=self.headers)

    def get_date_from_unix(self, unix_timestamp):
        """
        Convert Unix timestamp to standardized UTC datetime object
        """
        # Create timezone-aware UTC datetime from Unix timestamp
        dt_object = datetime.fromtimestamp(unix_timestamp, tz=pytz.UTC)
        return dt_object

    def parse_season(self, response):
        sport_id = int(response.url.split('feed/f_')[-1].split('_')[0])
        sport = [v for v in self.all_sports if self.all_sports[v] == sport_id][0]
        country = ''
        league = ''

        for match in response.text.split('~'):
            if match.startswith('ZA÷'):
                if '¬ZY÷' in match:
                    country = match.split('¬ZY÷')[-1].split('¬')[0]
                else:
                    country = match.split('ZA÷')[-1].split('¬')[0].split(':')[0]
                league = ':'.join(match.split('ZA÷')[-1].split('¬')[0].split(':')[1:]).strip()

            if match.startswith('AA÷'):
                match_id = match.split('¬')[0].split('÷')[-1]
                status = match.split('¬AC÷')[-1].split('¬')[0]
                if status in ['1', ]:
                    unix_time = int(match.split('¬AD÷')[-1].split('¬')[0])
                    # Use timezone-aware comparison
                    current_utc = datetime.now(pytz.UTC)
                    match_utc = datetime.fromtimestamp(unix_time, tz=pytz.UTC)
                    
                    if current_utc > match_utc:
                        continue
                    
                    # Get standardized datetime object
                    match_date = self.get_date_from_unix(unix_time)

                    cp1 = match.split('¬AE÷')[-1].split('¬')[0]
                    cp2 = match.split('AF÷')[-1].split('¬')[0]
                    match_info = {
                        "match_id": match_id,
                        "sport": sport,
                        "country": country,
                        "group": league,
                        "timestamp": match_date,
                        "competitor1": cp1,
                        "competitor2": cp2,
                        "status": "sched",
                        "is_country": False,
                        "prices": {},
                    }
                    
                    if sport == 'soccer':
                        sport_ = 'football'
                    else:
                        sport_ = sport
                    match_link = f'https://www.flashscore.com/match/{sport_}/{match_id}/#/match-summary'
                    match_info['match_link'] = match_link
                    
                    for country_data in self.all_country_data:
                        if country_data['id'] == match_info["country"].lower():
                            match_info['is_country'] = True
                            break

                    self.all_matches.append(match_info)

    def close(self, reason):
        try:
            store_data_into_mongodb(self.all_matches, 'matches_data')
            print('ok')
        except Exception as e:
            print('error', e)


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(FlashscoreResults)
    process.start()