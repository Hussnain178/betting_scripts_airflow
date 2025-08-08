import scrapy
import json
from pymongo import MongoClient
from scrapy.crawler import CrawlerProcess


class FlashscoreResults(scrapy.Spider):
    name = 'results-flashscore'
    all_sports = dict()
    all_countries = dict()
    all_matches = list()

    client = MongoClient('mongodb://localhost:27017')
    db = client['betting']
    get_matches_data = db['matches_data']
    status_keys = {
        '1': '&nbsp;',
        '45': 'To finish',
        '42': 'Awaiting updates',
        '2': 'Live',
        '12': '1st Half',
        '38': 'Half Time',
        '13': '2nd Half',
        '6': 'Extra Time',
        '7': 'Penalties',
        '46': 'Break Time',
        '3': 'Finished',
        '10': 'After Extra Time',
        '11': 'After Penalties',
        '9': 'Walkover',
        '43': 'Delayed',
        '36': 'Interrupted',
        '4': 'Postponed',
        '5': 'Cancelled',
        '37': 'Abandoned',
        '54': 'Awarded'
    }

    result_keys = {
        '1st-half': 'half-1',
        '2nd-half': 'half-2',
        '1st-quarter': 'quarter-1',
        '2nd-quarter': 'quarter-2',
        '3rd-quarter': 'quarter-3',
        '4th-quarter': 'quarter-4',
        '1st-period': 'third-1',
        '2nd-period': 'third-2',
        '3rd-period': 'third-3',
        'match': 'full-time'
    }

    custom_settings = {
        'CONCURRENT_REQUESTS': 50,
        "LOG_ENABLED": False
    }
    headers = {
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
    all_status = set()
    final_data = []

    def start_requests(self):
        url = 'https://www.flashscore.com/x/js/core_2_2188000000.js'
        yield scrapy.Request(
            url=url,
            callback=self.parse
        )

    def parse(self, response, **kwargs):
        self.all_sports = json.loads('{' + response.text.split('sport_list":{')[-1].split('},"')[0] + '}')
        for sport in self.all_sports.items():
            if sport[1] not in [14, 16, 23, 28, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41]:  # ids of racing sports, right now we are also ignoring darts, boxing, golf and mma
                for day in [0, ]:  # 0 means today
                    url = 'https://global.flashscore.ninja/2/x/feed/f_{}_{}_5_en_1'.format(sport[1], day)
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_season,
                        headers=self.headers
                    )

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
                unix_time = int(match.split('¬AD÷')[-1].split('¬')[0])
                match_status = self.status_keys[status]

                cp1 = match.split('¬CX÷')[-1].split('¬')[0]
                cp2 = match.split('¬FK÷')[-1].split('¬')[0]

                team1_goals = match.split('¬AG÷')[-1].split('¬')[0]
                team2_goals = match.split('¬AH÷')[-1].split('¬')[0]
                if match_status in {'1st Half', '2nd Half', 'After Extra Time', 'After Penalties', 'Awaiting updates', 'Live'}:
                    status = 'live'
                elif match_status in {'Cancelled', 'Postponed'}:
                    status = match_status
                    team1_goals = '-'
                    team2_goals = '-'
                elif match_status == 'Finished':
                    status = 'Finished'
                else:
                    continue
                self.get_matches_data.update_one(
                    {"match_id": match_id},
                    {"$set": {
                        "status": status,
                        'currentScore_competitor1': team1_goals,
                        'currentScore_competitor2': team2_goals

                    }})


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(FlashscoreResults)
    process.start()
