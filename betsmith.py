import requests
from scrapy import Selector
import json
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017')
db = client['betting']
get_collection = db['ots']
all_mapping_data = list(get_collection.find())
url = 'https://d-cf.betsmithplayground.net/stc-1533379659/stc-1533379659'

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=0, i",
    "referer": "https://www.betsmith.com/",
    "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "iframe",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "cross-site",
    "sec-fetch-storage-access": "active",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
}

request = requests.get(
    url=url,
    headers=headers
)

sel = Selector(text=request.text)

raw_data = sel.css('script[name="config"]::text').get()

json_data = json.loads(raw_data.strip().replace('window.obgClientEnvironmentConfig = ', '')[:-1])
cate_json = json.loads(sel.css('script::text').getall()[-1])
categories = cate_json['NGRX_STATE']['sportsbook']['sportCatalog']['offering']['categories']
for key, values in categories.items():
    slug = values['slug']
    id_ = values['id']
    # url = f'https://d-cf.betsmithplayground.net/api/sb/v1/widgets/view/v1?categoryIds={category}&configurationKey=sportsbook.region.v2&excludedWidgetKeys=sportsbook.tournament.carousel&regionIds=1&slug=snooker/international&timezoneOffsetMinutes=300&priceFormats=1'
    category_url = f'https://d-cf.betsmithplayground.net/api/sb/v1/widgets/view/v1?categoryIds={id_}&configurationKey=sportsbook.category&excludedWidgetKeys=sportsbook.tournament.carousel&slug={slug}&timezoneOffsetMinutes=300&priceFormats=1'
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "brandid": "abbae10d-550b-4bb1-8f61-183b76f4e06f",
        "content-type": "application/json",
        "correlationid": "c5a44885-c7be-46a8-a746-378a6872cc1f",
        "marketcode": "en",
        "priority": "u=1, i",
        "referer": "https://d-cf.betsmithplayground.net/stc-1533379659/stc-1533379659/winter-sports?tab=outrights",
        "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-fetch-storage-access": "active",
        "sessiontoken": "ew0KICAiYWxnIjogIkhTMjU2IiwNCiAgInR5cCI6ICJKV1QiDQp9.ew0KICAianVyaXNkaWN0aW9uIjogIlVua25vd24iLA0KICAidXNlcklkIjogIjExMTExMTExLTExMTEtMTExMS0xMTExLTExMTExMTExMTExMSIsDQogICJsb2dpblNlc3Npb25JZCI6ICIxMTExMTExMS0xMTExLTExMTEtMTExMS0xMTExMTExMTExMTEiDQp9.yuBO_qNKJHtbCWK3z04cEqU59EKU8pZb2kXHhZ7IeuI",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "x-obg-channel": "Web",
        "x-obg-device": "Desktop",
        "x-sb-app-version": "7.19.6-a56d324",
        "x-sb-channel": "Web",
        "x-sb-country-code": "NO",
        "x-sb-currency-code": "EUR",
        "x-sb-device-type": "Desktop",
        "x-sb-frame-ancestors": "https://www.betsmith.com",
        "x-sb-identifier": "SPORTSBOOK_REGION_V2_WIDGET_REQUEST",
        "x-sb-jurisdiction": "Mga",
        "x-sb-language-code": "en",
        "x-sb-parent-base-url": "https://www.betsmith.com",
        "x-sb-segment-id": "83da9e87-97c6-44ab-96e7-521c09f7908e",
        "x-sb-static-context-id": "stc-1533379659",
        "x-sb-type": "b2b",
        "x-sb-user-context-id": "stc-1533379659"
    }

    category_request = requests.get(
        url=category_url,
        headers=headers,
    )
    v1_json = category_request.json()
    pre_match = [v for v in v1_json['data']['widgets'] if v['key'] == 'sportsbook.category.prematch'][0]
    live = [v for v in v1_json['data']['widgets'] if v['key'] == 'sportsbook.category.live'][0]
    items = pre_match['data']['data']['items']
    for item in items:
        widgetRequest = item['widgetRequest']
        startsBefore = widgetRequest['startsBefore']
        startsOnOrAfter = widgetRequest['startsOnOrAfter']
        competitionIds = ','.join(widgetRequest['competitionIds'])
        day_url = f'https://d-cf.betsmithplayground.net/api/sb/v1/widgets/events-table/v2?categoryIds={id_}&competitionIds={competitionIds}&eventPhase=Prematch&eventSortBy=StartDate&maxMarketCount=2&pageNumber=1&startsBefore={startsBefore}&startsOnOrAfter={startsOnOrAfter}&priceFormats=1'
        day_request = requests.get(
            url=day_url,
            headers=headers,
        )
        events = day_request.json()['data']['events']
        templates = [
            "MW3W",
            "MTG2W25",
            "MTG2W",
            "1HTG",
            "T2HGOU",
            "BTTS",
            "DC",
            "MW2W",
            "MW3W1H",
            "M3WHCP"
        ]

        for event in reversed(events):
            participants = {}
            for participant in event['participants']:
                participants[participant['label'].lower()] = participant['side']

            event_id = event['id']
            for template in templates:
                event_url = f'https://d-cf.betsmithplayground.net/api/sb/v1/widgets/accordion/v1?eventId={event_id}&marketTemplateIds={template}'
                event_request = requests.get(
                    url=event_url,
                    headers=headers,
                )
                odds = event_request.json()['data']
                if odds:
                    selections = odds['accordions'][template]['selections']
                    for selection in selections:
                        participants[selection['participantLabel'].lower()]


                h = 1

        'data.widgets[1].data.data.items'
j = 1
h = 1


