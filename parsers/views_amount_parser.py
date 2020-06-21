import requests
from bs4 import BeautifulSoup
import json


class GetUrlViewsAmount():
    def __init__(
        self,
        url: str,
        url_netloc: str,
    ):
        self.url = url
        self.url_netloc = url_netloc

    def get_youtube_views_amount(self, url):
        url_html_response = requests.get(url).text
        beautiful_soup = BeautifulSoup(url_html_response, 'lxml')
        views_amount = beautiful_soup.find(
            'div', class_='watch-view-count').text.replace('\xa0', '').split(' ')[0]

        return int(views_amount)

    def get_rutube_views_amount(self, url):
        url_html_response = requests.get(url).text
        beautiful_soup = BeautifulSoup(url_html_response, 'lxml')
        views_amount = beautiful_soup.find(
            'span', class_='video-info-card__view-count').text.replace(',', '')

        return int(views_amount)

    def get_vimeo_views_amount(self, url):
        url_html_response = requests.get(url).text
        beautiful_soup = BeautifulSoup(url_html_response, 'lxml')
        json_data = json.loads(beautiful_soup.find('script', type='application/ld+json').string)
        if 'interactionStatistic' in json_data[0]:
            for item in json_data[0]['interactionStatistic']:
                if 'WatchAction' in item['interactionType']:
                    return int(item['userInteractionCount'])
        return 0

    def get_views_amount_with_multiple_retries(self, get_views_amount):
        retry = 0
        result = None
        while not isinstance(result, int) and retry <= 3:
            retry += 1
            try:
                result = get_views_amount(self.url)
            except AttributeError:
                pass
        if not isinstance(result, int):
            return 0
        return result

    def execute(self):
        if self.url_netloc == 'rutube.ru':
            return self.get_views_amount_with_multiple_retries(self.get_rutube_views_amount)

        if self.url_netloc == 'vimeo.com':
            return self.get_views_amount_with_multiple_retries(self.get_vimeo_views_amount)

        if self.url_netloc == 'youtube.com':
            return self.get_views_amount_with_multiple_retries(self.get_youtube_views_amount)
