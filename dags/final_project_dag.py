import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urlparse
from datetime import timedelta
from typing import Optional

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago

NOT_VALID_URL = 'wrong domen'
NO_RESPONSE_URL = 'got no response'
SUCCESS_URL = 'perfect url'
ERROR = ':-('
VALID_DOMENS = ['youtube.com', 'habr.com', 'vimeo.com', 'pornhub.com', 'rutube.ru', 'pikabu.com']  # еще какой-то 6-ой домен, не могу найти какой!

GOOGLE_SHEET_CREDENTIALS_JSON = Variable.get('google_secret_key', deserialize_json=True)


default_args = {
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'final_project_dag',
    default_args=default_args,
    description='DAG updates urls viewers amount daily',
    schedule_interval='0 4 * * *',
)


def get_google_sheet_data():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        GOOGLE_SHEET_CREDENTIALS_JSON, scope)
    client = gspread.authorize(creds)
    sheet = client.open('airflow101.экселька').worksheet('Sheet1')

    google_sheet_data = sheet.get_all_records(head=2)
    return google_sheet_data


def append_url_info(
        total_urls_info: str,
        url: str,
        url_index: int,
        url_group: str,
        cell_data: Optional[str, int],
):
    total_urls_info[url] = []
    total_urls_info[url].append(url_index)
    total_urls_info[url].append(url_group)
    total_urls_info[url].append(cell_data)


def get_url_netloc(url: str):
    url_netloc = urlparse(url).netloc
    if url_netloc.startswith('www'):
      return url_netloc[4:]
    return url_netloc


def get_black_list_urls():
    """здесь делаем селект из базы и создаем black list."""
    return []


def process_urls():
    urls_data = get_google_sheet_data()
    total_urls_info = {}
    for row in urls_data:

        url = row['ссылка']

        blacklist_urls = get_black_list_urls()
        if url in blacklist_urls:
            continue

        url_netloc = get_url_netloc(url)
        if url_netloc not in VALID_DOMENS:
            append_url_info(total_urls_info, url, NOT_VALID_URL, ERROR)
            continue

        url_response = requests.get(url, allow_redirects=True)
        if url_response.status_code != 200:
            append_url_info(total_urls_info, url, NO_RESPONSE_URL, ERROR)

        else:
            views_amount = GetUrlViewsAmount(url, url_netloc).execute()
            append_url_info(total_urls_info, url, SUCCESS_URL, views_amount)

    return total_urls_info
