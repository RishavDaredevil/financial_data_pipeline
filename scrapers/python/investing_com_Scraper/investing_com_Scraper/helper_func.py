import json
from curl_cffi import requests
from rich import print

def fetch_historical_data(param_id, start_date, end_date):
    """
    Fetch historical financial data for a given parameter ID and date range.

    Args:
        param_id (str or int): The ID to be inserted in the URL endpoint.
        start_date (str): The start date in the required format (e.g., 'YYYY-MM-DD').
        end_date (str): The end date in the required format (e.g., 'YYYY-MM-DD').

    Returns:
        Response object from the API call.
    """
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
        'cache-control': 'no-cache',
        'domain-id': 'in',
        'origin': 'https://in.investing.com',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://in.investing.com/',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }

    params = {
        'start-date': start_date,
        'end-date': end_date,
        'time-frame': 'Daily',
        'add-missing-rows': 'false',
    }

    url = f'https://api.investing.com/api/financialdata/historical/{param_id}'
    response = requests.get(url, params=params, headers=headers)
    return response.json()