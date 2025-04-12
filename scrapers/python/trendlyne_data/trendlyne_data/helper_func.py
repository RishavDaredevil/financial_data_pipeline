import requests
import json # Keep json import for later use in the spider
from rich import print # Keep rich print import as used in your original code

# --- Modified Helper Function ---
def non_curl_cffi_req(url, headers=None, data=None):
    """
    Makes a GET request using the requests library.

    Args:
        url (str): The URL to request.
        headers (dict, optional): Dictionary of HTTP Headers to send. Defaults to None.
        data (dict, optional): Dictionary, list of tuples, bytes, or file-like object
                               to send in the body. Defaults to None.

    Returns:
        requests.Response: The full Response object from the requests library.
                           Returns None if an exception occurs during the request.
    """
    if headers is None:
        headers = {}
    if data is None:
        data = {} # Note: For GET requests, 'data' is usually not used. 'params' is common.
                  # Keeping 'data' as per your original function signature.
    try:
        response = requests.request("GET", url=url, headers=headers, data=data, timeout=120) # Added timeout
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"[bold red]Request failed for {url}: {e}[/bold red]")
        return None # Return None if the request itself failed (network error, timeout, bad status)

# testing the fnction

# url = "https://trendlyne.com/fundamentals/tl-all-in-one-screener-data-get/?screenpk=661798&perPageCount=10000&groupType=all&groupName=all"
#
# payload = {}
# headers = {
#   'accept': 'application/json, text/javascript, */*; q=0.01',
#   'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
#   'cache-control': 'no-cache',
#   'pragma': 'no-cache',
#   'priority': 'u=1, i',
#   'referer': 'https://trendlyne.com/fundamentals/stock-screener/661798/backward-metrics/',
#   'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
#   'sec-ch-ua-mobile': '?0',
#   'sec-ch-ua-platform': '"Windows"',
#   'sec-fetch-dest': 'empty',
#   'sec-fetch-mode': 'cors',
#   'sec-fetch-site': 'same-origin',
#   'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
#   'x-requested-with': 'XMLHttpRequest',
#   'Cookie': '__stripe_mid=cfbee81c-e608-4244-8148-b5fa7bfd6a3f6a2264; .trendlyne=8pgukjbte9b1su8zrr3omzm0e1sg1d5u; csrftoken=ATFMQFil066z6eYT2FivrpjVt1WbrxicCXw4wkgY8ZZJYijqkPSc278rYIqvoHx2; csrftoken=ATFMQFil066z6eYT2FivrpjVt1WbrxicCXw4wkgY8ZZJYijqkPSc278rYIqvoHx2'
# }
#
# # Parse the JSON
# data = json.loads(non_curl_cffi_req(url,headers,payload))
#
# print(data['body']['tableHeaders'])
#
# no_of_headers = len(data['body']['tableHeaders'])
#
# print(no_of_headers)
#
