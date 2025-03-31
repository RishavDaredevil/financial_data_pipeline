import http.client
import json
from rich import print
from codecs import encode
import csv
from bs4 import BeautifulSoup
 
BOUNDARY = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
HEADERS = {
    'User-Agent': 'PostmanRuntime/7.30.1',
    'x-requested-with': 'XMLHttpRequest',
    'Cookie': '__cf_bm=NF9eSYEUWmtdv65CHwB93hCdT.WRE_d4iNpmR30o.v0-1676294618-0-AYM6y0RiHGfESmbt61eiam4R1NBuFibW2W71ttdOuSTBogFDoXSTzd07IUeFSNYaM4hNA9R5252aklB7z4C65q8=; firstUdid=0; smd=cb703e1968c6d724ea9d4228e82600fa-1676294628; udid=cb703e1968c6d724ea9d4228e82600fa; PHPSESSID=4qn4pjpfu2a2evaa9n86nv8fph; __cflb=02DiuGRugds2TUWHMkimMbdK71gXQtrnhM92GyeHyjJnY',
    'Content-type': 'multipart/form-data; boundary={}'.format(BOUNDARY)
}
 
 
def add_form_data(name: str, value: str) -> []:
    result = []
    # boundary = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
 
    result.append(encode('--' + BOUNDARY))
    result.append(encode(f'Content-Disposition: form-data; name={name};'))
    result.append(encode('Content-Type: {}'.format('text/plain')))
    result.append(encode(''))
    result.append(encode(f"{value}"))
    return result
 
 
def construct_request(page: int = 0):
    data_list = []

    # List of country codes to include. Adjust as needed.
    country_codes = [
        '5', '17', '43',  # original ones
        '25', '32', '6', '37', '72', '22', '17', '39', '14', '10',
        '35', '43', '56', '36', '110', '11', '26', '12', '4', '5'
    ]
    
    for code in country_codes:
        data_list.extend(add_form_data('country[]', code))
 
    data_list.extend(add_form_data('dateFrom', '2025-03-31'))
 
    data_list.extend(add_form_data('dateTo', '2025-12-31'))
 
    data_list.extend(add_form_data('timeZone', '8'))
 
    data_list.extend(add_form_data('timeFilter', 'timeRemain'))
 
    data_list.extend(add_form_data('currentTab', 'custom'))
 
    data_list.extend(add_form_data('submitFilters', '1'))
 
    data_list.extend(add_form_data('limit_from', str(page)))
 
    data_list.append(encode('--' + BOUNDARY + '--'))
    data_list.append(encode(''))
    return b'\r\n'.join(data_list)


def get_calendar_rows(start):
    # Use the start parameter to construct the request payload
    payload = construct_request(start)
    conn = http.client.HTTPSConnection("www.investing.com")
    conn.request("POST", "/economic-calendar/Service/getCalendarFilteredData", payload, HEADERS)
    res = conn.getresponse()
    data = res.read()
    json_data = json.loads(data.decode("utf-8"))
    soup = BeautifulSoup(json_data['data'], 'html.parser')
    return soup.find_all('tr')


def process_calendar_data(rows):
    """Extract event data along with the current date from the list of rows."""
    results = []
    current_date = None

    for row in rows:
        # Update the current_date when the row contains a date td
        date_td = row.find('td', class_='theDay')
        if date_td:
            current_date = date_td.get_text(strip=True)
            continue
        
        # Process event rows (those with the class "js-event-item")
        if row.get('class') and "js-event-item" in row.get('class'):
            cols = row.find_all('td')
            if len(cols) < 7:
                continue

            time = cols[0].get_text(strip=True)
            cur = cols[1].get_text(strip=True)
            imp = cols[2].get_text(strip=True)
            event = cols[3].get_text(strip=True)
            actual = cols[4].get_text(strip=True)
            forecast = cols[5].get_text(strip=True)
            previous = cols[6].get_text(strip=True)

            results.append({
                'Date': current_date,
                'Time': time,
                'Cur': cur,
                'Imp': imp,
                'Event': event,
                'Actual': actual,
                'Forecast': forecast,
                'Previous': previous
            })
    return results


def save_to_csv(data, filename='calendar_data.csv'):
    """Save a list of dictionaries to a CSV file."""
    if not data:
        print("No data to save.")
        return

    fieldnames = data[0].keys()
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Data saved to {filename}")

all_data = []
start = 0

while True:
    rows = get_calendar_rows(start)
    # Filter for event rows only
    event_items = [row for row in rows if row.get('class') and "js-event-item" in row.get('class')]
    print(f"Start: {start} -> Found {len(event_items)} event rows.")
    # Continue looping until 200 event items are received
    if len(event_items) != 200:
        all_data.extend(process_calendar_data(rows))
        save_to_csv(all_data)
        break
    else:
        all_data.extend(process_calendar_data(rows))
        print(all_data[0])
        print("Event rows count is 200, scraping with next page...")
        start += 1  # Increment the start parameter
