import json
import traceback
from datetime import datetime
from scrapy.exceptions import CloseSpider


def convert_date_numeric_is_day(self, month_day_of_data, year_of_data, var_name=None):
    try:
        # Remove any periods from the abbreviated month, e.g. "Dec. 27" -> "Dec 27"
        month_day_clean = month_day_of_data.replace('.', '')

        # Combine into a date string like "2025 Dec 27"
        date_str = f"{year_of_data} {month_day_clean}"

        # Parse the string into a datetime object. The format %Y for the year, %b for the abbreviated month, and %d for the day.
        constructed_date = datetime.strptime(date_str, "%Y %b %d")
        formatted_date = constructed_date.date().strftime("%Y-%m-%d")  # Format for JSON
        return formatted_date

    except Exception:
        error_message = traceback.format_exc()
        if var_name:
            raise CloseSpider(f"An error occurred while assigning to variable '{var_name}':")
        else:
            print("An error occurred:")
        print(error_message)
        return None

def convert_date_numeric_is_year(month_day_of_data, var_name=None):
    try:
        # Remove any periods from the abbreviated month, e.g. "Dec. 27" -> "Dec 27"
        month_day_clean = month_day_of_data.replace('.', ' ').replace(' (P)', '')

        # Combine into a date string like "2025 Dec 27"
        date_str = f"{month_day_clean}"

        # Parse the string into a datetime object. The format %Y for the year, %b for the abbreviated month, and %d for the day.
        constructed_date = datetime.strptime(date_str, "%b %y")
        formatted_date = constructed_date.date().strftime("%Y-%m-%d")  # Format for JSON
        return formatted_date

    except Exception:
        error_message = traceback.format_exc()
        if var_name:
            raise CloseSpider(f"An error occurred while assigning to variable '{var_name}':")
        else:
            print("An error occurred:")
        print(error_message)
        return None


def convert_quarter_date(q_str):
    try:
        if q_str.startswith("Q") and ":" in q_str:
            parts = q_str.split(":")
            quarter_num = parts[0][1]  # e.g. '4' from 'Q4'
            year_part = parts[1].split("-")[0]  # e.g. '2020' from '2020-21'
            if (quarter_num == '4'):
                year_part = int(year_part) + 1
            mapping = {
                "1": "06-01",
                "2": "09-01",
                "3": "12-01",
                "4": "03-01"
            }
            md = mapping.get(quarter_num, "01-01")
            return f"20{year_part}-{md}"
    except:
        pass
    # Return original if something doesn't match
    return q_str

def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def check_duplicate_date(extracted_date):
    try:
        with open(self.jsonl_file_path, 'r', encoding='utf-8') as file:
            for line in file:
                data_point = json.loads(line.strip())
                if data_point.get('date') == extracted_date:
                    raise CloseSpider(f'Data with date {extracted_date} is already present. Closing spider.')
    except FileNotFoundError:
        self.logger.warning(f'File not found: {self.jsonl_file_path}. Proceeding as new file.')
    except json.JSONDecodeError as e:
        self.logger.error(f'Error reading JSONL file: {e}')
