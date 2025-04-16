# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os
import re # Import regex for filename sanitization
from datetime import datetime

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter # To handle items easily in pipeline
import json

# --- Define the Item Pipeline for Product + Process Filenames ---
class SpotProductProcessJsonLinesPipeline: # Renamed to reflect new logic

    def __init__(self):
        # Dictionary to hold items buffered during the run, keyed by product ID
        self.items_buffer = {}
        # Dictionary to hold file handles, keyed by product ID
        self.file_handles = {}
        # Base output directory remains the same
        self.base_path = r"D:\Desktop\financial_data_pipeline\data\raw\Oil data\oil_data_Energy_Information_Administration"

    def _sanitize_filename(self, name):
        """Removes or replaces characters invalid for filenames."""
        # Use a default if the combined name ends up empty
        if not name:
            name = "unknown_product_process" # Default for combined name
        # Remove characters often problematic in filenames
        name = re.sub(r'[\\/*?:"<>|]', "", name)
        # Replace sequences of whitespace or underscores with a single underscore
        name = re.sub(r'\s+', '_', name)
        name = re.sub(r'_+', '_', name)
        # Ensure it's not empty after sanitization
        if not name:
            name = "unknown_product_process"
        # Add the .jsonl extension
        return name + ".jsonl"

    def open_spider(self, spider):
        # Only ensure the base directory exists. Files are opened as needed.
        os.makedirs(self.base_path, exist_ok=True)
        spider.logger.info(f"Pipeline initialized. Output directory: {self.base_path}")
        # Clear buffers/handles from previous runs if spider instance is reused (safer)
        self.items_buffer = {}
        self.file_handles = {}


    def close_spider(self, spider):
        # Process buffered items at the end of the crawl
        spider.logger.info(f"Processing buffered items for {len(self.items_buffer)} product/process combinations.")
        for product_id, items in self.items_buffer.items(): # Still keying internally by product_id
            if not items:
                # This check might be redundant if buffer is only created when item arrives, but safe to keep
                spider.logger.info(f"No items buffered for product ID {product_id}.")
                continue

            # Get product and process name from the first item for logging purposes
            # Use product_id as fallback if keys are missing
            first_item = items[0]
            product_name_log = first_item.get('product-name', 'UnknownProduct')
            process_name_log = first_item.get('process-name', 'UnknownProcess')
            log_name = f"{product_name_log}_{process_name_log}" # Combined name for logging

            spider.logger.info(f"Sorting and appending {len(items)} items for: '{log_name}' (Product ID: {product_id})...")

            # Sort the buffered items by the 'period' field (date)
            try:
                items.sort(key=lambda x: datetime.strptime(x.get('period', ''), '%Y-%m-%d'))
            except ValueError as e:
                spider.logger.error(f"Error parsing date for sorting '{log_name}': {e}. Appending unsorted.")
            except Exception as e:
                 spider.logger.error(f"Unexpected error during sorting '{log_name}': {e}. Appending unsorted.")

            # Write sorted items to the appropriate file
            file_handle = self.file_handles.get(product_id)
            # Check handle exists and is not closed
            if file_handle and not file_handle.closed:
                try:
                    for item in items:
                        # Convert item dictionary to a JSON string and add newline
                        line = json.dumps(item, ensure_ascii=False) + "\n"
                        file_handle.write(line)
                except IOError as e:
                    spider.logger.error(f"IOError writing to file for '{log_name}' ({product_id}): {e}")
            else:
                 spider.logger.error(f"File handle not found or already closed for '{log_name}' ({product_id}) during write.")

        # Close all file handles managed by this pipeline
        spider.logger.info("Closing all product/process file handles.")
        for product_id, file_handle in self.file_handles.items():
             # Check handle exists and is not closed before attempting to close
             if file_handle and not file_handle.closed:
                 try:
                     file_handle.close()
                     # Logging closing using product_id, as retrieving combined name here adds complexity
                     spider.logger.info(f"Closed file for product ID {product_id}.")
                 except IOError as e:
                     spider.logger.error(f"IOError closing file for product ID {product_id}: {e}")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        # Use product ID as the primary key internally for buffers/handles
        product_id = adapter.get('product')
        # Get product and process names for filename generation, providing defaults
        product_name = adapter.get('product-name', 'UnknownProduct')
        process_name = adapter.get('process-name', 'UnknownProcess')

        # Ensure we have a product ID to work with
        if not product_id:
            spider.logger.warning(f"Item missing 'product' ID: {item}. Ignoring.")
            return item # Pass along items without product ID if needed, or drop

        # Check if we have already opened a file for this product ID in this run
        if product_id not in self.file_handles:
            # First time seeing this product ID: create filename, open file, init buffer
            try:
                # --- Create combined name for filename ---
                combined_name_for_file = f"{product_name}_{process_name}"
                # ---

                filename = self._sanitize_filename(combined_name_for_file) # Sanitize the combined name
                file_path = os.path.join(self.base_path, filename)

                # Open file in append mode ('a')
                self.file_handles[product_id] = open(file_path, 'a', encoding='utf-8')
                self.items_buffer[product_id] = [] # Initialize buffer list
                # Log using the combined name for clarity on which file was opened
                spider.logger.info(f"Opened file for appending (Product/Process: '{combined_name_for_file}' [{product_id}]): {file_path}")
            except IOError as e:
                 spider.logger.error(f"Failed to open file for '{combined_name_for_file}' ({product_id}) at {file_path}: {e}")
                 return None # Drop item if file can't be opened
            except Exception as e:
                 spider.logger.error(f"Unexpected error setting up file/buffer for '{combined_name_for_file}' ({product_id}): {e}")
                 return None # Drop item on unexpected setup error

        # Append item to the buffer for the corresponding product ID (if setup was successful)
        if product_id in self.items_buffer:
             # Store the item as a dictionary
            self.items_buffer[product_id].append(adapter.asdict())
        # else: # This case implies file handle setup failed above and item was dropped/ignored
             # spider.logger.warning(f"Buffer not ready for product {product_id}, cannot add item.")

        # Return the item so other pipelines (if any) can process it
        return item

# --- Define the Item Pipeline for Product + Process Filenames ---
class FuturesProductProcessJsonLinesPipeline: # Renamed to reflect new logic

    def __init__(self):
        # Dictionary to hold items buffered during the run, keyed by product ID
        self.items_buffer = {}
        # Dictionary to hold file handles, keyed by product ID
        self.file_handles = {}
        # Base output directory remains the same
        self.base_path = r"D:\Desktop\financial_data_pipeline\data\raw\Oil data\oil_data_Energy_Information_Administration"

    def _sanitize_filename(self, name):
        """Removes or replaces characters invalid for filenames."""
        # Use a default if the combined name ends up empty
        if not name:
            name = "unknown_product_process" # Default for combined name
        # Remove characters often problematic in filenames
        name = re.sub(r'[\\/*?:"<>|]', "", name)
        # Replace sequences of whitespace or underscores with a single underscore
        name = re.sub(r'\s+', '_', name)
        name = re.sub(r'_+', '_', name)
        # Ensure it's not empty after sanitization
        if not name:
            name = "unknown_product_process"
        # Add the .jsonl extension
        return name + ".jsonl"

    def open_spider(self, spider):
        # Only ensure the base directory exists. Files are opened as needed.
        os.makedirs(self.base_path, exist_ok=True)
        spider.logger.info(f"Pipeline initialized. Output directory: {self.base_path}")
        # Clear buffers/handles from previous runs if spider instance is reused (safer)
        self.items_buffer = {}
        self.file_handles = {}


    def close_spider(self, spider):
        # Process buffered items at the end of the crawl
        spider.logger.info(f"Processing buffered items for {len(self.items_buffer)} product/process combinations.")
        for product_id, items in self.items_buffer.items(): # Still keying internally by product_id
            if not items:
                # This check might be redundant if buffer is only created when item arrives, but safe to keep
                spider.logger.info(f"No items buffered for product ID {product_id}.")
                continue

            # Get product and process name from the first item for logging purposes
            # Use product_id as fallback if keys are missing
            first_item = items[0]
            product_name_log = first_item.get('product-name', 'UnknownProduct')
            process_name_log = first_item.get('process-name', 'UnknownProcess')
            log_name = f"{product_name_log}_{process_name_log}" # Combined name for logging

            spider.logger.info(f"Sorting and appending {len(items)} items for: '{log_name}' (Product ID: {product_id})...")

            # Sort the buffered items by the 'period' field (date)
            try:
                items.sort(key=lambda x: datetime.strptime(x.get('period', ''), '%Y-%m-%d'))
            except ValueError as e:
                spider.logger.error(f"Error parsing date for sorting '{log_name}': {e}. Appending unsorted.")
            except Exception as e:
                 spider.logger.error(f"Unexpected error during sorting '{log_name}': {e}. Appending unsorted.")

            # Write sorted items to the appropriate file
            file_handle = self.file_handles.get(product_id)
            # Check handle exists and is not closed
            if file_handle and not file_handle.closed:
                try:
                    for item in items:
                        # Convert item dictionary to a JSON string and add newline
                        line = json.dumps(item, ensure_ascii=False) + "\n"
                        file_handle.write(line)
                except IOError as e:
                    spider.logger.error(f"IOError writing to file for '{log_name}' ({product_id}): {e}")
            else:
                 spider.logger.error(f"File handle not found or already closed for '{log_name}' ({product_id}) during write.")

        # Close all file handles managed by this pipeline
        spider.logger.info("Closing all product/process file handles.")
        for product_id, file_handle in self.file_handles.items():
             # Check handle exists and is not closed before attempting to close
             if file_handle and not file_handle.closed:
                 try:
                     file_handle.close()
                     # Logging closing using product_id, as retrieving combined name here adds complexity
                     spider.logger.info(f"Closed file for product ID {product_id}.")
                 except IOError as e:
                     spider.logger.error(f"IOError closing file for product ID {product_id}: {e}")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        # Use product ID as the primary key internally for buffers/handles
        product_id = adapter.get('process')
        # Get product and process names for filename generation, providing defaults
        product_name = adapter.get('product-name', 'UnknownProduct')
        process_name = adapter.get('process-name', 'UnknownProcess')

        # Ensure we have a product ID to work with
        if not product_id:
            spider.logger.warning(f"Item missing 'process' ID: {item}. Ignoring.")
            return item # Pass along items without product ID if needed, or drop

        # Check if we have already opened a file for this product ID in this run
        if product_id not in self.file_handles:
            # First time seeing this product ID: create filename, open file, init buffer
            try:
                # --- Create combined name for filename ---
                combined_name_for_file = f"{product_name}_{process_name}"
                # ---

                filename = self._sanitize_filename(combined_name_for_file) # Sanitize the combined name
                file_path = os.path.join(self.base_path, filename)

                # Open file in append mode ('a')
                self.file_handles[product_id] = open(file_path, 'a', encoding='utf-8')
                self.items_buffer[product_id] = [] # Initialize buffer list
                # Log using the combined name for clarity on which file was opened
                spider.logger.info(f"Opened file for appending (Product/Process: '{combined_name_for_file}' [{product_id}]): {file_path}")
            except IOError as e:
                 spider.logger.error(f"Failed to open file for '{combined_name_for_file}' ({product_id}) at {file_path}: {e}")
                 return None # Drop item if file can't be opened
            except Exception as e:
                 spider.logger.error(f"Unexpected error setting up file/buffer for '{combined_name_for_file}' ({product_id}): {e}")
                 return None # Drop item on unexpected setup error

        # Append item to the buffer for the corresponding product ID (if setup was successful)
        if product_id in self.items_buffer:
             # Store the item as a dictionary
            self.items_buffer[product_id].append(adapter.asdict())
        # else: # This case implies file handle setup failed above and item was dropped/ignored
             # spider.logger.warning(f"Buffer not ready for product {product_id}, cannot add item.")

        # Return the item so other pipelines (if any) can process it
        return item