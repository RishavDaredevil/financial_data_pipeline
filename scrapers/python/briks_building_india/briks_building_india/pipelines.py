# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from datetime import datetime

from typing import Any
from scrapy.http import Request, Response
from scrapy.pipelines.files import FilesPipeline
import shutil
import os

from scrapy.pipelines.media import MediaPipeline


class BriksBuildingIndiaPipeline(FilesPipeline):
    def file_path(self,
        request: Request,
        response: Response | None = None,
        info: MediaPipeline.SpiderInfo | None = None,
        *, item: Any = None) -> str:

        filename = "inflation_expectations_survey.xlsx"
        return f"full\\{filename}"


class Backup_Pipeline_briks_building_india(object):
    def process_item(self, item, spider):
        # Define paths
        source_dir = "D:\\Desktop\\financial_data_pipeline\\scrapers\\python\\briks_building_india\\full"
        file_to_copy = "inflation_expectations_survey.xlsx"
        dest_dir1 = "D:\\Desktop\\financial_data_pipeline\\data\\raw\\briks_building_india"
        dest_dir2 = "D:\\Desktop\\financial_data_pipeline\\backups\\yearly"

        # Ensure destination directories exist
        os.makedirs(dest_dir1, exist_ok=True)
        os.makedirs(dest_dir2, exist_ok=True)

        # Copy file to destination1 without renaming
        shutil.copy(os.path.join(source_dir, file_to_copy), dest_dir1)

        # Copy file to destination2 with renaming
        new_file_name = f"inflation_expectations_survey_{(datetime.now()).strftime('%Y-%b-%d')}.xlsx"
        shutil.copy(os.path.join(source_dir, file_to_copy), os.path.join(dest_dir2, new_file_name))

        # Remove the parent directory
        shutil.rmtree(source_dir)

        print("File copied to both destinations and parent directory deleted.")


