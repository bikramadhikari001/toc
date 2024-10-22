import os
import csv
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from typing import Dict
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def extract_filename_from_url(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    if 'fn' in query_params:
        return query_params['fn'][0]
    else:
        return os.path.basename(parsed_url.path) or "Unknown"

class TocMrfMetadataProcessor:
    def __init__(self, output_file: str, carrier: str):
        self.output_file = output_file
        self.carrier = carrier
        self.fieldnames = ['reporting_entity_name', 'reporting_entity_type', 'reporting_structure', 'in_network_file_name', 'in_network_file_location', 'in_network_file_description', 'allowed_amount_file_name', 'allowed_amount_file_location', 'allowed_amount_file_description', 'plan_name', 'plan_id_type', 'plan_id', 'plan_market_type', 'toc_source_file_name', 'parsed_date', 'carrier', 'batch']
        self.csv_file = None
        self.csv_writer = None
        self.processed_rows = 0
        self.flush_threshold = 1000  # Flush to disk every 1000 rows

    def __enter__(self):
        self._open_csv()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finalize()

    def _open_csv(self):
        if self.csv_file is None:
            self.csv_file = open(self.output_file, 'a', newline='')
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=self.fieldnames)
            if os.path.getsize(self.output_file) == 0:
                self.csv_writer.writeheader()

    def process(self, item: Dict):
        self._open_csv()
        reporting_entity_name = item.get('reporting_entity_name', '')
        reporting_entity_type = item.get('reporting_entity_type', '')

        for file in item.get('in_network_files', []):
            for plan in item.get('reporting_plans', [{}]):
                row = {
                    'reporting_entity_name': reporting_entity_name,
                    'reporting_entity_type': reporting_entity_type,
                    'reporting_structure': 'group',
                    'in_network_file_name': extract_filename_from_url(file.get('location', '')),
                    'in_network_file_location': file.get('location', ''),
                    'in_network_file_description': file.get('description', ''),
                    'allowed_amount_file_name': '',
                    'allowed_amount_file_location': '',
                    'allowed_amount_file_description': '',
                    'plan_name': plan.get('plan_name', ''),
                    'plan_id_type': plan.get('plan_id_type', ''),
                    'plan_id': plan.get('plan_id', ''),
                    'plan_market_type': plan.get('plan_market_type', ''),
                    'toc_source_file_name': 'anthem_index.json',
                    'parsed_date': datetime.now().strftime('%Y-%m-%d'),
                    'carrier': self.carrier,
                    'batch': '2024-10'
                }
                self.csv_writer.writerow(row)
                self.processed_rows += 1
                logger.debug(f"Added MRF metadata entry: {row}")

                if self.processed_rows >= self.flush_threshold:
                    self.csv_file.flush()
                    self.processed_rows = 0

    def finalize(self):
        if self.csv_file:
            self.csv_file.flush()
            self.csv_file.close()
            self.csv_file = None
            self.csv_writer = None
        logger.info(f"Processed and wrote {self.processed_rows} rows of toc_mrf_metadata to {self.output_file}")

def process_and_write_toc_mrf_metadata(output_file: str, carrier: str, streaming: bool = False):
    return TocMrfMetadataProcessor(output_file, carrier)
