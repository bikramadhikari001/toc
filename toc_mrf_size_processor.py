import csv
import requests
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def get_file_size(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        if 'Content-Length' in response.headers:
            return int(response.headers['Content-Length']), ''
        else:
            return None, 'Content-Length not available in headers'
    except requests.RequestException as e:
        return None, f"Error: {str(e)}"

def extract_filename_from_url(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    if 'fn' in query_params:
        return query_params['fn'][0]
    else:
        return "Unknown"

class TocMrfSizeProcessor:
    def __init__(self, output_file: str, carrier: str):
        self.output_file = output_file
        self.carrier = carrier
        self.fieldnames = ['in_network_file_name', 'in_network_file_size', 'remarks', 'carrier', 'batch']
        self.csv_file = open(self.output_file, 'w', newline='')
        self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=self.fieldnames)
        self.csv_writer.writeheader()
        self.processed_rows = 0
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.futures = []

    def process(self, item: Dict):
        if 'in_network_files' in item:
            for file_info in item['in_network_files']:
                file_url = file_info.get('location', '')
                file_name = extract_filename_from_url(file_url)
                future = self.executor.submit(self._process_file, file_name, file_url)
                self.futures.append(future)

    def _process_file(self, file_name: str, file_url: str):
        file_size, remarks = get_file_size(file_url)
        row = {
            'in_network_file_name': file_name,
            'in_network_file_size': str(file_size) if file_size is not None else '',
            'remarks': remarks,
            'carrier': self.carrier,
            'batch': '2024-10'
        }
        self.csv_writer.writerow(row)
        self.csv_file.flush()  # Ensure the data is written immediately
        self.processed_rows += 1
        logger.debug(f"Added MRF size entry: {row}")

    def finalize(self):
        for future in as_completed(self.futures):
            pass  # Wait for all futures to complete
        self.executor.shutdown()
        self.csv_file.close()
        logger.info(f"Processed and wrote {self.processed_rows} rows of toc_mrf_size_data to {self.output_file}")

def process_and_write_toc_mrf_size_data(output_file: str, carrier: str, streaming: bool = False):
    return TocMrfSizeProcessor(output_file, carrier)
