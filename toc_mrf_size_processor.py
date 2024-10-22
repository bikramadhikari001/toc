import csv
import requests
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os

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
        self.csv_file = None
        self.csv_writer = None
        self.processed_rows = 0
        self.executor = None
        self.futures = []
        self.flush_threshold = 100  # Flush to disk every 100 rows

    def __enter__(self):
        self._open_csv()
        self.executor = ThreadPoolExecutor(max_workers=10)
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
        if 'in_network_files' in item:
            for file_info in item['in_network_files']:
                file_url = file_info.get('location', '')
                file_name = extract_filename_from_url(file_url)
                future = self.executor.submit(self._process_file, file_name, file_url)
                self.futures.append(future)
                
                # Process completed futures
                self._process_completed_futures(all_completed=False)

    def _process_file(self, file_name: str, file_url: str):
        file_size, remarks = get_file_size(file_url)
        return {
            'in_network_file_name': file_name,
            'in_network_file_size': str(file_size) if file_size is not None else '',
            'remarks': remarks,
            'carrier': self.carrier,
            'batch': '2024-10'
        }

    def _process_completed_futures(self, all_completed=False):
        completed_futures = [future for future in as_completed(self.futures)] if all_completed else []
        for future in completed_futures:
            self.futures.remove(future)
            row = future.result()
            self.csv_writer.writerow(row)
            self.processed_rows += 1
            logger.debug(f"Added MRF size entry: {row}")

            if self.processed_rows >= self.flush_threshold:
                self.csv_file.flush()
                self.processed_rows = 0

    def finalize(self):
        self._process_completed_futures(all_completed=True)
        if self.executor:
            self.executor.shutdown()
            self.executor = None
        if self.csv_file:
            self.csv_file.flush()
            self.csv_file.close()
            self.csv_file = None
            self.csv_writer = None
        logger.info(f"Processed and wrote {self.processed_rows} rows of toc_mrf_size_data to {self.output_file}")

def process_and_write_toc_mrf_size_data(output_file: str, carrier: str, streaming: bool = False):
    return TocMrfSizeProcessor(output_file, carrier)
