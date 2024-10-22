import csv
import requests
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
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
        return os.path.basename(parsed_url.path) or "Unknown"

class TocMrfSizeProcessor:
    def __init__(self, output_file: str, carrier: str):
        self.output_file = output_file
        self.carrier = carrier
        self.fieldnames = ['in_network_file_name', 'in_network_file_size', 'remarks', 'carrier', 'batch']
        self.batch: List[Dict] = []
        self.batch_size = 100
        self.total_rows_written = 0
        self.executor = ThreadPoolExecutor(max_workers=10)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finalize()

    @contextmanager
    def _open_csv(self):
        try:
            file = open(self.output_file, 'a', newline='')
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            if os.path.getsize(self.output_file) == 0:
                writer.writeheader()
            yield writer
        finally:
            file.close()

    def _write_batch(self):
        with self._open_csv() as writer:
            writer.writerows(self.batch)
        self.total_rows_written += len(self.batch)
        self.batch.clear()
        logger.info(f"Wrote batch. Total rows written: {self.total_rows_written}")

    def process(self, item: Dict):
        try:
            if 'in_network_files' in item:
                futures = []
                for file_info in item['in_network_files']:
                    file_url = file_info.get('location', '')
                    file_name = extract_filename_from_url(file_url)
                    future = self.executor.submit(self._process_file, file_name, file_url)
                    futures.append(future)

                for future in as_completed(futures):
                    row = future.result()
                    self.batch.append(row)

                    if len(self.batch) >= self.batch_size:
                        self._write_batch()

        except Exception as e:
            logger.error(f"Error processing item: {str(e)}")

    def _process_file(self, file_name: str, file_url: str):
        file_size, remarks = get_file_size(file_url)
        return {
            'in_network_file_name': file_name,
            'in_network_file_size': str(file_size) if file_size is not None else '',
            'remarks': remarks,
            'carrier': self.carrier,
            'batch': '2024-10'
        }

    def finalize(self):
        if self.batch:
            self._write_batch()
        self.executor.shutdown()
        logger.info(f"Processed and wrote {self.total_rows_written} rows of toc_mrf_size_data to {self.output_file}")

def process_and_write_toc_mrf_size_data(output_file: str, carrier: str, streaming: bool = False):
    return TocMrfSizeProcessor(output_file, carrier)
