import csv
import requests
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
from contextlib import contextmanager
import mmap

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def get_file_size(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
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
        self.batch_size = 100000
        self.total_rows_written = 0
        self.executor = ThreadPoolExecutor(max_workers=50)
        self.mmap_file = None
        self.mmap_size = 1024 * 1024 * 1024  # 1GB initial size

    def __enter__(self):
        self._create_mmap_file()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finalize()

    def _create_mmap_file(self):
        with open(self.output_file, 'wb') as f:
            f.write(b'\0' * self.mmap_size)
        self.mmap_file = mmap.mmap(os.open(self.output_file, os.O_RDWR), self.mmap_size)
        self._write_header()

    def _write_header(self):
        header = ','.join(self.fieldnames) + '\n'
        self.mmap_file.write(header.encode())

    def _write_batch(self):
        batch_data = '\n'.join(','.join(str(row.get(field, '')) for field in self.fieldnames) for row in self.batch) + '\n'
        encoded_data = batch_data.encode()
        if self.mmap_file.tell() + len(encoded_data) > self.mmap_size:
            self._resize_mmap_file()
        self.mmap_file.write(encoded_data)
        self.total_rows_written += len(self.batch)
        self.batch.clear()

    def _resize_mmap_file(self):
        self.mmap_size *= 2
        self.mmap_file.close()
        with open(self.output_file, 'ab') as f:
            f.write(b'\0' * self.mmap_size)
        self.mmap_file = mmap.mmap(os.open(self.output_file, os.O_RDWR), self.mmap_size)
        self.mmap_file.seek(0, 2)  # Move to the end of the file

    def process_batch(self, items: List[Dict]):
        futures = []
        for item in items:
            if 'in_network_files' in item:
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

        if self.batch:
            self._write_batch()

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
        if self.mmap_file:
            self.mmap_file.flush()
            self.mmap_file.close()
        logger.info(f"Processed and wrote {self.total_rows_written} rows of toc_mrf_size_data to {self.output_file}")

def process_and_write_toc_mrf_size_data(output_file: str, carrier: str):
    return TocMrfSizeProcessor(output_file, carrier)
