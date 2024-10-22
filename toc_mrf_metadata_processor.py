import os
import csv
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from typing import Dict, List
import logging
from contextlib import contextmanager
import mmap

logging.basicConfig(level=logging.ERROR)
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
        self.batch = []
        self.batch_size = 100000
        self.total_rows_written = 0
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
        for item in items:
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
                    self.batch.append(row)

                    if len(self.batch) >= self.batch_size:
                        self._write_batch()

        if self.batch:
            self._write_batch()

    def finalize(self):
        if self.batch:
            self._write_batch()
        if self.mmap_file:
            self.mmap_file.flush()
            self.mmap_file.close()
        logger.info(f"Processed and wrote {self.total_rows_written} rows of toc_mrf_metadata to {self.output_file}")

def process_and_write_toc_mrf_metadata(output_file: str, carrier: str):
    return TocMrfMetadataProcessor(output_file, carrier)
