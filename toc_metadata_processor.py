import os
import csv
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import logging
from contextlib import contextmanager
import mmap
import numpy as np

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def extract_filename_from_url(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    if 'fn' in query_params:
        return query_params['fn'][0]
    else:
        return os.path.basename(parsed_url.path) or "Unknown"

class TocMetadataProcessor:
    def __init__(self, output_file, carrier):
        self.output_file = output_file
        self.carrier = carrier
        self.fieldnames = ['carrier', 'dh_re_id', 're_name', 'toc_source_url', 'batch', 'toc_file_name', 'toc_file_url', 'toc_or_mrf_file', 'mrf_file_plan_name', 'reporting_structure_index', 'remarks']
        self.reporting_structure_index = 0
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

    def process_batch(self, items):
        for item in items:
            self.reporting_structure_index += 1
            re_name = item.get('reporting_entity_name', '')
            
            for file_info in item.get('in_network_files', []):
                file_url = file_info.get('location', '')
                file_name = extract_filename_from_url(file_url)
                
                metadata_entry = {
                    'carrier': self.carrier,
                    'dh_re_id': '',
                    're_name': re_name,
                    'toc_source_url': 'anthem_index.json',
                    'batch': '2024-10',
                    'toc_file_name': file_name,
                    'toc_file_url': file_url,
                    'toc_or_mrf_file': 'TOC' if 'table_of_contents' in file_name.lower() else 'MRF',
                    'mrf_file_plan_name': '',
                    'reporting_structure_index': self.reporting_structure_index,
                    'remarks': ''
                }
                self.batch.append(metadata_entry)

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
        logger.info(f"Completed processing toc_metadata: {self.reporting_structure_index} structures processed, {self.total_rows_written} total rows written")

def process_and_write_toc_metadata(output_file, carrier):
    return TocMetadataProcessor(output_file, carrier)
