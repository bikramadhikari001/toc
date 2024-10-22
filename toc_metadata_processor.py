import os
import csv
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import logging
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
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
        self.batch_size = 1000
        self.total_rows_written = 0

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

    def process(self, item):
        try:
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

        except Exception as e:
            logger.error(f"Error processing item: {str(e)}")

    def finalize(self):
        if self.batch:
            self._write_batch()
        logger.info(f"Completed processing toc_metadata: {self.reporting_structure_index} structures processed, {self.total_rows_written} total rows written")

def process_and_write_toc_metadata(output_file, carrier, streaming=False):
    return TocMetadataProcessor(output_file, carrier)
