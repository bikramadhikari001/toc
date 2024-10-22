import os
import csv
from datetime import datetime
from urllib.parse import urlparse, parse_qs
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

class TocMetadataProcessor:
    def __init__(self, output_file, carrier):
        self.output_file = output_file
        self.carrier = carrier
        self.fieldnames = ['carrier', 'dh_re_id', 're_name', 'toc_source_url', 'batch', 'toc_file_name', 'toc_file_url', 'toc_or_mrf_file', 'mrf_file_plan_name', 'reporting_structure_index', 'remarks']
        self.reporting_structure_index = 0
        self.csv_file = None
        self.csv_writer = None
        self.rows_written = 0
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

    def process(self, item):
        self._open_csv()
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
            self.csv_writer.writerow(metadata_entry)
            self.rows_written += 1

            if self.rows_written >= self.flush_threshold:
                self.csv_file.flush()
                self.rows_written = 0

            logger.debug(f"Added metadata entry: {metadata_entry}")

    def finalize(self):
        if self.csv_file:
            self.csv_file.flush()
            self.csv_file.close()
            self.csv_file = None
            self.csv_writer = None
        logger.info(f"Completed processing toc_metadata: {self.reporting_structure_index} structures processed")

def process_and_write_toc_metadata(output_file, carrier, streaming=False):
    return TocMetadataProcessor(output_file, carrier)
