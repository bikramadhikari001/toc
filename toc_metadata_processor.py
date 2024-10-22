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
        self.csv_file = open(self.output_file, 'w', newline='')
        self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=self.fieldnames)
        self.csv_writer.writeheader()
        self.reporting_structure_index = 0
        self.current_structure = {}

    def process(self, item):
        self.reporting_structure_index += 1
        self.current_structure['reporting_entity_name'] = item.get('reporting_entity_name', '')
        
        for file_info in item.get('in_network_files', []):
            file_url = file_info.get('location', '')
            file_name = extract_filename_from_url(file_url)
            
            metadata_entry = {
                'carrier': self.carrier,
                'dh_re_id': '',
                're_name': self.current_structure['reporting_entity_name'],
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
            self.csv_file.flush()  # Ensure the data is written immediately
            logger.debug(f"Added metadata entry: {metadata_entry}")

    def finalize(self):
        self.csv_file.close()
        logger.info(f"Completed processing toc_metadata: {self.reporting_structure_index} structures processed")

def process_and_write_toc_metadata(output_file, carrier, streaming=False):
    return TocMetadataProcessor(output_file, carrier)
