import os
import csv
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import ijson
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def extract_filename_from_url(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    if 'fn' in query_params:
        return query_params['fn'][0]
    else:
        return "Unknown"

def process_toc_metadata(json_parser, source_file):
    metadata = []
    reporting_structure_index = 0
    current_structure = {}
    
    logger.debug(f"Starting to process JSON from {source_file}")
    
    for prefix, event, value in json_parser:
        logger.debug(f"Parsing: prefix={prefix}, event={event}, value={value}")
        
        if prefix == 'reporting_entity_name':
            current_structure['reporting_entity_name'] = value
        elif prefix == 'reporting_structure.item':
            if event == 'start_map':
                reporting_structure_index += 1
                current_structure = {'reporting_structure_index': reporting_structure_index, 'reporting_entity_name': current_structure.get('reporting_entity_name', '')}
            elif event == 'end_map':
                logger.debug(f"Completed reporting structure: {current_structure}")
        elif prefix.endswith('.in_network_files.item'):
            if event == 'start_map':
                current_file = {}
            elif event == 'end_map':
                file_url = current_file.get('location', '')
                file_name = extract_filename_from_url(file_url)
                
                metadata.append({
                    'carrier': 'uhc',
                    'dh_re_id': '',
                    're_name': current_structure.get('reporting_entity_name', ''),
                    'toc_source_url': source_file,
                    'batch': '2024-10',
                    'toc_file_name': file_name,
                    'toc_file_url': file_url,
                    'toc_or_mrf_file': 'TOC' if 'table_of_contents' in file_name.lower() else 'MRF',
                    'mrf_file_plan_name': '',
                    'reporting_structure_index': current_structure.get('reporting_structure_index', ''),
                    'remarks': ''
                })
                logger.debug(f"Added metadata entry: {metadata[-1]}")
        elif prefix.endswith('.in_network_files.item.location'):
            current_file['location'] = value
    
    logger.info(f"Processed toc_metadata: {len(metadata)} records")
    return metadata

def write_toc_metadata_csv(data, filename):
    fieldnames = ['carrier', 'dh_re_id', 're_name', 'toc_source_url', 'batch', 'toc_file_name', 'toc_file_url', 'toc_or_mrf_file', 'mrf_file_plan_name', 'reporting_structure_index', 'remarks']
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    logger.info(f"Created {filename} with {len(data)} rows")

def process_and_write_toc_metadata(json_file, output_file):
    logger.info(f"Processing file: {json_file}")
    with open(json_file, 'rb') as file:
        parser = ijson.parse(file)
        metadata = process_toc_metadata(parser, json_file)
    write_toc_metadata_csv(metadata, output_file)
    logger.info(f"Completed processing {json_file}")
