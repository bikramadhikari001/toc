import os
import csv
import ijson
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from typing import Iterator, Dict

def extract_filename_from_url(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    if 'fn' in query_params:
        return query_params['fn'][0]
    else:
        return "Unknown"

def process_toc_mrf_metadata(json_file: str) -> Iterator[Dict]:
    with open(json_file, 'rb') as file:
        parser = ijson.parse(file)
        
        reporting_entity_name = ''
        reporting_entity_type = ''
        current_structure = {}
        current_plan = {}
        current_file = {}
        
        for prefix, event, value in parser:
            if prefix == 'reporting_entity_name':
                reporting_entity_name = value
            elif prefix == 'reporting_entity_type':
                reporting_entity_type = value
            elif prefix == 'reporting_structure.item':
                if event == 'start':
                    current_structure = {'reporting_plans': [], 'in_network_files': []}
                elif event == 'end':
                    for file in current_structure.get('in_network_files', []):
                        for plan in current_structure.get('reporting_plans', [{}]):
                            yield {
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
                                'toc_source_file_name': os.path.basename(json_file),
                                'parsed_date': datetime.now().strftime('%Y-%m-%d'),
                                'carrier': 'uhc',
                                'batch': '2024-10'
                            }
            elif prefix.endswith('.in_network_files.item'):
                if event == 'start':
                    current_file = {}
                elif event == 'end':
                    current_structure['in_network_files'].append(current_file)
            elif prefix.endswith('.in_network_files.item.location'):
                current_file['location'] = value
            elif prefix.endswith('.in_network_files.item.description'):
                current_file['description'] = value
            elif prefix.endswith('.reporting_plans.item'):
                if event == 'start':
                    current_plan = {}
                elif event == 'end':
                    current_structure['reporting_plans'].append(current_plan)
            elif prefix.endswith('.reporting_plans.item.plan_name'):
                current_plan['plan_name'] = value
            elif prefix.endswith('.reporting_plans.item.plan_id_type'):
                current_plan['plan_id_type'] = value
            elif prefix.endswith('.reporting_plans.item.plan_id'):
                current_plan['plan_id'] = value
            elif prefix.endswith('.reporting_plans.item.plan_market_type'):
                current_plan['plan_market_type'] = value

def write_toc_mrf_metadata_csv(data_iterator: Iterator[Dict], filename: str, chunk_size: int = 1000):
    fieldnames = ['reporting_entity_name', 'reporting_entity_type', 'reporting_structure', 'in_network_file_name', 'in_network_file_location', 'in_network_file_description', 'allowed_amount_file_name', 'allowed_amount_file_location', 'allowed_amount_file_description', 'plan_name', 'plan_id_type', 'plan_id', 'plan_market_type', 'toc_source_file_name', 'parsed_date', 'carrier', 'batch']
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        chunk = []
        for row in data_iterator:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                writer.writerows(chunk)
                chunk = []
        
        if chunk:
            writer.writerows(chunk)

def process_and_write_toc_mrf_metadata(json_file: str, output_file: str):
    mrf_metadata_iterator = process_toc_mrf_metadata(json_file)
    write_toc_mrf_metadata_csv(mrf_metadata_iterator, output_file)
    print(f"Processed and wrote toc_mrf_metadata to {output_file}")
