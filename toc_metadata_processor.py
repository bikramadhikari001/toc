import os
import csv
from datetime import datetime
from urllib.parse import urlparse, parse_qs

def extract_filename_from_url(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    if 'fn' in query_params:
        return query_params['fn'][0]
    else:
        return "Unknown"

def process_toc_metadata(json_data, source_file):
    metadata = []
    reporting_structures = json_data.get('reporting_structure', [])
    
    for structure in reporting_structures:
        in_network_files = structure.get('in_network_files', [])
        for file in in_network_files:
            file_url = file.get('location', '')
            file_name = extract_filename_from_url(file_url)
            metadata.append({
                'carrier': 'uhc',
                'dh_re_id': '',
                're_name': json_data.get('reporting_entity_name', ''),
                'toc_source_url': source_file,
                'batch': '2024-10',
                'toc_file_name': file_name,
                'toc_file_url': file_url,
                'toc_or_mrf_file': 'MRF',
                'mrf_file_plan_name': '',
                'remarks': ''
            })
    
    print(f"Processed toc_metadata: {len(metadata)} records")
    return metadata

def write_toc_metadata_csv(data, filename):
    fieldnames = ['carrier', 'dh_re_id', 're_name', 'toc_source_url', 'batch', 'toc_file_name', 'toc_file_url', 'toc_or_mrf_file', 'mrf_file_plan_name', 'remarks']
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    print(f"Created {filename} with {len(data)} rows")

def process_and_write_toc_metadata(json_data, source_file, output_file):
    metadata = process_toc_metadata(json_data, source_file)
    write_toc_metadata_csv(metadata, output_file)
