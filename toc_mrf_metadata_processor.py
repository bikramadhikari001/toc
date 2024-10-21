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

def process_toc_mrf_metadata(json_data):
    mrf_metadata = []
    reporting_structures = json_data.get('reporting_structure', [])
    
    print(f"Number of reporting structures: {len(reporting_structures)}")
    
    for structure in reporting_structures:
        reporting_plans = structure.get('reporting_plans', [])
        in_network_files = structure.get('in_network_files', [])
        
        print(f"Number of reporting plans in this structure: {len(reporting_plans)}")
        print(f"Number of in-network files in this structure: {len(in_network_files)}")
        
        for plan in reporting_plans:
            for file in in_network_files:
                file_url = file.get('location', '')
                file_name = extract_filename_from_url(file_url)
                mrf_metadata.append({
                    'reporting_entity_name': json_data.get('reporting_entity_name', ''),
                    'reporting_entity_type': json_data.get('reporting_entity_type', ''),
                    'reporting_structure': 'group',  # Assuming 'group' based on the JSON structure
                    'in_network_file_name': file_name,
                    'in_network_file_location': file_url,
                    'in_network_file_description': file.get('description', ''),
                    'allowed_amount_file_name': '',
                    'allowed_amount_file_location': '',
                    'allowed_amount_file_description': '',
                    'plan_name': plan.get('plan_name', ''),
                    'plan_id_type': plan.get('plan_id_type', ''),
                    'plan_id': plan.get('plan_id', ''),
                    'plan_market_type': plan.get('plan_market_type', ''),
                    'toc_source_file_name': os.path.basename(json_data.get('reporting_entity_name', '')),
                    'parsed_date': datetime.now().strftime('%Y-%m-%d'),
                    'carrier': 'uhc',
                    'batch': '2024-10'
                })
    
    print(f"Processed toc_mrf_metadata: {len(mrf_metadata)} records")
    return mrf_metadata

def write_toc_mrf_metadata_csv(data, filename):
    fieldnames = ['reporting_entity_name', 'reporting_entity_type', 'reporting_structure', 'in_network_file_name', 'in_network_file_location', 'in_network_file_description', 'allowed_amount_file_name', 'allowed_amount_file_location', 'allowed_amount_file_description', 'plan_name', 'plan_id_type', 'plan_id', 'plan_market_type', 'toc_source_file_name', 'parsed_date', 'carrier', 'batch']
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    print(f"Created {filename} with {len(data)} rows")

def process_and_write_toc_mrf_metadata(json_data, output_file):
    mrf_metadata = process_toc_mrf_metadata(json_data)
    write_toc_mrf_metadata_csv(mrf_metadata, output_file)
