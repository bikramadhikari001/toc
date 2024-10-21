import csv
import requests
from datetime import datetime
from urllib.parse import urlparse, parse_qs

def get_file_size(url):
    try:
        response = requests.head(url, allow_redirects=True)
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
        return "Unknown"

def process_toc_mrf_size_data(json_data):
    mrf_size_data = []
    reporting_structures = json_data.get('reporting_structure', [])
    
    print(f"Number of reporting structures: {len(reporting_structures)}")
    
    for structure in reporting_structures:
        in_network_files = structure.get('in_network_files', [])
        print(f"Number of in-network files in this structure: {len(in_network_files)}")
        
        for file in in_network_files:
            file_url = file.get('location', '')
            file_size, remarks = get_file_size(file_url)
            file_name = extract_filename_from_url(file_url)
            mrf_size_data.append({
                'in_network_file_name': file_name,
                'in_network_file_size': str(file_size) if file_size is not None else '',
                'remarks': remarks,
                'carrier': 'uhc',
                'batch': '2024-10'
            })
    
    print(f"Processed toc_mrf_size_data: {len(mrf_size_data)} records")
    return mrf_size_data

def write_toc_mrf_size_csv(data, filename):
    fieldnames = ['in_network_file_name', 'in_network_file_size', 'remarks', 'carrier', 'batch']
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    print(f"Created {filename} with {len(data)} rows")

def process_and_write_toc_mrf_size(json_data, output_file):
    mrf_size_data = process_toc_mrf_size_data(json_data)
    write_toc_mrf_size_csv(mrf_size_data, output_file)
