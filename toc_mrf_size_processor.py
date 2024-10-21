import csv
import requests
import ijson
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from typing import Iterator, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_file_size(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
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

def process_toc_mrf_size_data(json_file: str) -> Iterator[Dict]:
    with open(json_file, 'rb') as file:
        parser = ijson.parse(file)
        
        for prefix, event, value in parser:
            if prefix.endswith('.in_network_files.item') and event == 'end':
                file_url = value.get('location', '')
                file_name = extract_filename_from_url(file_url)
                yield {
                    'in_network_file_name': file_name,
                    'in_network_file_url': file_url,
                    'carrier': 'uhc',
                    'batch': '2024-10'
                }

def get_file_sizes(data_iterator: Iterator[Dict]) -> Iterator[Dict]:
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(get_file_size, item['in_network_file_url']): item for item in data_iterator}
        for future in as_completed(future_to_url):
            item = future_to_url[future]
            file_size, remarks = future.result()
            item['in_network_file_size'] = str(file_size) if file_size is not None else ''
            item['remarks'] = remarks
            yield item

def write_toc_mrf_size_csv(data_iterator: Iterator[Dict], filename: str, chunk_size: int = 100):
    fieldnames = ['in_network_file_name', 'in_network_file_size', 'remarks', 'carrier', 'batch']
    
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

def process_and_write_toc_mrf_size_data(json_file: str, output_file: str):
    mrf_size_data_iterator = process_toc_mrf_size_data(json_file)
    mrf_size_data_with_sizes = get_file_sizes(mrf_size_data_iterator)
    write_toc_mrf_size_csv(mrf_size_data_with_sizes, output_file)
    print(f"Processed and wrote toc_mrf_size_data to {output_file}")
