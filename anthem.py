import os
import logging
import requests
import gzip
import shutil
import json
import argparse
import traceback
import psutil
import multiprocessing as mp
from toc_metadata_processor import process_and_write_toc_metadata
from toc_mrf_metadata_processor import process_and_write_toc_mrf_metadata
from toc_mrf_size_processor import process_and_write_toc_mrf_size_data
import config
import tracemalloc
import time
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
from urllib.parse import urlparse, parse_qs

logging.basicConfig(
    filename=config.LOG_FILE,
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

ANTHEM_URL = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-10-01_anthem_index.json.gz"
DOWNLOAD_DIR = "downloads"
ANTHEM_FILE_NAME = "anthem_index.json.gz"
UNZIPPED_FILE_NAME = "anthem_index.json"
CARRIER_NAME = "anthem"
BATCH_SIZE = 1000
MAX_WORKERS = min(32, mp.cpu_count() * 2)
BUFFER_SIZE = 1024 * 1024

def extract_filename_from_url(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    if 'fn' in query_params:
        return query_params['fn'][0]
    else:
        return os.path.basename(parsed_url.path) or "Unknown"

def process_json_object(obj, reporting_structure_index):
    """Process a single JSON object and return data for all three CSVs"""
    re_name = obj.get('reporting_entity_name', '')
    re_type = obj.get('reporting_entity_type', '')
    
    metadata_rows = []
    mrf_metadata_rows = []
    mrf_size_rows = []
    
    for file_info in obj.get('in_network_files', []):
        file_url = file_info.get('location', '')
        file_name = extract_filename_from_url(file_url)
        
        # TOC Metadata
        metadata_row = {
            'carrier': CARRIER_NAME,
            'dh_re_id': '',
            're_name': re_name,
            'toc_source_url': 'anthem_index.json',
            'batch': '2024-10',
            'toc_file_name': file_name,
            'toc_file_url': file_url,
            'toc_or_mrf_file': 'TOC' if 'table_of_contents' in file_name.lower() else 'MRF',
            'mrf_file_plan_name': '',
            'reporting_structure_index': reporting_structure_index,
            'remarks': ''
        }
        metadata_rows.append(metadata_row)
        
        # MRF Metadata
        for plan in obj.get('reporting_plans', [{}]):
            mrf_metadata_row = {
                'reporting_entity_name': re_name,
                'reporting_entity_type': re_type,
                'reporting_structure': 'group',
                'in_network_file_name': file_name,
                'in_network_file_location': file_url,
                'in_network_file_description': file_info.get('description', ''),
                'allowed_amount_file_name': '',
                'allowed_amount_file_location': '',
                'allowed_amount_file_description': '',
                'plan_name': plan.get('plan_name', ''),
                'plan_id_type': plan.get('plan_id_type', ''),
                'plan_id': plan.get('plan_id', ''),
                'plan_market_type': plan.get('plan_market_type', ''),
                'toc_source_file_name': 'anthem_index.json',
                'parsed_date': time.strftime('%Y-%m-%d'),
                'carrier': CARRIER_NAME,
                'batch': '2024-10'
            }
            mrf_metadata_rows.append(mrf_metadata_row)
        
        # MRF Size
        mrf_size_row = {
            'in_network_file_name': file_name,
            'in_network_file_size': '',  # Will be populated later if needed
            'remarks': '',
            'carrier': CARRIER_NAME,
            'batch': '2024-10'
        }
        mrf_size_rows.append(mrf_size_row)
    
    return metadata_rows, mrf_metadata_rows, mrf_size_rows

def process_anthem_file(file_path):
    """Process the Anthem file and write directly to CSVs"""
    try:
        print("Starting file processing...")
        total_objects = 0
        reporting_structure_index = 0
        
        # Initialize CSV writers
        with open(config.TOC_METADATA_CSV, 'w', newline='') as f1, \
             open(config.TOC_MRF_METADATA_CSV, 'w', newline='') as f2, \
             open(config.TOC_MRF_SIZE_DATA_CSV, 'w', newline='') as f3:
            
            # Write headers
            writer1 = csv.DictWriter(f1, fieldnames=['carrier', 'dh_re_id', 're_name', 'toc_source_url', 'batch', 
                                                    'toc_file_name', 'toc_file_url', 'toc_or_mrf_file', 
                                                    'mrf_file_plan_name', 'reporting_structure_index', 'remarks'])
            writer2 = csv.DictWriter(f2, fieldnames=['reporting_entity_name', 'reporting_entity_type', 'reporting_structure',
                                                    'in_network_file_name', 'in_network_file_location', 'in_network_file_description',
                                                    'allowed_amount_file_name', 'allowed_amount_file_location', 'allowed_amount_file_description',
                                                    'plan_name', 'plan_id_type', 'plan_id', 'plan_market_type', 'toc_source_file_name',
                                                    'parsed_date', 'carrier', 'batch'])
            writer3 = csv.DictWriter(f3, fieldnames=['in_network_file_name', 'in_network_file_size', 'remarks', 'carrier', 'batch'])
            
            writer1.writeheader()
            writer2.writeheader()
            writer3.writeheader()
            
            # Process file
            with open(file_path, 'r') as f:
                # Skip the initial [
                f.readline()
                
                while True:
                    batch = []
                    for _ in range(BATCH_SIZE):
                        line = f.readline().strip()
                        if not line or line == ']':
                            break
                        if line.endswith(','):
                            line = line[:-1]
                        batch.append(line)
                    
                    if not batch:
                        break
                    
                    # Process batch
                    for line in batch:
                        try:
                            obj = json.loads(line)
                            reporting_structure_index += 1
                            metadata_rows, mrf_metadata_rows, mrf_size_rows = process_json_object(obj, reporting_structure_index)
                            
                            # Write to CSVs
                            writer1.writerows(metadata_rows)
                            writer2.writerows(mrf_metadata_rows)
                            writer3.writerows(mrf_size_rows)
                            
                            total_objects += 1
                            
                            if total_objects % 100 == 0:
                                progress = (f.tell() / os.path.getsize(file_path)) * 100
                                print(f"\rProgress: {progress:.2f}% | Objects: {total_objects:,} | Memory: {psutil.Process().memory_info().rss/1024/1024:.0f}MB", end='')
                                sys.stdout.flush()
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error: {str(e)}")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing object: {str(e)}")
                            continue
        
        print(f"\nProcessing completed successfully")
        print(f"Total objects processed: {total_objects:,}")
        return True
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        logger.error(traceback.format_exc())
        print(f"An error occurred: {str(e)}")
        return False

def download_anthem_file():
    """Download the Anthem index file"""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, ANTHEM_FILE_NAME)
    
    try:
        with requests.get(ANTHEM_URL, stream=True) as response:
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            progress_size = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=BUFFER_SIZE):
                    f.write(chunk)
                    progress_size += len(chunk)
                    if total_size:
                        progress = (progress_size / total_size) * 100
                        print(f"\rDownloading: {progress:.1f}%", end='')
            
            print("\nDownload completed successfully")
            return file_path
    except requests.RequestException as e:
        logger.error(f"Error downloading file: {str(e)}")
        print(f"Error downloading file: {str(e)}")
        return None

def unzip_file(file_path):
    """Unzip the downloaded file"""
    unzipped_path = os.path.join(DOWNLOAD_DIR, UNZIPPED_FILE_NAME)
    try:
        total_size = os.path.getsize(file_path)
        progress_size = 0
        
        with gzip.open(file_path, 'rb') as f_in:
            with open(unzipped_path, 'wb') as f_out:
                while True:
                    chunk = f_in.read(BUFFER_SIZE)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    progress_size += len(chunk)
                    progress = (progress_size / total_size) * 100
                    print(f"\rUnzipping: {progress:.1f}%", end='')
        
        print("\nUnzipping completed successfully")
        return unzipped_path
    except Exception as e:
        logger.error(f"Error unzipping file: {str(e)}")
        print(f"Error unzipping file: {str(e)}")
        return None

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Process Anthem index file")
    parser.add_argument("--process-only", action="store_true", help="Process existing file without downloading")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        tracemalloc.start()
        start_time = time.time()
        
        if args.process_only:
            unzipped_file = os.path.join(DOWNLOAD_DIR, UNZIPPED_FILE_NAME)
            if os.path.exists(unzipped_file):
                print(f"Processing existing file: {unzipped_file}")
                success = process_anthem_file(unzipped_file)
            else:
                print(f"File not found: {unzipped_file}")
                success = False
        else:
            print("Downloading file...")
            downloaded_file = download_anthem_file()
            if downloaded_file:
                print("Unzipping file...")
                unzipped_file = unzip_file(downloaded_file)
                if unzipped_file:
                    success = process_anthem_file(unzipped_file)
                else:
                    success = False
            else:
                success = False
        
        end_time = time.time()
        current, peak = tracemalloc.get_traced_memory()
        
        print("\nExecution Summary:")
        print(f"Status: {'Successful' if success else 'Failed'}")
        print(f"Total Time: {(end_time - start_time) / 60:.2f} minutes")
        print(f"Peak Memory Usage: {peak / 10**6:.2f}MB")
        print(f"Final Memory Usage: {current / 10**6:.2f}MB")
        
        tracemalloc.stop()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        logger.error(traceback.format_exc())
        print(f"An unexpected error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
