import os
import logging
import requests
import gzip
import shutil
import ijson
import argparse
from toc_metadata_processor import process_and_write_toc_metadata
from toc_mrf_metadata_processor import process_and_write_toc_mrf_metadata
from toc_mrf_size_processor import process_and_write_toc_mrf_size_data
import config

# Set up logging
logging.basicConfig(filename=config.LOG_FILE, level=config.LOG_LEVEL,
                    format='%(asctime)s - %(levelname)s - %(message)s')

ANTHEM_URL = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-10-01_anthem_index.json.gz"
DOWNLOAD_DIR = "downloads"
ANTHEM_FILE_NAME = "anthem_index.json.gz"
UNZIPPED_FILE_NAME = "anthem_index.json"
CARRIER_NAME = "anthem"

def download_anthem_file():
    """
    Download the Anthem file from the specified URL.
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, ANTHEM_FILE_NAME)
    
    try:
        response = requests.get(ANTHEM_URL, stream=True)
        response.raise_for_status()
        
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logging.info(f"Successfully downloaded {ANTHEM_FILE_NAME}")
        return file_path
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading file: {str(e)}")
        return None

def unzip_file(file_path):
    """
    Unzip the downloaded .gz file.
    """
    unzipped_path = os.path.join(DOWNLOAD_DIR, UNZIPPED_FILE_NAME)
    with gzip.open(file_path, 'rb') as f_in:
        with open(unzipped_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    logging.info(f"Successfully unzipped {ANTHEM_FILE_NAME}")
    return unzipped_path

def process_anthem_file(file_path):
    """
    Process the Anthem file using streaming processors.
    """
    try:
        toc_metadata_processor = process_and_write_toc_metadata(config.TOC_METADATA_CSV, CARRIER_NAME, streaming=True)
        toc_mrf_metadata_processor = process_and_write_toc_mrf_metadata(config.TOC_MRF_METADATA_CSV, CARRIER_NAME, streaming=True)
        toc_mrf_size_processor = process_and_write_toc_mrf_size_data(config.TOC_MRF_SIZE_DATA_CSV, CARRIER_NAME, streaming=True)

        with open(file_path, 'rb') as file:
            parser = ijson.parse(file)
            
            current_object = {}
            object_count = 0
            in_reporting_structure = False
            current_in_network_file = {}
            current_reporting_plan = {}

            for prefix, event, value in parser:
                if prefix == 'reporting_entity_name':
                    current_object['reporting_entity_name'] = value
                elif prefix == 'reporting_entity_type':
                    current_object['reporting_entity_type'] = value
                elif prefix == 'reporting_structure' and event == 'start_array':
                    in_reporting_structure = True
                    current_object['in_network_files'] = []
                    current_object['reporting_plans'] = []
                elif prefix == 'reporting_structure.item' and event == 'end_map':
                    if in_reporting_structure:
                        object_count += 1
                        
                        # Process the current object with all processors
                        toc_metadata_processor.process(current_object)
                        logging.info(f"Record added to {config.TOC_METADATA_CSV}")
                        
                        toc_mrf_metadata_processor.process(current_object)
                        logging.info(f"Record added to {config.TOC_MRF_METADATA_CSV}")
                        
                        toc_mrf_size_processor.process(current_object)
                        logging.info(f"Record added to {config.TOC_MRF_SIZE_DATA_CSV}")
                        
                        # Reset for next object
                        current_object = {'in_network_files': [], 'reporting_plans': []}
                elif prefix.endswith('.in_network_files.item') and event == 'start_map':
                    current_in_network_file = {}
                elif prefix.endswith('.in_network_files.item') and event == 'end_map':
                    current_object['in_network_files'].append(current_in_network_file)
                elif prefix.endswith('.in_network_files.item.location'):
                    current_in_network_file['location'] = value
                elif prefix.endswith('.in_network_files.item.description'):
                    current_in_network_file['description'] = value
                elif prefix.endswith('.reporting_plans.item') and event == 'start_map':
                    current_reporting_plan = {}
                elif prefix.endswith('.reporting_plans.item') and event == 'end_map':
                    current_object['reporting_plans'].append(current_reporting_plan)
                elif prefix.endswith('.reporting_plans.item.plan_name'):
                    current_reporting_plan['plan_name'] = value
                elif prefix.endswith('.reporting_plans.item.plan_id_type'):
                    current_reporting_plan['plan_id_type'] = value
                elif prefix.endswith('.reporting_plans.item.plan_id'):
                    current_reporting_plan['plan_id'] = value
                elif prefix.endswith('.reporting_plans.item.plan_market_type'):
                    current_reporting_plan['plan_market_type'] = value
            
        # Finalize all processors
        toc_metadata_processor.finalize()
        toc_mrf_metadata_processor.finalize()
        toc_mrf_size_processor.finalize()
        
        logging.info(f"Successfully processed {file_path}. Total objects processed: {object_count}")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {str(e)}")

def main():
    """
    Main function to download, unzip, and process the Anthem file.
    """
    parser = argparse.ArgumentParser(description="Process Anthem index file")
    parser.add_argument("--process-only", action="store_true", help="Process the existing file without downloading")
    args = parser.parse_args()

    try:
        if args.process_only:
            unzipped_file = os.path.join(DOWNLOAD_DIR, UNZIPPED_FILE_NAME)
            if os.path.exists(unzipped_file):
                logging.info("Processing existing Anthem file.")
                process_anthem_file(unzipped_file)
            else:
                logging.error(f"File not found: {unzipped_file}")
        else:
            downloaded_file = download_anthem_file()
            if downloaded_file:
                unzipped_file = unzip_file(downloaded_file)
                process_anthem_file(unzipped_file)
        logging.info("Anthem file processing completed.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()
