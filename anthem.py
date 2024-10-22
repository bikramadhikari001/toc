import os
import logging
import requests
import gzip
import shutil
import json
import argparse
import gc
import traceback
import psutil
from toc_metadata_processor import process_and_write_toc_metadata
from toc_mrf_metadata_processor import process_and_write_toc_mrf_metadata
from toc_mrf_size_processor import process_and_write_toc_mrf_size_data
import config
import tracemalloc

# Set up logging
logging.basicConfig(filename=config.LOG_FILE, level=config.LOG_LEVEL,
                    format='%(asctime)s - %(levelname)s - %(message)s')

ANTHEM_URL = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-10-01_anthem_index.json.gz"
DOWNLOAD_DIR = "downloads"
ANTHEM_FILE_NAME = "anthem_index.json.gz"
UNZIPPED_FILE_NAME = "anthem_index.json"
CARRIER_NAME = "anthem"
BATCH_SIZE = 50  # Further reduced batch size for better memory management

def download_anthem_file():
    """
    Download the Anthem file from the specified URL.
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, ANTHEM_FILE_NAME)
    
    try:
        with requests.get(ANTHEM_URL, stream=True) as response:
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

def read_json_file(file_path):
    """
    Generator function to read JSON objects from the file.
    """
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line == '[' or line == ']':
                continue
            if line.endswith(','):
                line = line[:-1]
            
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON: {str(e)}")

def get_memory_usage():
    """
    Get current memory usage of the process.
    """
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # in MB

def process_anthem_file(file_path):
    """
    Process the Anthem file using streaming processors and batch processing.
    """
    try:
        with process_and_write_toc_metadata(config.TOC_METADATA_CSV, CARRIER_NAME, streaming=True) as toc_metadata_processor, \
             process_and_write_toc_mrf_metadata(config.TOC_MRF_METADATA_CSV, CARRIER_NAME, streaming=True) as toc_mrf_metadata_processor, \
             process_and_write_toc_mrf_size_data(config.TOC_MRF_SIZE_DATA_CSV, CARRIER_NAME, streaming=True) as toc_mrf_size_processor:

            batch = []
            object_count = 0
            error_count = 0

            for obj in read_json_file(file_path):
                batch.append(obj)
                object_count += 1

                if len(batch) >= BATCH_SIZE:
                    for item in batch:
                        try:
                            toc_metadata_processor.process(item)
                            toc_mrf_metadata_processor.process(item)
                            toc_mrf_size_processor.process(item)
                        except Exception as e:
                            logging.error(f"Error processing item: {str(e)}")
                            error_count += 1
                    
                    logging.info(f"Processed batch of {len(batch)} records")
                    batch.clear()
                    gc.collect()  # Periodic garbage collection
                    
                    memory_usage = get_memory_usage()
                    logging.info(f"Current memory usage: {memory_usage:.2f} MB")

                if object_count % 1000 == 0:
                    logging.info(f"Processed {object_count} objects, Errors: {error_count}")
                    print(f"Processed {object_count} objects, Errors: {error_count}", end='\r')  # Progress indicator

            # Process any remaining objects in the last batch
            if batch:
                for item in batch:
                    try:
                        toc_metadata_processor.process(item)
                        toc_mrf_metadata_processor.process(item)
                        toc_mrf_size_processor.process(item)
                    except Exception as e:
                        logging.error(f"Error processing item: {str(e)}")
                        error_count += 1
                logging.info(f"Processed final batch of {len(batch)} records")

        logging.info(f"Successfully processed {file_path}. Total objects processed: {object_count}, Errors: {error_count}")
        print(f"\nSuccessfully processed {object_count} objects, Errors: {error_count}")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {str(e)}")
        logging.error(traceback.format_exc())

def main():
    """
    Main function to download, unzip, and process the Anthem file.
    """
    parser = argparse.ArgumentParser(description="Process Anthem index file")
    parser.add_argument("--process-only", action="store_true", help="Process the existing file without downloading")
    args = parser.parse_args()

    try:
        tracemalloc.start()
        
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
        
        current, peak = tracemalloc.get_traced_memory()
        logging.info(f"Final memory usage: Current: {current / 10**6:.2f}MB; Peak: {peak / 10**6:.2f}MB")
        print(f"Final memory usage: Current: {current / 10**6:.2f}MB; Peak: {peak / 10**6:.2f}MB")
        tracemalloc.stop()
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    main()
