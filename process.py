import os
import logging
from toc_metadata_processor import process_and_write_toc_metadata
from toc_mrf_metadata_processor import process_and_write_toc_mrf_metadata
from toc_mrf_size_processor import process_and_write_toc_mrf_size_data
import config

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    downloads_dir = os.path.join(os.getcwd(), config.DOWNLOAD_DIR)
    json_files = [f for f in os.listdir(downloads_dir) if f.endswith('.json')]
    
    if not json_files:
        logger.warning("No JSON files found in the downloads directory.")
        return

    for json_file in json_files:
        full_path = os.path.join(downloads_dir, json_file)
        logger.info(f"Processing file: {full_path}")

        process_and_write_toc_metadata(full_path, config.TOC_METADATA_CSV)
        process_and_write_toc_mrf_metadata(full_path, config.TOC_MRF_METADATA_CSV)
        process_and_write_toc_mrf_size_data(full_path, config.TOC_MRF_SIZE_DATA_CSV)
    
    logger.info("CSV file creation process completed.")

if __name__ == "__main__":
    main()
