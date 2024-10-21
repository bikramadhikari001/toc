import os
import json
import time
import logging
import ijson
from typing import List, Dict, Any, Iterator
from concurrent.futures import ProcessPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from toc_metadata_processor import process_and_write_toc_metadata
from toc_mrf_metadata_processor import process_and_write_toc_mrf_metadata
from toc_mrf_size_processor import process_and_write_toc_mrf_size_data
import config
from tqdm import tqdm

# Set up logging
logging.basicConfig(filename=config.LOG_FILE, level=config.LOG_LEVEL,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def setup_chrome_driver() -> webdriver.Chrome:
    """
    Set up and configure the Chrome WebDriver.

    Returns:
        webdriver.Chrome: Configured Chrome WebDriver instance.
    """
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": os.path.abspath(config.DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    
    return webdriver.Chrome(options=chrome_options)

def download_json_files(url: str, num_files: int = config.NUM_FILES_TO_PROCESS) -> List[str]:
    """
    Download JSON files from the given URL.

    Args:
        url (str): The URL to download files from.
        num_files (int): Number of files to download.

    Returns:
        List[str]: List of downloaded file paths.
    """
    driver = setup_chrome_driver()
    driver.get(url)
    
    downloaded_files = []
    try:
        links = WebDriverWait(driver, config.WEBDRIVER_WAIT_TIME).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.ant-list-item a"))
        )
        
        for i, link in enumerate(tqdm(links[:num_files], desc="Downloading files")):
            filename = link.text
            link.click()
            
            logging.info(f"Downloading {filename}...")
            time.sleep(config.DOWNLOAD_WAIT_TIME)  # Wait for download to complete
            
            downloaded_file = os.path.join(config.DOWNLOAD_DIR, filename)
            if os.path.exists(downloaded_file):
                logging.info(f"File downloaded successfully: {downloaded_file}")
                downloaded_files.append(downloaded_file)
            else:
                logging.error(f"File download failed: {filename}")
    
    except Exception as e:
        logging.error(f"An error occurred during download: {str(e)}")
    
    finally:
        driver.quit()
    
    return downloaded_files

def process_single_file(json_file: str) -> None:
    """
    Process a single JSON file and write the extracted data to CSV files.

    Args:
        json_file (str): Path to the JSON file to process.
    """
    logging.info(f"Processing file: {json_file}")
    try:
        process_and_write_toc_metadata(json_file, config.TOC_METADATA_CSV)
        process_and_write_toc_mrf_metadata(json_file, config.TOC_MRF_METADATA_CSV)
        process_and_write_toc_mrf_size_data(json_file, config.TOC_MRF_SIZE_DATA_CSV)
    except ijson.JSONError:
        logging.error(f"Invalid JSON in file: {json_file}")
    except Exception as e:
        logging.error(f"Error processing file {json_file}: {str(e)}")

def process_json_files(json_files: List[str]) -> None:
    """
    Process the downloaded JSON files in parallel and generate CSV outputs.

    Args:
        json_files (List[str]): List of JSON file paths to process.
    """
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        list(tqdm(executor.map(process_single_file, json_files), total=len(json_files), desc="Processing files"))

def main():
    """
    Main function to orchestrate the download and processing of JSON files.
    """
    try:
        with open('input_url.txt', 'r') as f:
            base_url = f.read().strip()

        downloaded_files = download_json_files(base_url)
        process_json_files(downloaded_files)

        logging.info("Processing completed for all downloaded JSON files.")
    except FileNotFoundError:
        logging.error("input_url.txt file not found.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()
