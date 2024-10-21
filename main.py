import os
import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from toc_metadata_processor import process_toc_metadata, write_toc_metadata_csv
from toc_mrf_metadata_processor import process_toc_mrf_metadata, write_toc_mrf_metadata_csv
from toc_mrf_size_processor import process_toc_mrf_size_data, write_toc_mrf_size_csv

def setup_chrome_driver():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    downloads_dir = os.path.join(os.getcwd(), "downloads")
    os.makedirs(downloads_dir, exist_ok=True)
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": downloads_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    
    return webdriver.Chrome(options=chrome_options)

def download_json_files(url, num_files=5):
    driver = setup_chrome_driver()
    driver.get(url)
    
    downloaded_files = []
    try:
        links = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.ant-list-item a"))
        )
        
        for i, link in enumerate(links[:num_files], 1):
            filename = link.text
            link.click()
            
            print(f"Downloading {filename}...")
            time.sleep(10)  # Wait for download to complete
            
            downloaded_file = os.path.join("downloads", filename)
            if os.path.exists(downloaded_file):
                print(f"File downloaded successfully: {downloaded_file}")
                downloaded_files.append(downloaded_file)
            else:
                print(f"File download failed: {filename}")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    finally:
        driver.quit()
    
    return downloaded_files

def process_json_files(json_files):
    all_toc_metadata = []
    all_toc_mrf_metadata = []
    all_toc_mrf_size_data = []

    for json_file in json_files:
        print(f"Processing file: {json_file}")
        with open(json_file, 'r') as f:
            json_data = json.load(f)
        
        all_toc_metadata.extend(process_toc_metadata(json_data, json_file))
        all_toc_mrf_metadata.extend(process_toc_mrf_metadata(json_data))
        all_toc_mrf_size_data.extend(process_toc_mrf_size_data(json_data))

    write_toc_metadata_csv(all_toc_metadata, 'toc_metadata.csv')
    write_toc_mrf_metadata_csv(all_toc_mrf_metadata, 'toc_mrf_metadata.csv')
    write_toc_mrf_size_csv(all_toc_mrf_size_data, 'toc_mrf_size_data.csv')

def main():
    with open('input_url.txt', 'r') as f:
        base_url = f.read().strip()

    downloaded_files = download_json_files(base_url, num_files=5)
    process_json_files(downloaded_files)

    print("Processing completed for all downloaded JSON files.")

if __name__ == "__main__":
    main()
