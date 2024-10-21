import os
import json
from toc_metadata_processor import process_and_write_toc_metadata
from toc_mrf_metadata_processor import process_and_write_toc_mrf_metadata
from toc_mrf_size_processor import process_and_write_toc_mrf_size

def main():
    downloads_dir = os.path.join(os.getcwd(), "downloads")
    json_files = [f for f in os.listdir(downloads_dir) if f.endswith('.json')]
    
    if not json_files:
        print("No JSON files found in the downloads directory.")
        return

    json_file = os.path.join(downloads_dir, json_files[0])
    print(f"Processing file: {json_file}")

    with open(json_file, 'r') as f:
        json_data = json.load(f)
    
    process_and_write_toc_metadata(json_data, json_file, 'toc_metadata.csv')
    process_and_write_toc_mrf_metadata(json_data, 'toc_mrf_metadata.csv')
    process_and_write_toc_mrf_size(json_data, 'toc_mrf_size_data.csv')
    
    print("CSV file creation process completed.")

if __name__ == "__main__":
    main()
