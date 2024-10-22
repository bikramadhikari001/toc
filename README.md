# Transparency in Coverage Data Processor

This project is designed to download and process Transparency in Coverage (ToC) data from healthcare providers. It automates the process of fetching JSON files, extracting relevant information, and generating standardized CSV outputs. The project has been optimized to handle large JSON files efficiently, including the 20GB Anthem index file.

## Features

- Automated download of ToC JSON files from specified URLs
- Memory-efficient processing of large JSON files using streaming parser (ijson)
- Parallel processing of downloaded JSON files to extract metadata, MRF (Machine-Readable File) metadata, and MRF size information
- Chunked writing of data to CSV files to manage memory usage
- Generation of three CSV files:
  - toc_metadata.csv
  - toc_mrf_metadata.csv
  - toc_mrf_size_data.csv
- Configurable settings via config.py
- Comprehensive error handling and logging
- Progress tracking for long-running processes
- Unit tests for key functions
- Support for both streaming and non-streaming processing approaches
- Option to process existing files without re-downloading

## Prerequisites

- Python 3.x
- Chrome browser
- ChromeDriver (matching your Chrome version)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/bikramadhikari001/toc.git
   cd toc
   ```

2. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

3. Ensure ChromeDriver is installed and in your system PATH.

4. Create an `input_url.txt` file in the project root directory and add the URL of the ToC data source.

## Configuration

You can customize the behavior of the application by modifying the `config.py` file. This file contains settings such as:

- Number of files to process
- Download directory
- Output CSV file names
- WebDriver wait times
- Logging configuration
- Chunk size for CSV writing
- Maximum number of worker processes and threads
- Timeout for file size retrieval requests

## Usage

Run the main script:

```
python main.py
```

This will:
1. Download JSON files from the specified URL
2. Process the downloaded files using memory-efficient streaming methods
3. Generate three CSV files with the extracted data

For processing the large Anthem index file, use:

```
python anthem.py
```

This script is specifically optimized to handle the 20GB Anthem index JSON file using a streaming approach.

To process an existing Anthem file without downloading:

```
python anthem.py --process-only
```

This option assumes that the file has already been downloaded and unzipped in the `downloads` directory.

## Logging

The application logs its activities to a file specified in `config.py`. By default, this is set to `toc_processor.log`. You can adjust the log level and file name in the configuration file.

The logging has been improved to show progress during the processing of the Anthem file. It now logs:
- The start of processing for each object
- A summary every 1000 objects processed
- The total number of objects processed at the end

## Running Tests

To run the unit tests:

```
python -m unittest test_main.py
```

## File Descriptions

- `main.py`: The main script that orchestrates the download and processing of files
- `anthem.py`: Specialized script for processing the large Anthem index file
- `toc_metadata_processor.py`: Processes and generates toc_metadata.csv
- `toc_mrf_metadata_processor.py`: Processes and generates toc_mrf_metadata.csv
- `toc_mrf_size_processor.py`: Processes and generates toc_mrf_size_data.csv
- `config.py`: Contains configuration settings for the application
- `test_main.py`: Contains unit tests for key functions

## Output

The script generates three CSV files in the project root directory:
1. `toc_metadata.csv`: Contains general metadata about the ToC files
2. `toc_mrf_metadata.csv`: Contains metadata specific to the Machine-Readable Files
3. `toc_mrf_size_data.csv`: Contains size information for the in-network files

## Performance Considerations

This version of the project has been optimized to handle large JSON files efficiently:
- It uses a streaming JSON parser (ijson) to process large files without loading them entirely into memory.
- Data is written to CSV files in chunks to manage memory usage.
- Parallel processing is used to speed up file processing.
- Concurrent requests are used for file size retrieval to improve performance.
- The Anthem index file processor (anthem.py) is specifically designed to handle the 20GB JSON file using a streaming approach.

## Contributing

Contributions to improve the project are welcome. Please follow these steps:

1. Fork the repository
2. Create a new branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License.

## Contact

Bikram Adhikari - bikramadhikari001@gmail.com

Project Link: [https://github.com/bikramadhikari001/toc](https://github.com/bikramadhikari001/toc)
