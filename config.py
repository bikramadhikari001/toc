# Configuration settings for the Transparency in Coverage Data Processor

# Number of JSON files to download and process
NUM_FILES_TO_PROCESS = 5

# Download directory
DOWNLOAD_DIR = "downloads"

# Output CSV file names
TOC_METADATA_CSV = "toc_metadata.csv"
TOC_MRF_METADATA_CSV = "toc_mrf_metadata.csv"
TOC_MRF_SIZE_DATA_CSV = "toc_mrf_size_data.csv"

# Selenium WebDriver settings
WEBDRIVER_WAIT_TIME = 20
DOWNLOAD_WAIT_TIME = 10

# Logging configuration
LOG_FILE = "toc_processor.log"
LOG_LEVEL = "INFO"

# New configuration parameters
# Chunk size for writing to CSV files
CSV_CHUNK_SIZE = 1000

# Maximum number of worker processes for multiprocessing
MAX_WORKERS = 4

# Maximum number of threads for concurrent file size retrieval
MAX_THREADS_FILE_SIZE = 10

# Timeout for file size retrieval requests (in seconds)
FILE_SIZE_REQUEST_TIMEOUT = 10
