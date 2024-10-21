import unittest
from unittest.mock import patch, MagicMock
import os
import json
from main import setup_chrome_driver, download_json_files, process_single_file, process_json_files
import config

class TestMain(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory for test files
        self.test_dir = 'test_downloads'
        os.makedirs(self.test_dir, exist_ok=True)

    def tearDown(self):
        # Remove the temporary directory after tests
        for file in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, file))
        os.rmdir(self.test_dir)

    @patch('main.webdriver.Chrome')
    def test_setup_chrome_driver(self, mock_chrome):
        driver = setup_chrome_driver()
        self.assertIsNotNone(driver)
        mock_chrome.assert_called_once()

    @patch('main.setup_chrome_driver')
    @patch('main.WebDriverWait')
    def test_download_json_files(self, mock_wait, mock_setup_driver):
        mock_driver = MagicMock()
        mock_setup_driver.return_value = mock_driver
        mock_wait.return_value.until.return_value = [MagicMock(text='file1.json'), MagicMock(text='file2.json')]

        with patch('main.os.path.exists', return_value=True):
            result = download_json_files('http://example.com', num_files=2)

        self.assertEqual(len(result), 2)
        self.assertTrue(all(file.endswith('.json') for file in result))

    def test_process_single_file(self):
        # Create a mock JSON file
        test_file = os.path.join(self.test_dir, 'test.json')
        test_data = {
            'reporting_entity_name': 'Test Entity',
            'reporting_entity_type': 'Test Type',
            'in_network_files': [{'description': 'Test File', 'location': 'http://example.com/file.json'}]
        }
        with open(test_file, 'w') as f:
            json.dump(test_data, f)

        result = process_single_file(test_file)

        self.assertIn('toc_metadata', result)
        self.assertIn('toc_mrf_metadata', result)
        self.assertIn('toc_mrf_size_data', result)
        self.assertTrue(all(isinstance(data, list) for data in result.values()))

    @patch('main.write_toc_metadata_csv')
    @patch('main.write_toc_mrf_metadata_csv')
    @patch('main.write_toc_mrf_size_csv')
    def test_process_json_files(self, mock_write_size, mock_write_mrf, mock_write_metadata):
        test_files = [os.path.join(self.test_dir, f'test{i}.json') for i in range(3)]
        for file in test_files:
            with open(file, 'w') as f:
                json.dump({}, f)

        process_json_files(test_files)

        mock_write_metadata.assert_called_once()
        mock_write_mrf.assert_called_once()
        mock_write_size.assert_called_once()

if __name__ == '__main__':
    unittest.main()
