import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pathlib import Path
import tempfile
import logging

# Make sure src.binance_downloader is in PYTHONPATH or adjust as needed for imports
# For the purpose of this environment, we assume direct import works if structure is tests/ and src/
from src.binance_downloader.utils import convert_csv_to_feather, convert_csv_directory_to_feather

# Dummy DataFrame for creating test CSV files
DUMMY_DF_CONTENT = pd.DataFrame({'colA': [1, 2], 'colB': ['data1', 'data2']})

class TestConvertCsvToFeather(unittest.TestCase):
    def setUp(self):
        self.temp_dir_obj = tempfile.TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_dir_obj.name)
        self.csv_path = self.temp_dir_path / "sample.csv"
        DUMMY_DF_CONTENT.to_csv(self.csv_path, index=False)
        # Suppress logging during tests unless specifically testing log output
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        self.temp_dir_obj.cleanup()
        logging.disable(logging.NOTSET) # Re-enable logging

    def test_convert_successfully(self):
        result = convert_csv_to_feather(self.csv_path, delete_csv=False)
        self.assertTrue(result)
        feather_path = self.csv_path.with_suffix(".feather")
        self.assertTrue(feather_path.exists())
        self.assertTrue(self.csv_path.exists()) # CSV should still exist

    def test_convert_and_delete_csv(self):
        result = convert_csv_to_feather(self.csv_path, delete_csv=True)
        self.assertTrue(result)
        feather_path = self.csv_path.with_suffix(".feather")
        self.assertTrue(feather_path.exists())
        self.assertFalse(self.csv_path.exists()) # CSV should be deleted

    def test_convert_non_existent_csv(self):
        non_existent_path = self.temp_dir_path / "non_existent.csv"
        result = convert_csv_to_feather(non_existent_path, delete_csv=False)
        self.assertFalse(result)
        self.assertFalse(non_existent_path.with_suffix(".feather").exists())

    @patch('pandas.read_csv')
    def test_conversion_failure_read_csv(self, mock_read_csv):
        mock_read_csv.side_effect = Exception("Failed to read CSV")
        result = convert_csv_to_feather(self.csv_path, delete_csv=False)
        self.assertFalse(result)
        self.assertFalse(self.csv_path.with_suffix(".feather").exists())
        self.assertTrue(self.csv_path.exists()) # Original CSV should still be there

    @patch('pandas.DataFrame.to_feather')
    def test_conversion_failure_to_feather(self, mock_to_feather):
        mock_to_feather.side_effect = Exception("Failed to write Feather")
        result = convert_csv_to_feather(self.csv_path, delete_csv=False)
        self.assertFalse(result)
        self.assertFalse(self.csv_path.with_suffix(".feather").exists())
        self.assertTrue(self.csv_path.exists()) # Original CSV should still be there

class TestConvertCsvDirectoryToFeather(unittest.TestCase):
    def setUp(self):
        self.temp_dir_obj = tempfile.TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_dir_obj.name)
        # Suppress logging during tests unless specifically testing log output
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        self.temp_dir_obj.cleanup()
        logging.disable(logging.NOTSET) # Re-enable logging

    def test_empty_directory(self):
        with patch('src.binance_downloader.utils.logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            convert_csv_directory_to_feather(self.temp_dir_path, delete_csv=False)
            self.assertEqual(len(list(self.temp_dir_path.glob("*.feather"))), 0)
            # Check log for summary
            mock_logger.info.assert_any_call(f"CSV to Feather conversion for directory {self.temp_dir_path} completed.")
            mock_logger.info.assert_any_call("Successful conversions: 0")
            mock_logger.info.assert_any_call("Failed conversions: 0")


    def test_directory_with_no_csv_files(self):
        (self.temp_dir_path / "somefile.txt").write_text("hello")
        with patch('src.binance_downloader.utils.logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            convert_csv_directory_to_feather(self.temp_dir_path, delete_csv=False)
            self.assertEqual(len(list(self.temp_dir_path.glob("*.feather"))), 0)
            mock_logger.info.assert_any_call("Successful conversions: 0")


    def test_directory_with_csv_files_no_delete(self):
        csv_path1 = self.temp_dir_path / "data1.csv"
        csv_path2 = self.temp_dir_path / "data2.csv"
        DUMMY_DF_CONTENT.to_csv(csv_path1, index=False)
        DUMMY_DF_CONTENT.to_csv(csv_path2, index=False)

        with patch('src.binance_downloader.utils.logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            convert_csv_directory_to_feather(self.temp_dir_path, delete_csv=False)
            self.assertTrue((self.temp_dir_path / "data1.feather").exists())
            self.assertTrue((self.temp_dir_path / "data2.feather").exists())
            self.assertTrue(csv_path1.exists())
            self.assertTrue(csv_path2.exists())
            mock_logger.info.assert_any_call("Successful conversions: 2")

    def test_directory_with_csv_files_with_delete(self):
        csv_path1 = self.temp_dir_path / "data1.csv"
        csv_path2 = self.temp_dir_path / "data2.csv"
        DUMMY_DF_CONTENT.to_csv(csv_path1, index=False)
        DUMMY_DF_CONTENT.to_csv(csv_path2, index=False)

        with patch('src.binance_downloader.utils.logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            convert_csv_directory_to_feather(self.temp_dir_path, delete_csv=True)
            self.assertTrue((self.temp_dir_path / "data1.feather").exists())
            self.assertTrue((self.temp_dir_path / "data2.feather").exists())
            self.assertFalse(csv_path1.exists())
            self.assertFalse(csv_path2.exists())
            mock_logger.info.assert_any_call("Successful conversions: 2")

    def test_non_existent_directory(self):
        non_existent_dir = self.temp_dir_path / "non_existent_sub_dir"
        with patch('src.binance_downloader.utils.logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            convert_csv_directory_to_feather(non_existent_dir, delete_csv=False)
            # Check that an error was logged
            mock_logger.error.assert_called_with(f"Directory not found or not a directory: {non_existent_dir}")

    @patch('src.binance_downloader.utils.convert_csv_to_feather')
    def test_partial_failure_in_directory(self, mock_convert_single):
        csv_path1 = self.temp_dir_path / "good.csv"
        csv_path2 = self.temp_dir_path / "bad.csv"
        csv_path3 = self.temp_dir_path / "another_good.csv"
        DUMMY_DF_CONTENT.to_csv(csv_path1, index=False)
        DUMMY_DF_CONTENT.to_csv(csv_path2, index=False)
        DUMMY_DF_CONTENT.to_csv(csv_path3, index=False)

        # Simulate failure for bad.csv, success for others
        def side_effect_convert(path, delete):
            if path.name == "bad.csv":
                return False
            # Actual conversion for good files isn't mocked here, so they won't produce .feather
            # For this test, we only care about the counts returned by convert_csv_to_feather
            # A more integrated test would let the original function run for successful files.
            # However, the prompt asks to mock `convert_csv_to_feather`
            # To make feather files appear for "good" ones, we'd need to call original or create them manually
            if path.name == "good.csv":
                (path.parent / "good.feather").touch() # Create dummy feather file
                if delete: path.unlink()
                return True
            if path.name == "another_good.csv":
                (path.parent / "another_good.feather").touch()
                if delete: path.unlink()
                return True
            return False

        mock_convert_single.side_effect = side_effect_convert

        with patch('src.binance_downloader.utils.logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            convert_csv_directory_to_feather(self.temp_dir_path, delete_csv=False)

            self.assertTrue((self.temp_dir_path / "good.feather").exists())
            self.assertFalse((self.temp_dir_path / "bad.feather").exists()) # Should not exist
            self.assertTrue((self.temp_dir_path / "another_good.feather").exists())

            self.assertTrue((self.temp_dir_path / "good.csv").exists()) # delete_csv=False
            self.assertTrue((self.temp_dir_path / "bad.csv").exists())
            self.assertTrue((self.temp_dir_path / "another_good.csv").exists())

            mock_logger.info.assert_any_call("Successful conversions: 2")
            mock_logger.info.assert_any_call("Failed conversions: 1")

if __name__ == '__main__':
    unittest.main()
