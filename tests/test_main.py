import asyncio
import tempfile
from pathlib import Path
from unittest.mock import patch
import pytest

from src.binance_downloader.core import DataMarket, DataType, Frequency
from src.binance_downloader.downloader import BinanceDataDownloader
from src.binance_downloader.main import main
from src.binance_downloader.utils import parse_arguments, setup_logging


class TestUtils:  # Renamed from TestArgumentParsing
    """Test cases for utility functions (argument parsing, logging setup)."""

    def test_parse_arguments_defaults(self, monkeypatch):
        """Test parsing arguments with defaults."""
        monkeypatch.setattr("sys.argv", ["somescript.py"])
        args = parse_arguments()
        assert args.data_markets == [market.value for market in DataMarket]
        assert args.data_types == [dt.value for dt in DataType]
        assert args.symbols is None # Default for symbols is now None
        assert args.output_directory == Path("./downloaded_data")
        assert args.log_level == "INFO"

    def test_parse_arguments_custom(self, monkeypatch):
        """Test parsing custom arguments."""
        custom_args = [
            "somescript.py",
            "--data-markets",
            "spot",
            "futures-um",
            "--data-types",
            "klines",
            "--symbols",
            "ETHUSDT",
            "ADAUSDT",
            "--start-date",
            "2023-01-01",
            "--output-directory",
            "/custom/dir",
            "--log-level",
            "DEBUG",
        ]
        monkeypatch.setattr("sys.argv", custom_args)
        args = parse_arguments()
        assert args.data_markets == ["spot", "futures-um"]
        assert args.data_types == ["klines"]
        assert args.symbols == ["ETHUSDT", "ADAUSDT"]
        assert args.start_date == "2023-01-01"
        assert args.output_directory == Path("/custom/dir")
        assert args.log_level == "DEBUG"

    def test_parse_arguments_no_symbols(self, monkeypatch):
        """Test parsing arguments when --symbols is not provided."""
        monkeypatch.setattr("sys.argv", ["somescript.py", "--data-markets", "spot"])
        args = parse_arguments()
        assert args.symbols is None # Should be None to indicate all symbols

    def test_parse_arguments_symbols_with_no_values(self, monkeypatch):
        """Test parsing arguments when --symbols is provided with no values."""
        monkeypatch.setattr("sys.argv", ["somescript.py", "--data-markets", "spot", "--symbols"])
        args = parse_arguments()
        assert args.symbols == [] # argparse behavior for nargs="*" with no values is an empty list.
                                  # This will be treated as "fetch all" by the downloader logic.

    def test_parse_arguments_symbols_with_values(self, monkeypatch):
        """Test parsing arguments when --symbols is provided with values."""
        monkeypatch.setattr("sys.argv", ["somescript.py", "--symbols", "BTCUSDT", "ETHUSDT"])
        args = parse_arguments()
        assert args.symbols == ["BTCUSDT", "ETHUSDT"]

    def test_setup_logging(self):
        """Test logging setup."""
        # This test can be expanded to check log handlers or levels
        # For now, just ensure it runs without error
        setup_logging("DEBUG")
        # Add assertions here if needed, e.g., check logger properties


# Integration Tests
class TestIntegration:
    """Integration tests for the complete system."""

    def setup_method(self):
        """Setup test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_end_to_end_workflow_main_entry(self, monkeypatch):
        """Test the complete end-to-end workflow via main entry point."""
        # Mock arguments
        mock_args_list = [
            "script_name",
            "--data-markets",
            "spot",
            "--data-types",
            "aggTrades", # Using a non-kline type to avoid frequency issues
            "--symbols",
            "BTCUSDT",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-01-01",
            "--output-directory",
            str(self.temp_dir),
            "--log-level",
            "DEBUG",
        ]
        monkeypatch.setattr("sys.argv", mock_args_list)

        # Mock the actual download part within BinanceDataDownloader
        with patch(
            "src.binance_downloader.downloader.BinanceDataDownloader.download_data"
        ) as mock_download_method:
            mock_download_method.return_value = None # Corrected: No need to await asyncio.sleep(0) here

            await main() # Call the main async function

            # Verify download_data was called with expected parameters
            mock_download_method.assert_called_once()
            call_args = mock_download_method.call_args[1]

            assert call_args['markets'] == [DataMarket.SPOT]
            assert call_args['data_types'] == [DataType.AGG_TRADES]
            assert call_args['symbols'] == ["BTCUSDT"]
            # Frequencies might be empty or default depending on logic for aggTrades
            assert call_args['start_date'] == "2025-01-01"
            assert call_args['end_date'] == "2025-01-01"
            # BinanceDataDownloader instance is created with output_directory,
            # so we don't check it in the call_args of download_data directly.
            # Instead, we ensured the mocked args for parse_arguments had the correct path.

    @pytest.mark.asyncio
    async def test_downloader_integration_within_main_context(self):
        """
        More focused integration test for BinanceDataDownloader's download_data,
        simulating how it's called from main().
        """
        downloader = BinanceDataDownloader(self.temp_dir)

        # Mock the underlying FileDownloader.download_tasks
        with patch.object(
            downloader.file_downloader, "download_tasks"
        ) as mock_file_download_tasks:
            # Simulate successful download of one task
            # The URL here is a placeholder as task generation is part of download_data
            mock_file_download_tasks.return_value = {"https://example.com/test.zip": True, "https://example.com/test2.zip": True} # Adjusted for 2 tasks

            await downloader.download_data(
                markets=[DataMarket.SPOT],
                data_types=[DataType.TRADES], # Using a non-kline type
                symbols=["ETHUSDT"],
                frequencies=[], # Explicitly empty for non-kline
                start_date="2025-02-01",
                end_date="2025-02-01",
                download_interval_type='both', # Added argument
            )

            mock_file_download_tasks.assert_called_once()
            # Further assertions can be made on the tasks passed to download_tasks
            # For example, checking the number of tasks or their properties
            tasks_passed = mock_file_download_tasks.call_args[0][0]
            # For SPOT, TRADES, 2025-02-01 to 2025-02-01, it generates:
            # 1. spot/daily/trades/ETHUSDT/ETHUSDT-trades-2025-02-01.zip
            # 2. spot/monthly/trades/ETHUSDT/ETHUSDT-trades-2025-02.zip
            assert len(tasks_passed) == 2
            daily_task = next(t for t in tasks_passed if "daily" in t.url)
            monthly_task = next(t for t in tasks_passed if "monthly" in t.url)

            assert daily_task.symbol == "ETHUSDT"
            assert daily_task.data_type == DataType.TRADES
            assert daily_task.date == "2025-02-01"
            assert str(self.temp_dir) in str(daily_task.output_path)

            assert monthly_task.symbol == "ETHUSDT"
            assert monthly_task.data_type == DataType.TRADES
            assert monthly_task.date == "2025-02" # Monthly date format
            assert str(self.temp_dir) in str(monthly_task.output_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
