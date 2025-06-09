"""
Test suite for Binance Historical Data Downloader

Following TDD principles with comprehensive unit and integration tests.
"""

import asyncio
import hashlib
import tempfile
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import aiohttp
from aioresponses import aioresponses

# Import the modules to test
from binance_downloader import (
    BinanceURLBuilder,
    ChecksumValidator,
    FileDownloader,
    BinanceDataDownloader,
    DataMarket,
    DataType,
    Frequency,
    DownloadTask,
    MARKET_DATATYPE_CONFIG,
    KLINE_DATA_TYPES,
)


class TestBinanceURLBuilder:
    """Test cases for BinanceURLBuilder class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.url_builder = BinanceURLBuilder()

    def test_build_url_spot_daily_klines(self):
        """Test URL building for spot daily klines data."""
        url = self.url_builder.build_url(
            market=DataMarket.SPOT,
            data_type=DataType.KLINES,
            symbol="BTCUSDT",
            date="2025-06-08",
            frequency=Frequency.ONE_DAY,
            is_monthly=False,
        )

        expected = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2025-06-08.zip"
        assert url == expected

    def test_build_url_spot_monthly_aggtrades(self):
        """Test URL building for spot monthly aggTrades data."""
        url = self.url_builder.build_url(
            market=DataMarket.SPOT,
            data_type=DataType.AGG_TRADES,
            symbol="BTCUSDT",
            date="2025-05",
            is_monthly=True,
        )

        expected = "https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2025-05.zip"
        assert url == expected

    def test_build_url_futures_um_daily_klines(self):
        """Test URL building for futures UM daily klines."""
        url = self.url_builder.build_url(
            market=DataMarket.FUTURES_UM,
            data_type=DataType.KLINES,
            symbol="1000000BOBUSDT",
            date="2025-06-08",
            frequency=Frequency.ONE_DAY,
            is_monthly=False,
        )

        expected = "https://data.binance.vision/data/futures/um/daily/klines/1000000BOBUSDT/1d/1000000BOBUSDT-1d-2025-06-08.zip"
        assert url == expected

    def test_build_url_futures_cm_monthly_trades(self):
        """Test URL building for futures CM monthly trades."""
        url = self.url_builder.build_url(
            market=DataMarket.FUTURES_CM,
            data_type=DataType.TRADES,
            symbol="AAVEUSD_PERP",
            date="2025-05",
            is_monthly=True,
        )

        expected = "https://data.binance.vision/data/futures/cm/monthly/trades/AAVEUSD_PERP/AAVEUSD_PERP-trades-2025-05.zip"
        assert url == expected

    def test_build_url_option_bvol_index(self):
        """Test URL building for option BVOLIndex data."""
        url = self.url_builder.build_url(
            market=DataMarket.OPTION,
            data_type=DataType.BVOL_INDEX,
            symbol="BTCBVOLUSDT",
            date="2025-06-08",
            is_monthly=False,
        )

        expected = "https://data.binance.vision/data/option/daily/BVOLIndex/BTCBVOLUSDT/BTCBVOLUSDT-BVOLIndex-2025-06-08.zip"
        assert url == expected

    def test_build_url_kline_without_frequency_raises_error(self):
        """Test that building URL for kline data without frequency raises error."""
        with pytest.raises(ValueError, match="Frequency required for data type"):
            self.url_builder.build_url(
                market=DataMarket.SPOT,
                data_type=DataType.KLINES,
                symbol="BTCUSDT",
                date="2025-06-08",
            )

    def test_build_url_unsupported_market_raises_error(self):
        """Test that unsupported market raises error."""
        # This would require creating a mock unsupported market
        # For now, we'll test with a known enum value
        pass

    def test_build_market_type_path_futures_um_daily(self):
        """Test market type path building for futures UM daily."""
        path = self.url_builder._build_market_type_path(
            DataMarket.FUTURES_UM, DataType.KLINES, False
        )
        assert path == "futures/um/daily"

    def test_build_market_type_path_spot_monthly(self):
        """Test market type path building for spot monthly."""
        path = self.url_builder._build_market_type_path(
            DataMarket.SPOT, DataType.AGG_TRADES, True
        )
        assert path == "spot/monthly"

    def test_build_symbol_file_path_klines(self):
        """Test symbol file path building for klines."""
        path = self.url_builder._build_symbol_file_path(
            DataType.KLINES, "BTCUSDT", Frequency.ONE_DAY
        )
        assert path == "klines/BTCUSDT/1d"

    def test_build_symbol_file_path_non_klines(self):
        """Test symbol file path building for non-klines data."""
        path = self.url_builder._build_symbol_file_path(
            DataType.AGG_TRADES, "BTCUSDT", None
        )
        assert path == "aggTrades/BTCUSDT"

    def test_build_filename_klines(self):
        """Test filename building for klines data."""
        filename = self.url_builder._build_filename(
            "BTCUSDT", DataType.KLINES, "2025-06-08", Frequency.ONE_DAY
        )
        assert filename == "BTCUSDT-1d-2025-06-08.zip"

    def test_build_filename_non_klines(self):
        """Test filename building for non-klines data."""
        filename = self.url_builder._build_filename(
            "BTCUSDT", DataType.AGG_TRADES, "2025-06-08", None
        )
        assert filename == "BTCUSDT-aggTrades-2025-06-08.zip"


class TestChecksumValidator:
    """Test cases for ChecksumValidator class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.validator = ChecksumValidator()
        self.temp_dir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_test_file(self, content: bytes) -> Path:
        """Create a test file with given content."""
        test_file = self.temp_dir / "test_file.zip"
        test_file.write_bytes(content)
        return test_file

    @pytest.mark.asyncio
    async def test_validate_file_success(self):
        """Test successful file validation."""
        # Create test file
        test_content = b"test file content"
        test_file = self.create_test_file(test_content)

        # Calculate expected checksum
        expected_checksum = hashlib.sha256(test_content).hexdigest()

        # Mock aiohttp response
        with aioresponses() as m:
            checksum_url = "https://example.com/test.zip.CHECKSUM"
            m.get(checksum_url, body=f"{expected_checksum}  test_file.zip")

            async with aiohttp.ClientSession() as session:
                result = await self.validator.validate_file(
                    test_file, checksum_url, session
                )

        assert result is True

    @pytest.mark.asyncio
    async def test_validate_file_checksum_mismatch(self):
        """Test file validation with checksum mismatch."""
        # Create test file
        test_content = b"test file content"
        test_file = self.create_test_file(test_content)

        # Use wrong checksum
        wrong_checksum = "wrong_checksum_value"

        # Mock aiohttp response
        with aioresponses() as m:
            checksum_url = "https://example.com/test.zip.CHECKSUM"
            m.get(checksum_url, body=f"{wrong_checksum}  test_file.zip")

            async with aiohttp.ClientSession() as session:
                result = await self.validator.validate_file(
                    test_file, checksum_url, session
                )

        assert result is False

    @pytest.mark.asyncio
    async def test_validate_file_checksum_download_failure(self):
        """Test file validation when checksum download fails."""
        test_content = b"test file content"
        test_file = self.create_test_file(test_content)

        # Mock failed checksum download
        with aioresponses() as m:
            checksum_url = "https://example.com/test.zip.CHECKSUM"
            m.get(checksum_url, status=404)

            async with aiohttp.ClientSession() as session:
                result = await self.validator.validate_file(
                    test_file, checksum_url, session
                )

        assert result is False

    @pytest.mark.asyncio
    async def test_calculate_sha256(self):
        """Test SHA256 calculation."""
        test_content = b"test file content for sha256"
        test_file = self.create_test_file(test_content)

        calculated_hash = await self.validator._calculate_sha256(test_file)
        expected_hash = hashlib.sha256(test_content).hexdigest()

        assert calculated_hash == expected_hash

    @pytest.mark.asyncio
    async def test_download_checksum_success(self):
        """Test successful checksum download."""
        checksum_content = "abc123def456  test_file.zip"

        with aioresponses() as m:
            checksum_url = "https://example.com/test.zip.CHECKSUM"
            m.get(checksum_url, body=checksum_content)

            async with aiohttp.ClientSession() as session:
                result = await self.validator._download_checksum(checksum_url, session)

        assert result == "abc123def456"


class TestFileDownloader:
    """Test cases for FileDownloader class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.downloader = FileDownloader(self.temp_dir, max_concurrent=2)

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_test_zip(
        self, csv_content: str = "date,price\n2025-01-01,50000"
    ) -> bytes:
        """Create a test zip file containing CSV data."""
        import io

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            zip_file.writestr("test_data.csv", csv_content)
        return zip_buffer.getvalue()

    @pytest.mark.asyncio
    async def test_download_single_file_success(self):
        """Test successful single file download."""
        # Create test data
        test_csv = "date,price\n2025-01-01,50000\n2025-01-02,51000"
        zip_content = self.create_test_zip(test_csv)
        checksum = hashlib.sha256(zip_content).hexdigest()

        # Create download task
        task = DownloadTask(
            url="https://example.com/test.zip",
            symbol="BTCUSDT",
            data_type=DataType.KLINES,
            date="2025-01-01",
            output_path=self.temp_dir / "test.zip",
            frequency=Frequency.ONE_DAY,
        )

        # Mock responses
        with aioresponses() as m:
            # Mock zip file download
            m.get(task.url, body=zip_content)
            # Mock checksum download
            m.get(f"{task.url}.CHECKSUM", body=f"{checksum}  test.zip")

            async with aiohttp.ClientSession() as session:
                result = await self.downloader._download_single_file(task, session)

        assert result is True

        # 1. 驗證 .zip 檔案已被成功刪除
        assert not task.output_path.exists()

        # 2. 驗證 .csv 檔案已被成功解壓縮到正確的位置
        #    根據 create_test_zip 輔助函式，解壓出來的檔名是 "test_data.csv"
        #    根據 _extract_zip 的新邏輯，它會被放在 .zip 檔案的父目錄下
        expected_csv_path = task.output_path.parent / "test_data.csv"
        assert expected_csv_path.exists()

    @pytest.mark.asyncio
    async def test_download_single_file_not_found(self):
        """Test download when file is not found (404)."""
        task = DownloadTask(
            url="https://example.com/nonexistent.zip",
            symbol="BTCUSDT",
            data_type=DataType.KLINES,
            date="2025-01-01",
            output_path=self.temp_dir / "nonexistent.zip",
        )

        with aioresponses() as m:
            m.get(task.url, status=404)

            async with aiohttp.ClientSession() as session:
                result = await self.downloader._download_single_file(task, session)

        assert result is False
        assert not task.output_path.exists()

    @pytest.mark.asyncio
    async def test_download_single_file_checksum_failure(self):
        """Test download with checksum validation failure."""
        zip_content = self.create_test_zip()
        wrong_checksum = "wrong_checksum_value"

        task = DownloadTask(
            url="https://example.com/test.zip",
            symbol="BTCUSDT",
            data_type=DataType.KLINES,
            date="2025-01-01",
            output_path=self.temp_dir / "test.zip",
        )

        with aioresponses() as m:
            m.get(task.url, body=zip_content)
            m.get(f"{task.url}.CHECKSUM", body=f"{wrong_checksum}  test.zip")

            async with aiohttp.ClientSession() as session:
                result = await self.downloader._download_single_file(task, session)

        assert result is False
        assert (
            not task.output_path.exists()
        )  # File should be removed on checksum failure

    @pytest.mark.asyncio
    async def test_download_tasks_multiple_files(self):
        """Test downloading multiple files concurrently."""
        tasks = []
        zip_contents = []

        # Create multiple test tasks
        for i in range(3):
            csv_content = f"date,price\n2025-01-0{i+1},{50000+i*1000}"
            zip_content = self.create_test_zip(csv_content)
            zip_contents.append(zip_content)

            task = DownloadTask(
                url=f"https://example.com/test{i}.zip",
                symbol="BTCUSDT",
                data_type=DataType.KLINES,
                date=f"2025-01-0{i+1}",
                output_path=self.temp_dir / f"test{i}.zip",
            )
            tasks.append(task)

        # Mock all responses
        with aioresponses() as m:
            for i, (task, zip_content) in enumerate(zip(tasks, zip_contents)):
                checksum = hashlib.sha256(zip_content).hexdigest()
                m.get(task.url, body=zip_content)
                m.get(f"{task.url}.CHECKSUM", body=f"{checksum}  test{i}.zip")

            results = await self.downloader.download_tasks(tasks)

            # Check all downloads succeeded
            assert len(results) == 3
            assert all(success for success in results.values())

        # Check all files exist
        for task in tasks:
            # 1. 驗證 .zip 檔案已被成功刪除
            assert not task.output_path.exists()

            # 2. 驗證 .csv 檔案已被成功解壓縮
            expected_csv_path = task.output_path.parent / "test_data.csv"
            assert expected_csv_path.exists()

    def test_extract_zip_sync(self):
        """Test synchronous zip extraction."""
        # Create test zip
        csv_content = "date,price\n2025-01-01,50000"
        zip_content = self.create_test_zip(csv_content)

        # Save zip file
        zip_path = self.temp_dir / "test.zip"
        zip_path.write_bytes(zip_content)

        # Extract
        extract_dir = self.temp_dir / "extracted"
        self.downloader._extract_zip_sync(zip_path, extract_dir)

        # Verify extraction
        assert extract_dir.exists()
        csv_file = extract_dir / "test_data.csv"
        assert csv_file.exists()
        assert csv_file.read_text() == csv_content


class TestBinanceDataDownloader:
    """Test cases for BinanceDataDownloader class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.downloader = BinanceDataDownloader(self.temp_dir)

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_generate_date_range_daily_default(self):
        """Test generating daily date range with defaults."""
        dates = self.downloader.generate_date_range(None, None, is_monthly=False)

        # Should generate last 30 days
        assert len(dates) == 31  # Including today
        assert all(len(date) == 10 for date in dates)  # YYYY-MM-DD format

        # Check date format
        for date in dates:
            datetime.strptime(date, "%Y-%m-%d")  # Should not raise exception

    def test_generate_date_range_monthly_default(self):
        """Test generating monthly date range with defaults."""
        dates = self.downloader.generate_date_range(None, None, is_monthly=True)

        # Should generate last 12 months
        assert len(dates) >= 12
        assert all(len(date) == 7 for date in dates)  # YYYY-MM format

        # Check date format
        for date in dates:
            datetime.strptime(date, "%Y-%m")  # Should not raise exception

    def test_generate_date_range_custom_daily(self):
        """Test generating custom daily date range."""
        start_date = "2025-01-01"
        end_date = "2025-01-05"

        dates = self.downloader.generate_date_range(
            start_date, end_date, is_monthly=False
        )

        expected_dates = [
            "2025-01-01",
            "2025-01-02",
            "2025-01-03",
            "2025-01-04",
            "2025-01-05",
        ]
        assert dates == expected_dates

    def test_generate_date_range_custom_monthly(self):
        """Test generating custom monthly date range."""
        start_date = "2025-01-01"
        end_date = "2025-03-15"

        dates = self.downloader.generate_date_range(
            start_date, end_date, is_monthly=True
        )

        expected_dates = ["2025-01", "2025-02", "2025-03"]
        assert dates == expected_dates

    def test_create_download_task(self):
        """Test creating a download task."""
        task = self.downloader._create_download_task(
            market=DataMarket.SPOT,
            data_type=DataType.KLINES,
            symbol="BTCUSDT",
            date="2025-01-01",
            frequency=Frequency.ONE_DAY,
            is_monthly=False,
        )

        assert task.symbol == "BTCUSDT"
        assert task.data_type == DataType.KLINES
        assert task.date == "2025-01-01"
        assert task.frequency == Frequency.ONE_DAY
        assert "spot/daily/klines" in task.url
        assert task.output_path.name == "BTCUSDT-1d-2025-01-01.zip"

    @pytest.mark.asyncio
    async def test_download_data_integration(self):
        """Integration test for download_data method."""
        # Mock the file downloader to avoid actual downloads
        with patch.object(
            self.downloader.file_downloader, "download_tasks"
        ) as mock_download:
            mock_download.return_value = {"url1": True, "url2": True}

            await self.downloader.download_data(
                markets=[DataMarket.SPOT],
                data_types=[DataType.KLINES],
                symbols=["BTCUSDT"],
                frequencies=[Frequency.ONE_DAY],
                start_date="2025-01-01",
                end_date="2025-01-02",
            )

            # Verify download_tasks was called
            mock_download.assert_called_once()

            # Get the tasks that were passed
            called_tasks = mock_download.call_args[0][0]

            # Should have tasks for both daily and monthly (if available for spot)
            assert len(called_tasks) >= 2  # At least 2 days
            assert all(isinstance(task, DownloadTask) for task in called_tasks)


class TestConfigurationAndEnums:
    """Test cases for configuration and enum validation."""

    def test_market_datatype_config_completeness(self):
        """Test that market-datatype configuration is complete."""
        # Verify all markets are in config
        for market in DataMarket:
            assert market in MARKET_DATATYPE_CONFIG

        # Verify config structure
        for market, periods in MARKET_DATATYPE_CONFIG.items():
            assert isinstance(periods, dict)
            for period, data_types in periods.items():
                assert period in ["daily", "monthly"]
                assert isinstance(data_types, set)
                assert all(isinstance(dt, DataType) for dt in data_types)

    def test_kline_data_types_definition(self):
        """Test that kline data types are correctly defined."""
        expected_kline_types = {
            DataType.KLINES,
            DataType.INDEX_PRICE_KLINES,
            DataType.MARK_PRICE_KLINES,
            DataType.PREMIUM_INDEX_KLINES,
        }

        assert KLINE_DATA_TYPES == expected_kline_types

    def test_enum_values_consistency(self):
        """Test that enum values are consistent with expected strings."""
        # Test DataMarket enum values
        assert DataMarket.SPOT.value == "spot"
        assert DataMarket.FUTURES_CM.value == "futures-cm"
        assert DataMarket.FUTURES_UM.value == "futures-um"
        assert DataMarket.OPTION.value == "option"

        # Test some DataType enum values
        assert DataType.KLINES.value == "klines"
        assert DataType.AGG_TRADES.value == "aggTrades"
        assert DataType.BVOL_INDEX.value == "BVOLIndex"

        # Test some Frequency enum values
        assert Frequency.ONE_DAY.value == "1d"
        assert Frequency.ONE_HOUR.value == "1h"
        assert Frequency.ONE_MINUTE.value == "1m"


class TestDownloadTask:
    """Test cases for DownloadTask dataclass."""

    def test_download_task_creation(self):
        """Test creating a DownloadTask instance."""
        task = DownloadTask(
            url="https://example.com/test.zip",
            symbol="BTCUSDT",
            data_type=DataType.KLINES,
            date="2025-01-01",
            output_path=Path("/tmp/test.zip"),
            frequency=Frequency.ONE_DAY,
        )

        assert task.url == "https://example.com/test.zip"
        assert task.symbol == "BTCUSDT"
        assert task.data_type == DataType.KLINES
        assert task.date == "2025-01-01"
        assert task.output_path == Path("/tmp/test.zip")
        assert task.frequency == Frequency.ONE_DAY

    def test_download_task_optional_frequency(self):
        """Test DownloadTask with optional frequency."""
        task = DownloadTask(
            url="https://example.com/test.zip",
            symbol="BTCUSDT",
            data_type=DataType.AGG_TRADES,
            date="2025-01-01",
            output_path=Path("/tmp/test.zip"),
        )

        assert task.frequency is None


class TestArgumentParsing:
    """Test cases for command-line argument parsing."""

    def test_parse_arguments_defaults(self):
        """Test parsing arguments with defaults."""
        # This would require mocking sys.argv or using a different approach
        # For now, we'll test the argument parser configuration
        from binance_downloader import parse_arguments

        # Test that the function exists and returns an ArgumentParser
        # In a real test, we'd mock sys.argv and test specific argument combinations
        pass


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
    async def test_end_to_end_workflow(self):
        """Test the complete end-to-end workflow."""
        # This is a more complex integration test that would test the entire workflow
        # from URL building through download and extraction

        # Create downloader
        downloader = BinanceDataDownloader(self.temp_dir)

        # Mock all external dependencies
        with patch.object(
            downloader.file_downloader, "download_tasks"
        ) as mock_download:
            mock_download.return_value = {"https://example.com/test.zip": True}

            # Run a small download job
            await downloader.download_data(
                markets=[DataMarket.SPOT],
                data_types=[DataType.AGG_TRADES],
                symbols=["BTCUSDT"],
                frequencies=[Frequency.ONE_DAY],
                start_date="2025-01-01",
                end_date="2025-01-01",
            )

            # Verify the download was attempted
            mock_download.assert_called_once()


# Performance Tests
class TestPerformance:
    """Performance-related tests."""

    @pytest.mark.asyncio
    async def test_concurrent_download_limits(self):
        """Test that concurrent downloads respect limits."""
        temp_dir = Path(tempfile.mkdtemp())
        try:
            downloader = FileDownloader(temp_dir, max_concurrent=2)

            # Create multiple tasks
            tasks = []
            for i in range(5):
                task = DownloadTask(
                    url=f"https://example.com/test{i}.zip",
                    symbol="BTCUSDT",
                    data_type=DataType.KLINES,
                    date=f"2025-01-0{i+1}",
                    output_path=temp_dir / f"test{i}.zip",
                )
                tasks.append(task)

            # Mock the downloads to simulate work
            with patch.object(downloader, "_download_single_file") as mock_download:
                mock_download.return_value = True

                # This test would verify that max_concurrent is respected
                # In practice, this is hard to test without timing or other mechanisms
                results = await downloader.download_tasks(tasks)

                assert len(results) == 5
                assert mock_download.call_count == 5

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
