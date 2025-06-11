import asyncio
import hashlib
import tempfile
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import logging # Import logging
import pytest
import aiohttp
from aioresponses import aioresponses

from src.binance_downloader.core import (
    DataMarket,
    DataType,
    Frequency,
    DownloadTask,
)
from src.binance_downloader.downloader import (
    ChecksumValidator,
    FileDownloader,
    BinanceDataDownloader,
)


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
                download_interval_type="both", # Added to match new required parameter
            )

            # Verify download_tasks was called
            mock_download.assert_called_once()

            # Get the tasks that were passed
            called_tasks = mock_download.call_args[0][0]

            # Should have tasks for both daily and monthly (if available for spot)
            # This assertion needs to be more specific based on MARKET_DATATYPE_CONFIG
            # For SPOT, KLINE, daily data from 2025-01-01 to 2025-01-02,
            # it generates:
            # 1. spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2025-01-01.zip
            # 2. spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2025-01-02.zip
            # 3. spot/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2025-01.zip (from the monthly period processing)
            assert len(called_tasks) == 3
            assert all(isinstance(task, DownloadTask) for task in called_tasks)
            # Check some properties of the generated tasks to ensure they are as expected
            daily_tasks = [t for t in called_tasks if "daily" in t.url and t.date in ["2025-01-01", "2025-01-02"]]
            monthly_tasks = [t for t in called_tasks if "monthly" in t.url and t.date == "2025-01"]
            assert len(daily_tasks) == 2
            assert len(monthly_tasks) == 1
            assert daily_tasks[0].frequency == Frequency.ONE_DAY
            assert monthly_tasks[0].frequency == Frequency.ONE_DAY # Current behavior uses specified freq for monthly too


    @pytest.mark.asyncio
    @patch("src.binance_downloader.downloader.get_all_symbols")
    async def test_download_data_fetch_all_symbols_success(self, mock_get_all_symbols):
        """Test download_data when no symbols are provided and fetching is successful."""
        # Mock get_all_symbols to return specific symbols for SPOT
        mock_get_all_symbols.return_value = ["BTCUSDT", "ETHUSDT"]

        # Mock the file downloader to prevent actual downloads
        with patch.object(self.downloader.file_downloader, "download_tasks") as mock_download_tasks:
            mock_download_tasks.return_value = {} # No actual downloads needed for this test focus

            await self.downloader.download_data(
                markets=[DataMarket.SPOT],
                data_types=[DataType.TRADES], # Using a non-kline type for simplicity
                symbols=None, # Explicitly None to trigger fetching
                frequencies=[],
                start_date="2024-01-01",
                end_date="2024-01-01",
                download_interval_type='both', # Added argument
            )

            # Verify get_all_symbols was called once for SPOT market
            mock_get_all_symbols.assert_called_once_with(DataMarket.SPOT, mock_get_all_symbols.call_args[0][1]) # Second arg is the session

            # Verify that download_tasks was called (or tasks were generated with fetched symbols)
            # We expect tasks for BTCUSDT and ETHUSDT for SPOT, TRADES, daily and monthly
            # Daily: 2024-01-01 (BTCUSDT, ETHUSDT) -> 2 tasks
            # Monthly: 2024-01 (BTCUSDT, ETHUSDT) -> 2 tasks
            # Total = 4 tasks
            mock_download_tasks.assert_called_once()
            tasks_passed_to_downloader = mock_download_tasks.call_args[0][0]
            assert len(tasks_passed_to_downloader) == 4

            symbols_in_tasks = {task.symbol for task in tasks_passed_to_downloader}
            assert symbols_in_tasks == {"BTCUSDT", "ETHUSDT"}


    @pytest.mark.asyncio
    @patch("src.binance_downloader.downloader.get_all_symbols")
    async def test_download_data_with_user_provided_symbols(self, mock_get_all_symbols):
        """Test download_data when user provides symbols; get_all_symbols should not be called."""
        user_symbols = ["ADAUSDT", "XRPUSDT"]

        with patch.object(self.downloader.file_downloader, "download_tasks") as mock_download_tasks:
            mock_download_tasks.return_value = {}

            await self.downloader.download_data(
                markets=[DataMarket.SPOT],
                data_types=[DataType.TRADES],
                symbols=user_symbols,
                frequencies=[],
                start_date="2024-01-01",
                end_date="2024-01-01",
                download_interval_type='both', # Added argument
            )

            mock_get_all_symbols.assert_not_called()

            mock_download_tasks.assert_called_once()
            tasks_passed_to_downloader = mock_download_tasks.call_args[0][0]
            # Expected: ADAUSDT (daily, monthly), XRPUSDT (daily, monthly) -> 4 tasks
            assert len(tasks_passed_to_downloader) == 4
            symbols_in_tasks = {task.symbol for task in tasks_passed_to_downloader}
            assert symbols_in_tasks == set(user_symbols)


    @pytest.mark.asyncio
    @patch("src.binance_downloader.downloader.get_all_symbols")
    async def test_download_data_fetch_all_symbols_returns_empty(self, mock_get_all_symbols, caplog):
        """Test download_data when no symbols are provided and fetching returns no symbols."""
        mock_get_all_symbols.return_value = [] # Simulate no symbols found

        with patch.object(self.downloader.file_downloader, "download_tasks") as mock_download_tasks:
            await self.downloader.download_data(
                markets=[DataMarket.SPOT],
                data_types=[DataType.TRADES],
                symbols=None,
                frequencies=[],
                start_date="2024-01-01",
                end_date="2024-01-01",
                download_interval_type='both', # Added argument
            )

            mock_get_all_symbols.assert_called_once_with(DataMarket.SPOT, mock_get_all_symbols.call_args[0][1])
            # Ensure an error/warning is logged
            assert "No symbols found for any of the selected markets. Exiting." in caplog.text
            # download_tasks should not be called if no symbols are processed
            mock_download_tasks.assert_not_called()


    @pytest.mark.asyncio
    async def test_download_data_skip_existing_files(self, caplog): # Added caplog fixture
        caplog.set_level(logging.DEBUG) # Configure caplog to capture DEBUG level messages
        # 1. Setup:
        #    - Use self.downloader (already initialized in setup_method with self.temp_dir)
        #    - Define parameters for download_data that would generate a known set of files
        market = DataMarket.SPOT
        data_type = DataType.KLINES
        symbol = "BTCUSDT"
        frequency = Frequency.ONE_DAY
        start_date = "2023-01-01" # A single day for simplicity
        # end_date will be varied for test cases

        #      File 1: CSV exists (daily)
        date_csv_exists = "2023-01-01"
        # Construct path similar to _create_download_task: market/period/data_type/symbol/frequency/...
        # The actual file name is symbol-frequency-date.csv
        # The task.output_path will be .../symbol-frequency-date.zip
        # The check is temp_task.output_path.with_suffix(".csv")
        # So, we create self.temp_dir / market / "daily" / data_type / symbol / frequency / filename.csv
        csv_file_name_for_skip = f"{symbol}-{frequency.value}-{date_csv_exists}.csv"
        path_for_csv_skip = self.temp_dir / market.value / "daily" / data_type.value / symbol / frequency.value / csv_file_name_for_skip
        path_for_csv_skip.parent.mkdir(parents=True, exist_ok=True)
        path_for_csv_skip.touch() # Create empty CSV file

        #      File 2: Feather exists (daily, different date)
        date_feather_exists = "2023-01-02"
        feather_file_name_for_skip = f"{symbol}-{frequency.value}-{date_feather_exists}.feather"
        path_for_feather_skip = self.temp_dir / market.value / "daily" / data_type.value / symbol / frequency.value / feather_file_name_for_skip
        path_for_feather_skip.parent.mkdir(parents=True, exist_ok=True)
        path_for_feather_skip.touch() # Create empty feather file

        #      File 3: Neither CSV nor Feather exists (daily, different date)
        #      This file *should* be part of the download tasks.
        date_for_download = "2023-01-03"

        # 2. Action:
        #    - Patch `self.downloader.file_downloader.download_tasks` to monitor its calls.
        with patch.object(self.downloader.file_downloader, "download_tasks", new_callable=AsyncMock) as mock_file_download_tasks:
            mock_file_download_tasks.return_value = {} # Simulate successful download

            await self.downloader.download_data(
                markets=[market],
                data_types=[data_type],
                symbols=[symbol],
                frequencies=[frequency],
                start_date=date_csv_exists, # Covers 2023-01-01
                end_date=date_for_download,   # Covers 2023-01-01, 2023-01-02, 2023-01-03 for daily
                                             # and 2023-01 for monthly
                download_interval_type='both', # Added argument
            )

        # 3. Assertions:
        #    - Check that `mock_file_download_tasks` was called.
        mock_file_download_tasks.assert_called_once()

        tasks_passed = mock_file_download_tasks.call_args[0][0]

        # Construct expected .zip output paths for skipped files
        # These paths correspond to DownloadTask.output_path
        zip_path_for_skipped_csv = self.temp_dir / market.value / "daily" / data_type.value / symbol / frequency.value / f"{symbol}-{frequency.value}-{date_csv_exists}.zip"
        zip_path_for_skipped_feather = self.temp_dir / market.value / "daily" / data_type.value / symbol / frequency.value / f"{symbol}-{frequency.value}-{date_feather_exists}.zip"

        # Check that tasks for the pre-existing files were NOT generated
        found_task_for_existing_csv = any(
            task.output_path == zip_path_for_skipped_csv for task in tasks_passed
        )
        assert not found_task_for_existing_csv, f"Task for existing CSV ({zip_path_for_skipped_csv}) should have been skipped"

        found_task_for_existing_feather = any(
            task.output_path == zip_path_for_skipped_feather for task in tasks_passed
        )
        assert not found_task_for_existing_feather, f"Task for existing Feather ({zip_path_for_skipped_feather}) should have been skipped"

        # Check that the task for the 'to-be-downloaded' file WAS generated
        expected_zip_path_for_download = self.temp_dir / market.value / "daily" / data_type.value / symbol / frequency.value / f"{symbol}-{frequency.value}-{date_for_download}.zip"
        found_task_for_download = any(
            task.output_path == expected_zip_path_for_download for task in tasks_passed
        )
        assert found_task_for_download, f"Task for non-existing file ({expected_zip_path_for_download}) was not generated"

        # Check that the monthly task for '2023-01' was also generated
        # Monthly file name uses the specified frequency in its name: symbol-frequency-YYYY-MM.zip
        # Path: market/monthly/data_type/symbol/frequency/filename.zip
        expected_monthly_zip_path = self.temp_dir / market.value / "monthly" / data_type.value / symbol / frequency.value / f"{symbol}-{frequency.value}-2023-01.zip"
        found_monthly_task = any(
            task.output_path == expected_monthly_zip_path for task in tasks_passed
        )
        assert found_monthly_task, f"Monthly task for 2023-01 ({expected_monthly_zip_path}) should have been generated"

        #    - Verify log messages
        # The log message uses the .stem of the csv path, which is the filename without .csv
        # path_for_csv_skip.stem is 'BTCUSDT-1d-2023-01-01'
        assert f"Skipping existing file (CSV or Feather): {path_for_csv_skip.stem}" in caplog.text
        assert f"Skipping existing file (CSV or Feather): {path_for_feather_skip.stem}" in caplog.text

        # Expected tasks: 1 daily (2023-01-03) + 1 monthly (2023-01)
        assert len(tasks_passed) == 2, f"Expected 2 tasks, but got {len(tasks_passed)}. Tasks: {[t.output_path for t in tasks_passed]}"


class TestBinanceDataDownloaderWithDownloadPeriod:
    """Test cases for BinanceDataDownloader focusing on download_period."""

    def setup_method(self):
        """Setup test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        # Ensure constructor accepts to_feather and delete_csv if those are class attributes
        # For these tests, we are mocking download_tasks, so actual FileDownloader details are less critical
        self.downloader = BinanceDataDownloader(self.temp_dir, to_feather=False, delete_csv=False)
        # Mock the actual file downloading part
        self.mock_file_downloader_patch = patch.object(
            self.downloader, "file_downloader", new_callable=AsyncMock
        )
        self.mock_file_downloader = self.mock_file_downloader_patch.start()
        self.mock_file_downloader.download_tasks = AsyncMock(return_value={})

        # Mock get_all_symbols to prevent actual API calls if symbols=None
        self.mock_get_all_symbols_patch = patch("src.binance_downloader.downloader.get_all_symbols", new_callable=AsyncMock)
        self.mock_get_all_symbols = self.mock_get_all_symbols_patch.start()
        self.mock_get_all_symbols.return_value = ["BTCUSDT"] # Default mock

    def teardown_method(self):
        """Clean up test fixtures."""
        self.mock_file_downloader_patch.stop()
        self.mock_get_all_symbols_patch.stop()
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def _run_download_data_and_get_tasks(self, download_interval_type_value: str, symbols: list = ["BTCUSDT"]):
        """Helper to run download_data and return the tasks passed to file_downloader."""
        await self.downloader.download_data(
            markets=[DataMarket.SPOT],
            data_types=[DataType.KLINES, DataType.TRADES], # Include a kline and non-kline type
            symbols=symbols,
            frequencies=[Frequency.ONE_DAY],
            start_date="2024-01-01",
            end_date="2024-01-01", # Single day for simplicity
            download_interval_type=download_interval_type_value,
        )
        if self.mock_file_downloader.download_tasks.call_args:
            return self.mock_file_downloader.download_tasks.call_args[0][0]
        return []

    @pytest.mark.asyncio
    async def test_download_data_daily_only(self):
        """Asserts that only daily data tasks are created when download_interval_type is 'daily'."""
        tasks = await self._run_download_data_and_get_tasks(download_interval_type_value="daily")

        assert tasks, "No tasks were generated"
        # For SPOT, KLINE (1d) and TRADES, 2024-01-01, BTCUSDT:
        # Expecting:
        # 1. spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2024-01-01.zip
        # 2. spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-01.zip
        assert len(tasks) == 2
        for task in tasks:
            assert "daily" in task.url, f"Task URL {task.url} should contain 'daily'"
            assert "monthly" not in task.url, f"Task URL {task.url} should not contain 'monthly'"
            assert task.output_path.parent.name == "BTCUSDT" or task.output_path.parent.name == Frequency.ONE_DAY.value # klines have freq in path

    @pytest.mark.asyncio
    async def test_download_data_monthly_only(self):
        """Asserts that only monthly data tasks are created when download_interval_type is 'monthly'."""
        tasks = await self._run_download_data_and_get_tasks(download_interval_type_value="monthly")

        assert tasks, "No tasks were generated"
        # For SPOT, KLINE (1d) and TRADES, 2024-01 (from 2024-01-01), BTCUSDT:
        # Expecting:
        # 1. spot/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2024-01.zip
        # 2. spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2024-01.zip
        assert len(tasks) == 2
        for task in tasks:
            assert "monthly" in task.url, f"Task URL {task.url} should contain 'monthly'"
            assert "daily" not in task.url, f"Task URL {task.url} should not contain 'daily'"
            assert task.output_path.parent.name == "BTCUSDT" or task.output_path.parent.name == Frequency.ONE_DAY.value

    @pytest.mark.asyncio
    async def test_download_data_both(self):
        """Asserts that both daily and monthly data tasks are created when download_interval_type is 'both'."""
        tasks = await self._run_download_data_and_get_tasks(download_interval_type_value="both")

        assert tasks, "No tasks were generated"
        # For SPOT, KLINE (1d) and TRADES, 2024-01-01 (daily) & 2024-01 (monthly), BTCUSDT:
        # Daily:
        # 1. spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2024-01-01.zip
        # 2. spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-01.zip
        # Monthly:
        # 3. spot/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2024-01.zip
        # 4. spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2024-01.zip
        assert len(tasks) == 4
        daily_tasks_count = sum(1 for task in tasks if "daily" in task.url)
        monthly_tasks_count = sum(1 for task in tasks if "monthly" in task.url)
        assert daily_tasks_count == 2, "Expected 2 daily tasks"
        assert monthly_tasks_count == 2, "Expected 2 monthly tasks"

    @pytest.mark.asyncio
    async def test_download_data_daily_only_no_symbols_fetch(self):
        """Test daily only with symbol fetching."""
        self.mock_get_all_symbols.return_value = ["ETHUSDT"] # Mock fetched symbol
        tasks = await self._run_download_data_and_get_tasks(download_interval_type_value="daily", symbols=None)

        assert tasks, "No tasks were generated"
        assert len(tasks) == 2 # ETHUSDT daily klines, ETHUSDT daily trades
        for task in tasks:
            assert "daily" in task.url
            assert "ETHUSDT" in task.url
        self.mock_get_all_symbols.assert_called_once()

    @pytest.mark.asyncio
    async def test_download_data_monthly_only_no_symbols_fetch(self):
        """Test monthly only with symbol fetching."""
        self.mock_get_all_symbols.return_value = ["BNBUSDT"] # Mock fetched symbol
        tasks = await self._run_download_data_and_get_tasks(download_interval_type_value="monthly", symbols=None)

        assert tasks, "No tasks were generated"
        assert len(tasks) == 2 # BNBUSDT monthly klines, BNBUSDT monthly trades
        for task in tasks:
            assert "monthly" in task.url
            assert "BNBUSDT" in task.url
        self.mock_get_all_symbols.assert_called_once()

    @pytest.mark.asyncio
    async def test_download_data_both_no_symbols_fetch(self):
        """Test both daily and monthly with symbol fetching."""
        self.mock_get_all_symbols.return_value = ["SOLUSDT"] # Mock fetched symbol
        tasks = await self._run_download_data_and_get_tasks(download_interval_type_value="both", symbols=None)

        assert tasks, "No tasks were generated"
        assert len(tasks) == 4 # SOLUSDT daily (klines, trades), SOLUSDT monthly (klines, trades)
        for task in tasks:
            assert "SOLUSDT" in task.url
        daily_tasks_count = sum(1 for task in tasks if "daily" in task.url and "SOLUSDT" in task.url)
        monthly_tasks_count = sum(1 for task in tasks if "monthly" in task.url and "SOLUSDT" in task.url)
        assert daily_tasks_count == 2
        assert monthly_tasks_count == 2
        self.mock_get_all_symbols.assert_called_once()
