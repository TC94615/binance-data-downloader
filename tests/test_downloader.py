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
            )

            mock_get_all_symbols.assert_called_once_with(DataMarket.SPOT, mock_get_all_symbols.call_args[0][1])
            # Ensure an error/warning is logged
            assert "No symbols found for any of the selected markets. Exiting." in caplog.text
            # download_tasks should not be called if no symbols are processed
            mock_download_tasks.assert_not_called()
