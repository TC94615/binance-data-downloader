import asyncio
import hashlib
import logging
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

import aiofiles
import aiohttp

from .core import (
    KLINE_DATA_TYPES,
    MARKET_DATATYPE_CONFIG,
    DataMarket,
    DataType,
    DownloadTask,
    Frequency,
)
from .utils import convert_csv_to_feather # Added import
from .url_builder import BinanceURLBuilder
from .symbol_fetcher import get_all_symbols


class ChecksumValidator:
    """Validates file checksums against CHECKSUM files."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def validate_file(
        self, file_path: Path, checksum_url: str, session: aiohttp.ClientSession
    ) -> bool:
        """
        Validate a file against its checksum.

        Args:
            file_path: Path to the file to validate
            checksum_url: URL of the checksum file
            session: aiohttp session for downloading checksum

        Returns:
            True if checksum is valid, False otherwise
        """
        try:
            # Download checksum file
            expected_checksum = await self._download_checksum(checksum_url, session)
            if not expected_checksum:
                self.logger.error(f"Failed to download checksum from {checksum_url}")
                return False

            # Calculate actual checksum
            actual_checksum = await self._calculate_sha256(file_path)

            # Compare checksums
            is_valid = actual_checksum.lower() == expected_checksum.lower()

            if is_valid:
                self.logger.info(f"Checksum validation passed for {file_path.name}")
            else:
                self.logger.error(f"Checksum validation failed for {file_path.name}")
                self.logger.error(f"Expected: {expected_checksum}")
                self.logger.error(f"Actual: {actual_checksum}")

            return is_valid

        except Exception as e:
            self.logger.error(f"Error validating checksum for {file_path}: {e}")
            return False

    async def _download_checksum(
        self, checksum_url: str, session: aiohttp.ClientSession
    ) -> Optional[str]:
        """Download and parse checksum file."""
        try:
            async with session.get(checksum_url) as response:
                if response.status == 200:
                    content = await response.text()
                    # Checksum files typically contain "checksum filename"
                    return content.split()[0].strip()
                else:
                    self.logger.warning(
                        f"Failed to download checksum: {response.status}"
                    )
                    return None
        except Exception as e:
            self.logger.error(f"Error downloading checksum: {e}")
            return None

    async def _calculate_sha256(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()

        async with aiofiles.open(file_path, "rb") as f:
            while chunk := await f.read(8192):
                sha256_hash.update(chunk)

        return sha256_hash.hexdigest()


class FileDownloader:
    """Handles file downloads with concurrent processing."""

    def __init__(self, output_dir: Path, to_feather: bool = False, delete_csv: bool = False, max_concurrent: int = 5):
        self.output_dir = output_dir
        self.to_feather = to_feather
        self.delete_csv = delete_csv
        self.max_concurrent = max_concurrent
        self.logger = logging.getLogger(__name__)
        self.checksum_validator = ChecksumValidator()

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def download_tasks(self, tasks: List[DownloadTask]) -> Dict[str, bool]:
        """
        Download multiple files concurrently.

        Args:
            tasks: List of download tasks

        Returns:
            Dictionary mapping task URLs to success status
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)
        results: Dict[str, bool] = {}

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=300)
        ) as session:
            download_coroutines = [
                self._download_with_semaphore(task, session, semaphore)
                for task in tasks
            ]

            completed_tasks = await asyncio.gather(
                *download_coroutines, return_exceptions=True
            )

            for task, result in zip(tasks, completed_tasks):
                if isinstance(result, BaseException):
                    self.logger.error(
                        f"Task {task.url} failed with exception: {result}"
                    )
                    results[task.url] = False
                else:
                    results[task.url] = result

        return results

    async def _download_with_semaphore(
        self,
        task: DownloadTask,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
    ) -> bool:
        """Download a single file with semaphore control."""
        async with semaphore:
            return await self._download_single_file(task, session)

    async def _download_single_file(
        self, task: DownloadTask, session: aiohttp.ClientSession
    ) -> bool:
        """Download and validate a single file."""
        try:
            self.logger.info(f"Downloading {task.url}")

            # Download the zip file
            async with session.get(task.url) as response:
                if response.status == 404:
                    self.logger.warning(f"File not found: {task.url}")
                    return False
                elif response.status != 200:
                    self.logger.error(
                        f"Download failed with status {response.status}: {task.url}"
                    )
                    return False

                # Save file
                async with aiofiles.open(task.output_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)

            # Validate checksum
            checksum_url = f"{task.url}.CHECKSUM"
            is_valid = await self.checksum_validator.validate_file(
                task.output_path, checksum_url, session
            )

            if not is_valid:
                # Remove invalid file
                task.output_path.unlink(missing_ok=True)
                return False

            # Extract zip file
            await self._extract_zip(task.output_path)

            self.logger.info(f"Successfully processed {task.output_path.name}")
            return True

        except Exception as e:
            self.logger.error(f"Error downloading {task.url}: {e}")
            # Clean up partial file
            task.output_path.unlink(missing_ok=True)
            return False

    async def _extract_zip(self, zip_path: Path) -> None:
        """Extract a zip file and then delete it."""
        try:
            # 解壓縮的目的地現在是 zip 檔案所在的目錄，而不是新的子目錄
            extract_dir = zip_path.parent

            # Use thread pool for CPU-bound zip extraction
            loop = asyncio.get_event_loop()
            extracted_file_paths: List[Path] = []
            with ThreadPoolExecutor() as executor:
                extracted_file_paths = await loop.run_in_executor(
                    executor, self._extract_zip_sync, zip_path, extract_dir
                )

            # Convert to Feather if requested
            if self.to_feather and extracted_file_paths: # self.to_feather will be added later
                self.logger.debug(f"Attempting Feather conversion for files from {zip_path.name}")
                for file_path in extracted_file_paths:
                    if file_path.suffix == ".csv":
                        # self.delete_csv will be added later
                        convert_csv_to_feather(file_path, self.delete_csv)

            # 解壓縮成功後，刪除 zip 檔案
            zip_path.unlink()
            self.logger.info(f"Extracted and deleted {zip_path.name}")

        except Exception as e:
            self.logger.error(f"Error processing zip file {zip_path}: {e}")

    def _extract_zip_sync(self, zip_path: Path, extract_dir: Path) -> List[Path]:
        """
        Synchronously extracts files from a zip archive to a specified directory,
        placing all contents directly in the target directory without preserving
        the internal folder structure of the zip file.
        Returns a list of paths to the extracted files.
        """
        extracted_files: List[Path] = []
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            for member in zip_ref.infolist():
                # 跳過任何可能是目錄的成員
                if member.is_dir():
                    continue

                # 建立一個新的檔案名稱，只保留原始檔案名，去除所有路徑
                new_filename = Path(member.filename).name

                # 設定目標路徑
                target_path = extract_dir / new_filename

                # 確保目標目錄存在
                target_path.parent.mkdir(parents=True, exist_ok=True)

                # 讀取來源檔案內容並寫入新檔案
                with zip_ref.open(member, "r") as source_file:
                    with open(target_path, "wb") as target_file:
                        target_file.write(source_file.read())
                extracted_files.append(target_path)
        return extracted_files


class BinanceDataDownloader:
    """Main class orchestrating the download process."""

    def __init__(self, output_dir: Path = Path("./downloaded_data"), to_feather: bool = False, delete_csv: bool = False):
        self.output_dir = output_dir
        self.to_feather = to_feather
        self.delete_csv = delete_csv
        self.url_builder = BinanceURLBuilder()
        # This will require FileDownloader.__init__ to be updated
        self.file_downloader = FileDownloader(output_dir, self.to_feather, self.delete_csv)
        self.logger = logging.getLogger(__name__)

    def generate_date_range(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        is_monthly: bool = False,
    ) -> List[str]:
        """Generate list of date strings for the specified range."""
        if start_date is None and end_date is None:
            # Default to last 30 days for daily, last 12 months for monthly
            end = datetime.now()
            if is_monthly:
                start = end - timedelta(days=365)
                dates = []
                current = start.replace(day=1)
                while current <= end:
                    dates.append(current.strftime("%Y-%m"))
                    # Move to next month
                    if current.month == 12:
                        current = current.replace(year=current.year + 1, month=1)
                    else:
                        current = current.replace(month=current.month + 1)
                return dates
            else:
                start = end - timedelta(days=30)
                dates = []
                current = start
                while current <= end:
                    dates.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
                return dates

        # Parse provided dates
        start = (
            datetime.strptime(start_date, "%Y-%m-%d")
            if start_date
            else datetime.now() - timedelta(days=30)
        )
        end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()

        dates = []
        if is_monthly:
            current = start.replace(day=1)
            while current <= end:
                dates.append(current.strftime("%Y-%m"))
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)
        else:
            current = start
            while current <= end:
                dates.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)

        return dates

    async def download_data(
        self,
        markets: List[DataMarket],
        data_types: List[DataType],
        symbols: Optional[List[str]], # Can be None if fetching all
        frequencies: List[Frequency],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> None:
        """Download data based on specified parameters, skipping existing files."""

        current_symbols = symbols
        if not current_symbols: # If symbols list is None or empty, fetch all
            self.logger.info("No symbols provided, attempting to fetch all symbols for selected markets.")
            fetched_symbols_set: Set[str] = set()
            async with aiohttp.ClientSession() as session:
                for market_to_fetch in markets:
                    market_symbols = await get_all_symbols(market_to_fetch, session)
                    if market_symbols:
                        fetched_symbols_set.update(market_symbols)
                    else:
                        self.logger.warning(f"No symbols fetched for market {market_to_fetch.value}")

            if not fetched_symbols_set:
                self.logger.error("No symbols found for any of the selected markets. Exiting.")
                return
            current_symbols = list(fetched_symbols_set)
            self.logger.info(f"Proceeding with {len(current_symbols)} fetched symbols.")

        tasks = []
        # Ensure current_symbols is not None before iterating
        if current_symbols is None: # Should not happen if logic above is correct, but as a safeguard
            self.logger.error("Symbol list is unexpectedly None after fetch attempt. Exiting.")
            return

        for market in markets:
            market_config = MARKET_DATATYPE_CONFIG.get(market, {})

            for period, valid_data_types in market_config.items():
                is_monthly = period == "monthly"

                filtered_data_types = [
                    dt for dt in data_types if dt in valid_data_types
                ]

                if not filtered_data_types:
                    continue

                dates = self.generate_date_range(start_date, end_date, is_monthly)

                for data_type in filtered_data_types:
                    for symbol in current_symbols: # Use the potentially fetched list of symbols
                        if data_type in KLINE_DATA_TYPES:
                            for frequency in frequencies:
                                for date in dates:
                                    # --- 修改點 1 開始 ---
                                    # 預先建立一個 "假" 的任務來取得預期路徑
                                    temp_task = self._create_download_task(
                                        market,
                                        data_type,
                                        symbol,
                                        date,
                                        frequency,
                                        is_monthly,
                                    )

                                    # 檢查最終的 CSV 檔案是否已存在
                                    final_csv_path = temp_task.output_path.with_suffix(
                                        ".csv"
                                    )
                                    final_feather_path = temp_task.output_path.with_suffix(
                                        ".feather"
                                    )
                                    if final_csv_path.exists() or final_feather_path.exists():
                                        self.logger.debug(
                                            f"Skipping existing file (CSV or Feather): {final_csv_path.stem}"
                                        )
                                        continue  # 如果存在，就跳過這個迴圈，不建立任務

                                    # 如果不存在，才將這個任務加入列表
                                    tasks.append(temp_task)
                                    # --- 修改點 1 結束 ---
                        else:
                            for date in dates:
                                # --- 修改點 2 開始 ---
                                # 對非 K 線數據重複同樣的邏輯
                                temp_task = self._create_download_task(
                                    market, data_type, symbol, date, None, is_monthly
                                )

                                final_csv_path = temp_task.output_path.with_suffix(
                                    ".csv"
                                )
                                final_feather_path = temp_task.output_path.with_suffix(
                                    ".feather"
                                )
                                if final_csv_path.exists() or final_feather_path.exists():
                                    self.logger.debug(
                                        f"Skipping existing file (CSV or Feather): {final_csv_path.stem}"
                                    )
                                    continue

                                tasks.append(temp_task)
                                # --- 修改點 2 結束 ---

        if not tasks:
            # 這個日誌現在更有意義了，它可能包含了被跳過的檔案
            self.logger.warning("No new files to download.")
            return

        self.logger.info(f"Generated {len(tasks)} new download tasks")

        # ... 方法的其餘部分保持不變 ...

        # Execute downloads
        results = await self.file_downloader.download_tasks(tasks)

        # Summary
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        self.logger.info(f"Download completed: {successful}/{total} successful")

    def _create_download_task(
        self,
        market: DataMarket,
        data_type: DataType,
        symbol: str,
        date: str,
        frequency: Optional[Frequency],
        is_monthly: bool,
    ) -> DownloadTask:
        """Create a download task with a directory structure matching the download URL."""
        # 步驟 1: 建立完整的 URL (這部分不變)
        url = self.url_builder.build_url(
            market, data_type, symbol, date, frequency, is_monthly
        )

        # 步驟 2: 透過移除 BASE_URL 來取得 URL 中的相對路徑
        # 例如: "spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2025-06-10.zip"
        relative_path_str = url.replace(self.url_builder.BASE_URL, "")

        # 步驟 3: 將使用者指定的輸出目錄與我們取得的相對路徑結合起來
        output_path = self.output_dir / Path(relative_path_str)

        # 步驟 4: 確保目標資料夾存在
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 步驟 5: 使用這個新的、結構更完整的路徑來建立下載任務
        return DownloadTask(
            url=url,
            symbol=symbol,
            data_type=data_type,
            date=date,
            output_path=output_path,
            frequency=frequency,
        )
