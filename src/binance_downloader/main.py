#!/usr/bin/env python3
"""
Binance Historical Data Downloader

A robust script for downloading historical data from Binance Data Vision portal
with checksum verification, concurrent downloads, and comprehensive error handling.
"""

import argparse
import asyncio
import hashlib
import logging
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import aiohttp
import aiofiles
from urllib.parse import urljoin


# Configuration and Data Structures
class DataMarket(Enum):
    SPOT = "spot"
    FUTURES_CM = "futures-cm"
    FUTURES_UM = "futures-um"
    OPTION = "option"


class DataType(Enum):
    AGG_TRADES = "aggTrades"
    BOOK_DEPTH = "bookDepth"
    BOOK_TICKER = "bookTicker"
    FUNDING_RATE = "fundingRate"
    INDEX_PRICE_KLINES = "indexPriceKlines"
    KLINES = "klines"
    LIQUIDATION_SNAPSHOT = "liquidationSnapshot"
    MARK_PRICE_KLINES = "markPriceKlines"
    METRICS = "metrics"
    PREMIUM_INDEX_KLINES = "premiumIndexKlines"
    TRADES = "trades"
    BVOL_INDEX = "BVOLIndex"
    EOH_SUMMARY = "EOHSummary"


class Frequency(Enum):
    ONE_SECOND = "1s"
    ONE_MINUTE = "1m"
    THREE_MINUTES = "3m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    TWO_HOURS = "2h"
    FOUR_HOURS = "4h"
    SIX_HOURS = "6h"
    EIGHT_HOURS = "8h"
    TWELVE_HOURS = "12h"
    ONE_DAY = "1d"
    THREE_DAYS = "3d"
    ONE_WEEK = "1w"
    ONE_MONTH = "1mo"


# Market-DataType mapping configuration
MARKET_DATATYPE_CONFIG = {
    DataMarket.OPTION: {"daily": {DataType.BVOL_INDEX, DataType.EOH_SUMMARY}},
    DataMarket.SPOT: {
        "daily": {DataType.AGG_TRADES, DataType.KLINES, DataType.TRADES},
        "monthly": {DataType.AGG_TRADES, DataType.KLINES, DataType.TRADES},
    },
    DataMarket.FUTURES_CM: {
        "daily": {
            DataType.AGG_TRADES,
            DataType.BOOK_DEPTH,
            DataType.BOOK_TICKER,
            DataType.INDEX_PRICE_KLINES,
            DataType.KLINES,
            DataType.LIQUIDATION_SNAPSHOT,
            DataType.MARK_PRICE_KLINES,
            DataType.METRICS,
            DataType.PREMIUM_INDEX_KLINES,
            DataType.TRADES,
        },
        "monthly": {
            DataType.AGG_TRADES,
            DataType.BOOK_TICKER,
            DataType.FUNDING_RATE,
            DataType.INDEX_PRICE_KLINES,
            DataType.KLINES,
            DataType.MARK_PRICE_KLINES,
            DataType.PREMIUM_INDEX_KLINES,
            DataType.TRADES,
        },
    },
    DataMarket.FUTURES_UM: {
        "daily": {
            DataType.AGG_TRADES,
            DataType.BOOK_DEPTH,
            DataType.BOOK_TICKER,
            DataType.INDEX_PRICE_KLINES,
            DataType.KLINES,
            DataType.LIQUIDATION_SNAPSHOT,
            DataType.MARK_PRICE_KLINES,
            DataType.METRICS,
            DataType.PREMIUM_INDEX_KLINES,
            DataType.TRADES,
        },
        "monthly": {
            DataType.AGG_TRADES,
            DataType.BOOK_TICKER,
            DataType.FUNDING_RATE,
            DataType.INDEX_PRICE_KLINES,
            DataType.KLINES,
            DataType.MARK_PRICE_KLINES,
            DataType.PREMIUM_INDEX_KLINES,
            DataType.TRADES,
        },
    },
}

# Kline-related data types (require frequency parameter)
KLINE_DATA_TYPES = {
    DataType.KLINES,
    DataType.INDEX_PRICE_KLINES,
    DataType.MARK_PRICE_KLINES,
    DataType.PREMIUM_INDEX_KLINES,
}


@dataclass
class DownloadTask:
    """Represents a single download task."""

    url: str
    symbol: str
    data_type: DataType
    date: str
    output_path: Path
    frequency: Optional[Frequency] = None


class BinanceURLBuilder:
    """Builds Binance Data Vision URLs for downloading files."""

    BASE_URL = "https://data.binance.vision/data/"

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def build_url(
        self,
        market: DataMarket,
        data_type: DataType,
        symbol: str,
        date: str,
        frequency: Optional[Frequency] = None,
        is_monthly: bool = False,
    ) -> str:
        """
        Build the complete URL for downloading a file.

        Args:
            market: The data market
            data_type: The data type
            symbol: The trading symbol
            date: Date string (YYYY-MM-DD for daily, YYYY-MM for monthly)
            frequency: Frequency for kline data types
            is_monthly: Whether this is monthly data

        Returns:
            Complete download URL
        """
        # Build path part 1 (market & type mapping)
        path_part1 = self._build_market_type_path(market, data_type, is_monthly)

        # Build path part 2 (symbol & file mapping)
        path_part2 = self._build_symbol_file_path(data_type, symbol, frequency)

        # Build filename
        filename = self._build_filename(symbol, data_type, date, frequency)

        # Combine all parts
        full_path = f"{path_part1}/{path_part2}/{filename}"
        url = urljoin(self.BASE_URL, full_path)

        self.logger.debug(f"Built URL: {url}")
        return url

    def _build_market_type_path(
        self, market: DataMarket, data_type: DataType, is_monthly: bool
    ) -> str:
        """Build the market and type part of the path."""
        period = "monthly" if is_monthly else "daily"

        if market == DataMarket.SPOT:
            return f"spot/{period}"
        elif market == DataMarket.FUTURES_CM:
            return f"futures/cm/{period}"
        elif market == DataMarket.FUTURES_UM:
            return f"futures/um/{period}"
        elif market == DataMarket.OPTION:
            return "option/daily"
        else:
            raise ValueError(f"Unsupported market: {market}")

    def _build_symbol_file_path(
        self, data_type: DataType, symbol: str, frequency: Optional[Frequency]
    ) -> str:
        """Build the symbol and file part of the path."""
        if data_type in KLINE_DATA_TYPES:
            if frequency is None:
                raise ValueError(f"Frequency required for data type: {data_type}")
            return f"{data_type.value}/{symbol}/{frequency.value}"
        else:
            return f"{data_type.value}/{symbol}"

    def _build_filename(
        self,
        symbol: str,
        data_type: DataType,
        date: str,
        frequency: Optional[Frequency],
    ) -> str:
        """Build the filename for the download."""
        if data_type in KLINE_DATA_TYPES:
            if frequency is None:
                raise ValueError(f"Frequency required for data type: {data_type}")
            return f"{symbol}-{frequency.value}-{date}.zip"
        else:
            return f"{symbol}-{data_type.value}-{date}.zip"


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

    def __init__(self, output_dir: Path, max_concurrent: int = 5):
        self.output_dir = output_dir
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
            with ThreadPoolExecutor() as executor:
                await loop.run_in_executor(
                    executor, self._extract_zip_sync, zip_path, extract_dir
                )

            # 解壓縮成功後，刪除 zip 檔案
            zip_path.unlink()
            self.logger.info(f"Extracted and deleted {zip_path.name}")

        except Exception as e:
            self.logger.error(f"Error processing zip file {zip_path}: {e}")

    def _extract_zip_sync(self, zip_path: Path, extract_dir: Path) -> None:
        """
        Synchronously extracts files from a zip archive to a specified directory,
        placing all contents directly in the target directory without preserving
        the internal folder structure of the zip file.
        """
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


class BinanceDataDownloader:
    """Main class orchestrating the download process."""

    def __init__(self, output_dir: Path = Path("./downloaded_data")):
        self.output_dir = output_dir
        self.url_builder = BinanceURLBuilder()
        self.file_downloader = FileDownloader(output_dir)
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
        symbols: List[str],
        frequencies: List[Frequency],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> None:
        """Download data based on specified parameters, skipping existing files."""
        tasks = []

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
                    for symbol in symbols:
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
                                    if final_csv_path.exists():
                                        self.logger.debug(
                                            f"Skipping existing file: {final_csv_path}"
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
                                if final_csv_path.exists():
                                    self.logger.debug(
                                        f"Skipping existing file: {final_csv_path}"
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


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("binance_downloader.log"),
        ],
    )


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Download historical data from Binance Data Vision"
    )

    parser.add_argument(
        "--data-markets",
        nargs="+",
        choices=[market.value for market in DataMarket],
        default=[market.value for market in DataMarket],
        help="Data markets to download",
    )

    parser.add_argument(
        "--data-types",
        nargs="+",
        choices=[dt.value for dt in DataType],
        default=[dt.value for dt in DataType],
        help="Data types to download",
    )

    parser.add_argument(
        "--frequencies",
        nargs="+",
        choices=[freq.value for freq in Frequency],
        default=[freq.value for freq in Frequency],
        help="Frequencies for kline data",
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT"],  # Default to BTC for demo
        help="Trading symbols to download",
    )

    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")

    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")

    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path("./downloaded_data"),
        help="Output directory for downloaded data",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    return parser.parse_args()


async def main():
    """Main entry point."""
    args = parse_arguments()
    setup_logging(args.log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting Binance Data Downloader")

    # Convert string arguments to enums
    markets = [DataMarket(market) for market in args.data_markets]
    data_types = [DataType(dt) for dt in args.data_types]
    frequencies = [Frequency(freq) for freq in args.frequencies]

    # Create downloader and start download
    downloader = BinanceDataDownloader(args.output_directory)

    try:
        await downloader.download_data(
            markets=markets,
            data_types=data_types,
            symbols=args.symbols,
            frequencies=frequencies,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        logger.info("Download process completed")
    except Exception as e:
        logger.error(f"Download process failed: {e}")
        raise


# --- 請在這裡新增這個新的同步函式 ---
def run():
    """Synchronous entry point that runs the async main function."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger = logging.getLogger(__name__)
        logger.info("Download process cancelled by user.")


# --- 新增結束 ---

if __name__ == "__main__":
    run()  # 現在這裡也可以直接呼叫 run()，保持一致性
