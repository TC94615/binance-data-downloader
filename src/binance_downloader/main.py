#!/usr/bin/env python3
"""
Binance Historical Data Downloader

A robust script for downloading historical data from Binance Data Vision portal
with checksum verification, concurrent downloads, and comprehensive error handling.
"""

import asyncio
import logging

from .core import DataMarket, DataType, Frequency
from .downloader import BinanceDataDownloader
from .utils import parse_arguments, setup_logging, convert_csv_directory_to_feather # Added import


async def main():
    """Main entry point."""
    args = parse_arguments()
    setup_logging(args.log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting Binance Data Downloader")

    if args.convert_dir:
        logger.info(f"Starting CSV to Feather conversion for directory: {args.convert_dir}")
        convert_csv_directory_to_feather(args.convert_dir, args.delete_csv)
        logger.info("Directory conversion finished.")
        return

    # Convert string arguments to enums
    markets = [DataMarket(market) for market in args.data_markets]
    data_types = [DataType(dt) for dt in args.data_types]
    frequencies = [Frequency(freq) for freq in args.frequencies]

    # Create downloader and start download
    # Assumes BinanceDataDownloader constructor will be updated to accept to_feather and delete_csv
    downloader = BinanceDataDownloader(args.output_directory, args.to_feather, args.delete_csv)

    try:
        await downloader.download_data(
            markets=markets,
            data_types=data_types,
            symbols=args.symbols,
            frequencies=frequencies,
            start_date=args.start_date,
            end_date=args.end_date,
            download_interval_type=args.download_interval_type,
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
