import argparse
import logging
from pathlib import Path
import pandas as pd # Added pandas import

from .core import DataMarket, DataType, Frequency


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
        nargs="*",  # Allow zero or more symbols
        default=None,  # Default to None, indicating all symbols should be fetched
        help="Trading symbols to download. If not provided, all symbols for the selected markets will be fetched.",
    )

    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")

    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")

    parser.add_argument(
        "--download-interval-type",
        dest='download_interval_type',
        choices=['daily', 'monthly', 'both'],
        default='both',
        help="Type of download interval: 'daily', 'monthly', or 'both'. This determines if daily data, monthly data, or both are downloaded. Default is 'both'."
    )

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

    parser.add_argument(
        "--to-feather",
        action="store_true",
        help="Convert downloaded CSV files to Feather format.",
    )

    parser.add_argument(
        "--delete-csv",
        action="store_true",
        help="Delete CSV files after converting to Feather format (only effective if --to-feather is active).",
    )

    parser.add_argument(
        "--convert-dir",
        type=Path,
        help="Convert all CSV files in the specified directory to Feather format. If this option is used, no downloads will be performed.",
    )

    return parser.parse_args()


def convert_csv_to_feather(csv_path: Path, delete_csv: bool) -> bool:
    """Converts a CSV file to Feather format.

    Args:
        csv_path: Path to the input CSV file.
        delete_csv: Whether to delete the original CSV file after conversion.

    Returns:
        True if conversion was successful, False otherwise.
    """
    logger = logging.getLogger(__name__)

    if not csv_path.exists() or not csv_path.is_file():
        logger.error(f"CSV file not found or is not a file: {csv_path}")
        return False

    feather_path = csv_path.with_suffix(".feather")

    try:
        df = pd.read_csv(csv_path)
        df.to_feather(feather_path)
        logger.info(f"Successfully converted {csv_path} to {feather_path}")

        if delete_csv:
            csv_path.unlink()
            logger.info(f"Deleted CSV file: {csv_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to convert {csv_path} to Feather: {e}")
        return False


def convert_csv_directory_to_feather(directory_path: Path, delete_csv: bool) -> None:
    """Converts all CSV files in a directory to Feather format.

    Args:
        directory_path: Path to the directory containing CSV files.
        delete_csv: Whether to delete original CSV files after conversion.
    """
    logger = logging.getLogger(__name__)

    if not directory_path.exists() or not directory_path.is_dir():
        logger.error(f"Directory not found or not a directory: {directory_path}")
        return

    success_count = 0
    failed_count = 0

    logger.info(f"Starting CSV to Feather conversion for directory: {directory_path}")

    for item_path in directory_path.iterdir():
        if item_path.is_file() and item_path.suffix == ".csv":
            if convert_csv_to_feather(item_path, delete_csv):
                success_count += 1
            else:
                failed_count += 1

    logger.info(f"CSV to Feather conversion for directory {directory_path} completed.")
    logger.info(f"Successful conversions: {success_count}")
    logger.info(f"Failed conversions: {failed_count}")
