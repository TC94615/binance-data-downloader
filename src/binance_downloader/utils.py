import argparse
import logging
from pathlib import Path

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
