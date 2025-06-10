import logging
from typing import Optional
from urllib.parse import urljoin

from .core import KLINE_DATA_TYPES, DataMarket, DataType, Frequency


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
