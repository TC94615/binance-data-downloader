from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Optional, Set


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
KLINE_DATA_TYPES: Set[DataType] = {
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
