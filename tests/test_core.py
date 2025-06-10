from pathlib import Path
import pytest

from src.binance_downloader.core import (
    DataMarket,
    DataType,
    Frequency,
    DownloadTask,
    MARKET_DATATYPE_CONFIG,
    KLINE_DATA_TYPES,
)


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
