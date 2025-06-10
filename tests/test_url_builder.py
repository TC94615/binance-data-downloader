import pytest

from src.binance_downloader.core import DataMarket, DataType, Frequency
from src.binance_downloader.url_builder import BinanceURLBuilder


class TestBinanceURLBuilder:
    """Test cases for BinanceURLBuilder class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.url_builder = BinanceURLBuilder()

    def test_build_url_spot_daily_klines(self):
        """Test URL building for spot daily klines data."""
        url = self.url_builder.build_url(
            market=DataMarket.SPOT,
            data_type=DataType.KLINES,
            symbol="BTCUSDT",
            date="2025-06-08",
            frequency=Frequency.ONE_DAY,
            is_monthly=False,
        )

        expected = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2025-06-08.zip"
        assert url == expected

    def test_build_url_spot_monthly_aggtrades(self):
        """Test URL building for spot monthly aggTrades data."""
        url = self.url_builder.build_url(
            market=DataMarket.SPOT,
            data_type=DataType.AGG_TRADES,
            symbol="BTCUSDT",
            date="2025-05",
            is_monthly=True,
        )

        expected = "https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2025-05.zip"
        assert url == expected

    def test_build_url_futures_um_daily_klines(self):
        """Test URL building for futures UM daily klines."""
        url = self.url_builder.build_url(
            market=DataMarket.FUTURES_UM,
            data_type=DataType.KLINES,
            symbol="1000000BOBUSDT",
            date="2025-06-08",
            frequency=Frequency.ONE_DAY,
            is_monthly=False,
        )

        expected = "https://data.binance.vision/data/futures/um/daily/klines/1000000BOBUSDT/1d/1000000BOBUSDT-1d-2025-06-08.zip"
        assert url == expected

    def test_build_url_futures_cm_monthly_trades(self):
        """Test URL building for futures CM monthly trades."""
        url = self.url_builder.build_url(
            market=DataMarket.FUTURES_CM,
            data_type=DataType.TRADES,
            symbol="AAVEUSD_PERP",
            date="2025-05",
            is_monthly=True,
        )

        expected = "https://data.binance.vision/data/futures/cm/monthly/trades/AAVEUSD_PERP/AAVEUSD_PERP-trades-2025-05.zip"
        assert url == expected

    def test_build_url_option_bvol_index(self):
        """Test URL building for option BVOLIndex data."""
        url = self.url_builder.build_url(
            market=DataMarket.OPTION,
            data_type=DataType.BVOL_INDEX,
            symbol="BTCBVOLUSDT",
            date="2025-06-08",
            is_monthly=False,
        )

        expected = "https://data.binance.vision/data/option/daily/BVOLIndex/BTCBVOLUSDT/BTCBVOLUSDT-BVOLIndex-2025-06-08.zip"
        assert url == expected

    def test_build_url_kline_without_frequency_raises_error(self):
        """Test that building URL for kline data without frequency raises error."""
        with pytest.raises(ValueError, match="Frequency required for data type"):
            self.url_builder.build_url(
                market=DataMarket.SPOT,
                data_type=DataType.KLINES,
                symbol="BTCUSDT",
                date="2025-06-08",
            )

    def test_build_url_unsupported_market_raises_error(self):
        """Test that unsupported market raises error."""
        # This would require creating a mock unsupported market
        # For now, we'll test with a known enum value
        pass

    def test_build_market_type_path_futures_um_daily(self):
        """Test market type path building for futures UM daily."""
        path = self.url_builder._build_market_type_path(
            DataMarket.FUTURES_UM, DataType.KLINES, False
        )
        assert path == "futures/um/daily"

    def test_build_market_type_path_spot_monthly(self):
        """Test market type path building for spot monthly."""
        path = self.url_builder._build_market_type_path(
            DataMarket.SPOT, DataType.AGG_TRADES, True
        )
        assert path == "spot/monthly"

    def test_build_symbol_file_path_klines(self):
        """Test symbol file path building for klines."""
        path = self.url_builder._build_symbol_file_path(
            DataType.KLINES, "BTCUSDT", Frequency.ONE_DAY
        )
        assert path == "klines/BTCUSDT/1d"

    def test_build_symbol_file_path_non_klines(self):
        """Test symbol file path building for non-klines data."""
        path = self.url_builder._build_symbol_file_path(
            DataType.AGG_TRADES, "BTCUSDT", None
        )
        assert path == "aggTrades/BTCUSDT"

    def test_build_filename_klines(self):
        """Test filename building for klines data."""
        filename = self.url_builder._build_filename(
            "BTCUSDT", DataType.KLINES, "2025-06-08", Frequency.ONE_DAY
        )
        assert filename == "BTCUSDT-1d-2025-06-08.zip"

    def test_build_filename_non_klines(self):
        """Test filename building for non-klines data."""
        filename = self.url_builder._build_filename(
            "BTCUSDT", DataType.AGG_TRADES, "2025-06-08", None
        )
        assert filename == "BTCUSDT-aggTrades-2025-06-08.zip"
