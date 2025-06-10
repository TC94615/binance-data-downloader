import asyncio
import pytest
import aiohttp
from aioresponses import aioresponses

from src.binance_downloader.core import DataMarket
from src.binance_downloader.symbol_fetcher import get_all_symbols, API_CONFIG

@pytest.mark.asyncio
async def test_get_all_symbols_spot_success():
    """Test successful symbol fetching for SPOT market."""
    market = DataMarket.SPOT
    config = API_CONFIG[market]
    mock_response = {
        config["symbol_key"]: [
            {config["name_key"]: "BTCUSDT", config["filter_key"]: "TRADING"},
            {config["name_key"]: "ETHUSDT", config["filter_key"]: "BREAK"},
            {config["name_key"]: "ADAUSDT", config["filter_key"]: "TRADING"},
        ]
    }
    expected_symbols = ["BTCUSDT", "ADAUSDT"]

    with aioresponses() as m:
        m.get(config["url"], payload=mock_response)
        async with aiohttp.ClientSession() as session:
            symbols = await get_all_symbols(market, session)
            assert sorted(symbols) == sorted(expected_symbols)

@pytest.mark.asyncio
async def test_get_all_symbols_futures_um_success():
    """Test successful symbol fetching for FUTURES_UM market."""
    market = DataMarket.FUTURES_UM
    config = API_CONFIG[market]
    mock_response = {
        config["symbol_key"]: [
            {config["name_key"]: "BTCUSDT", config["filter_key"]: "TRADING"},
            {config["name_key"]: "ETHUSDT", config["filter_key"]: "AUCTION_MATCH"},
            {config["name_key"]: "XRPUSDT", config["filter_key"]: "TRADING"},
        ]
    }
    expected_symbols = ["BTCUSDT", "XRPUSDT"]

    with aioresponses() as m:
        m.get(config["url"], payload=mock_response)
        async with aiohttp.ClientSession() as session:
            symbols = await get_all_symbols(market, session)
            assert sorted(symbols) == sorted(expected_symbols)

@pytest.mark.asyncio
async def test_get_all_symbols_futures_cm_success():
    """Test successful symbol fetching for FUTURES_CM market."""
    market = DataMarket.FUTURES_CM
    config = API_CONFIG[market]
    mock_response = {
        config["symbol_key"]: [
            {"symbol": "BTCUSD_PERP", "pair": "BTCUSD", config["filter_key"]: "TRADING", "contractType": "PERPETUAL"},
            {"symbol": "ETHUSD_231229", "pair": "ETHUSD", config["filter_key"]: "PENDING_TRADING", "contractType": "QUARTERLY"},
            {"symbol": "ADAUSD_PERP", "pair": "ADAUSD", config["filter_key"]: "TRADING", "contractType": "PERPETUAL"},
        ]
    }
    # Ensure we extract 'symbol' (e.g., BTCUSD_PERP) not 'pair'
    expected_symbols = ["BTCUSD_PERP", "ADAUSD_PERP"]

    with aioresponses() as m:
        m.get(config["url"], payload=mock_response)
        async with aiohttp.ClientSession() as session:
            symbols = await get_all_symbols(market, session)
            assert sorted(symbols) == sorted(expected_symbols)

@pytest.mark.asyncio
async def test_get_all_symbols_option_success():
    """Test successful symbol fetching for OPTION market."""
    market = DataMarket.OPTION
    config = API_CONFIG[market]
    # Example based on structure in symbol_fetcher.py for OPTION
    mock_response = {
        config["symbol_key"]: [
            {"name": "BTC-231229-30000-C", "underlying": "BTCUSDT", "expiryDate": "231229"},
            {"name": "ETH-231229-2000-P", "underlying": "ETHUSDT", "expiryDate": "231229"},
        ]
    }
    expected_symbols = ["BTC-231229-30000-C", "ETH-231229-2000-P"]

    with aioresponses() as m:
        m.get(config["url"], payload=mock_response)
        async with aiohttp.ClientSession() as session:
            symbols = await get_all_symbols(market, session)
            assert sorted(symbols) == sorted(expected_symbols)

@pytest.mark.parametrize("market_enum", [DataMarket.SPOT, DataMarket.FUTURES_UM, DataMarket.FUTURES_CM, DataMarket.OPTION])
@pytest.mark.asyncio
async def test_get_all_symbols_http_error(market_enum, caplog):
    """Test HTTP error handling for all market types."""
    config = API_CONFIG[market_enum]
    error_statuses = [404, 500, 451]

    for status_code in error_statuses:
        caplog.clear()
        with aioresponses() as m:
            m.get(config["url"], status=status_code)
            async with aiohttp.ClientSession() as session:
                symbols = await get_all_symbols(market_enum, session)
                assert symbols == []
                assert f"HTTP error fetching symbols for {market_enum.value}" in caplog.text
                assert str(status_code) in caplog.text

@pytest.mark.parametrize("market_enum", [DataMarket.SPOT, DataMarket.FUTURES_UM, DataMarket.FUTURES_CM, DataMarket.OPTION])
@pytest.mark.asyncio
async def test_get_all_symbols_timeout_error(market_enum, caplog):
    """Test timeout error handling."""
    config = API_CONFIG[market_enum]
    caplog.clear()
    with aioresponses() as m:
        m.get(config["url"], exception=asyncio.TimeoutError("Simulated timeout"))
        async with aiohttp.ClientSession() as session:
            symbols = await get_all_symbols(market_enum, session)
            assert symbols == []
            assert f"Timeout fetching symbols for {market_enum.value}" in caplog.text

@pytest.mark.parametrize("market_enum", [DataMarket.SPOT, DataMarket.FUTURES_UM, DataMarket.FUTURES_CM, DataMarket.OPTION])
@pytest.mark.asyncio
async def test_get_all_symbols_empty_response_list(market_enum, caplog):
    """Test handling of empty symbol list in API response."""
    config = API_CONFIG[market_enum]
    mock_response = {config["symbol_key"]: []}
    caplog.clear()
    with aioresponses() as m:
        m.get(config["url"], payload=mock_response)
        async with aiohttp.ClientSession() as session:
            symbols = await get_all_symbols(market_enum, session)
            assert symbols == []
            # A warning is expected if the key exists but the list is empty
            assert f"No items found under key '{config['symbol_key']}' for market {market_enum.value}" in caplog.text or \
                   f"Fetched 0 symbols for market {market_enum.value}" in caplog.text


@pytest.mark.parametrize("market_enum", [DataMarket.SPOT, DataMarket.FUTURES_UM, DataMarket.FUTURES_CM, DataMarket.OPTION])
@pytest.mark.asyncio
async def test_get_all_symbols_unexpected_json_structure(market_enum, caplog):
    """Test handling of unexpected JSON structure."""
    config = API_CONFIG[market_enum]
    mock_response = {"unexpected_key": "unexpected_value"} # Does not contain symbol_key
    caplog.clear()
    with aioresponses() as m:
        m.get(config["url"], payload=mock_response)
        async with aiohttp.ClientSession() as session:
            symbols = await get_all_symbols(market_enum, session)
            assert symbols == []
            assert f"No items found under key '{config['symbol_key']}' for market {market_enum.value}" in caplog.text


@pytest.mark.parametrize("market_enum", [DataMarket.SPOT, DataMarket.FUTURES_UM])
@pytest.mark.asyncio
async def test_get_all_symbols_no_matching_filter(market_enum, caplog):
    """Test case where symbols exist but none match the filter criteria."""
    market = market_enum
    config = API_CONFIG[market]
    mock_response = {
        config["symbol_key"]: [
            {config["name_key"]: "BTCUSDT", config["filter_key"]: "DELISTED"},
            {config["name_key"]: "ETHUSDT", config["filter_key"]: "BREAK"},
        ]
    }
    expected_symbols = []
    caplog.clear()
    # Ensure the specific logger is at INFO level for this test
    # to ensure the "Fetched 0 symbols..." message is captured.
    import logging
    logging.getLogger("src.binance_downloader.symbol_fetcher").setLevel(logging.INFO)

    with aioresponses() as m:
        m.get(config["url"], payload=mock_response)
        async with aiohttp.ClientSession() as session:
            symbols = await get_all_symbols(market, session)
            assert symbols == expected_symbols
            assert f"Fetched 0 symbols for market {market.value}" in caplog.text
