import asyncio
import logging
from typing import List, Dict, Any, Optional, TypedDict
import aiohttp
from aiohttp import ClientTimeout

from .core import DataMarket

logger = logging.getLogger(__name__)

API_TIMEOUT = 10 # Define API_TIMEOUT constant

class MarketDetails(TypedDict):
    url: str
    symbol_key: str
    filter_key: Optional[str]
    filter_value: Optional[str]
    name_key: str

API_CONFIG: Dict[DataMarket, MarketDetails] = {
    DataMarket.SPOT: {
        "url": "https://api.binance.com/api/v3/exchangeInfo",
        "symbol_key": "symbols",
        "filter_key": "status",
        "filter_value": "TRADING",
        "name_key": "symbol",
    },
    DataMarket.FUTURES_UM: {
        "url": "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "symbol_key": "symbols",
        "filter_key": "status",
        "filter_value": "TRADING",
        "name_key": "symbol",
    },
    DataMarket.FUTURES_CM: {
        "url": "https://dapi.binance.com/dapi/v1/exchangeInfo",
        "symbol_key": "symbols",
        "filter_key": "contractStatus",
        "filter_value": "TRADING",
        "name_key": "symbol",
    },
    DataMarket.OPTION: {
        "url": "https://eapi.binance.com/eapi/v1/exchangeInfo",
        "symbol_key": "optionSymbols",
        "filter_key": None,
        "filter_value": None,
        "name_key": "name",
    },
}


async def get_all_symbols(
    market: DataMarket, session: aiohttp.ClientSession
) -> List[str]:
    """
    Fetches all tradable symbols for a given market from the Binance API.

    Args:
        market: The DataMarket (SPOT, FUTURES_UM, FUTURES_CM, OPTION).
        session: An aiohttp.ClientSession for making HTTP requests.

    Returns:
        A list of symbol strings. Returns an empty list if fetching fails or no symbols are found.
    """
    if market not in API_CONFIG:
        logger.error(f"Unsupported market for symbol fetching: {market.value}")
        return []

    config = API_CONFIG[market]
    url = config["url"]
    symbol_key = config["symbol_key"]
    filter_key = config.get("filter_key")
    filter_value = config.get("filter_value")
    name_key = config["name_key"]

    try:
        logger.debug(f"Fetching symbols for market {market.value} from {url}")
        async with session.get(url, timeout=ClientTimeout(total=API_TIMEOUT)) as response:
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = await response.json()

            symbols: List[str] = []
            items = data.get(symbol_key, [])

            if not items:
                logger.warning(f"No items found under key '{symbol_key}' for market {market.value} in API response from {url}")
                return []

            if market == DataMarket.OPTION:
                # Options: 'optionSymbols' is a list of objects, each having a 'name'
                # Example structure from docs: data['optionSymbols'] = [{'name': 'BTC-231229-30000-C', ...}]
                symbols = [item.get(name_key) for item in items if item.get(name_key)]
            else:
                # Spot, Futures UM/CM
                for item in items:
                    if filter_key and item.get(filter_key) == filter_value:
                        symbol_name = item.get(name_key)
                        if symbol_name:
                            symbols.append(symbol_name)
                    elif not filter_key: # For markets that might not have a filter_key (future-proofing)
                        symbol_name = item.get(name_key)
                        if symbol_name:
                            symbols.append(symbol_name)

            logger.info(f"Fetched {len(symbols)} symbols for market {market.value}")
            return symbols

    except aiohttp.ClientResponseError as e:
        logger.error(
            f"HTTP error fetching symbols for {market.value} from {url}: {e.status} {e.message}"
        )
        return []
    except aiohttp.ClientConnectionError as e:
        logger.error(
            f"Connection error fetching symbols for {market.value} from {url}: {e}"
        )
        return []
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching symbols for {market.value} from {url}")
        return []
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while fetching symbols for {market.value} from {url}: {e}"
        )
        return []
