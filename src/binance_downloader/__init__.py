# src/binance_downloader/__init__.py

# 確保這些從 .main 導入的物件能在套件頂層被訪問
from .main import (
    DataMarket, DataType, Frequency,
    DownloadTask,
    BinanceURLBuilder, ChecksumValidator, FileDownloader, BinanceDataDownloader,
    MARKET_DATATYPE_CONFIG, KLINE_DATA_TYPES,
    parse_arguments, # <--- 新增這行
    setup_logging,   # <--- 建議也將常用函式導出
    main             # <--- 建議也將主入口導出，方便命令行工具調用
)

# 你也可以在這裡定義套件的版本號
# __version__ = "0.1.0"