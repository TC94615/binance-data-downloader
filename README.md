# Binance Historical Data Downloader

A robust, high-performance Python script for downloading historical data from the Binance Data Vision portal. This script features comprehensive checksum verification, concurrent downloads, and extensive error handling.

## ✨ Features

-   **High-Performance Concurrent Downloads**: Utilizes asyncio for asynchronous I/O, with configurable concurrency limits to maximize network bandwidth.
-   **Guaranteed Data Integrity**: Automatically downloads corresponding .CHECKSUM files and verifies each downloaded file using SHA256 to ensure data is not corrupted.
-   **Automatic Extraction**: Validated .zip files are automatically unzipped into organized folders, making the CSV data immediately accessible.
-   **Comprehensive Data Support**: Full support for all data types and frequencies across Binance Spot, UM Futures, CM Futures, and Options markets.
-   **Robust Error Handling**: Gracefully handles network failures, 404 Not Found errors, and checksum mismatches, automatically deleting invalid files.
-   **Modern Command-Line Interface**: An easy-to-use CLI with sensible defaults for all parameters, making it ready to use out-of-the-box.
-   **Thoroughly Tested**: Comes with an extensive test suite built with pytest, ensuring code stability and reliability.

## 🚀 Installation

This project is best managed with Poetry, but can also be installed using pip.

### A) For Developers (with Poetry)

This is the recommended approach for developers, as it simplifies managing development and testing environments.

1.  **Clone the repository**:
    ```bash
    $ git clone <your-repo-url>
    $ cd <project-directory>
    ```

2.  **Install dependencies**:
    This command installs all main dependencies, development tools (black, flake8) and the testing suite (pytest).
    ```bash
    $ poetry install --with dev,test
    ```

3.  **Activate the virtual environment**:
    ```bash
    $ poetry shell
    ``` 

4.  **Verify the installation**:
    ```bash
    $ python main.py --help
    ``` 

### B) For Users (with pip)

If you just want to use the script, follow these steps.

1.  **Save requirements.txt**:
    Create a file named `requirements.txt` with the following content:
    ```bash
    aiohttp>=3.9.0
    aiofiles>=23.2.1
    ```

2.  **Install dependencies**:
    ```bash
    $ pip install -r requirements.txt
    ```

3.  **Verify the installation**:
    ```bash
    $ python main.py --help
    ```

## 💡 Usage Examples

### Basic Usage

Download daily K-line data for BTCUSDT on the Spot market for the last 30 days:
```bash
$ python main.py --symbols BTCUSDT --data-markets spot --data-types klines --frequencies 1d
```
### Advanced Examples

Download data for multiple symbols, markets, types, and frequencies:
```bash
$ python main.py \
  --symbols BTCUSDT ETHUSDT \
  --data-markets spot futures-um \
  --data-types klines aggTrades \
  --frequencies 1h 4h 1d \
  --start-date 2025-01-01 \
  --end-date 2025-01-31
```

Download 5-minute K-line data for UM-Futures within a specific date range:
```bash
$ python main.py \
  --data-markets futures-um \
  --data-types klines \
  --symbols BTCUSDT ETHUSDT \
  --frequencies 5m \
  --start-date 2025-06-01 \
  --end-date 2025-06-07 \
  --output-directory ./my_futures_data
```
Download Options data:
```bash
$ python main.py \
  --data-markets option \
  --data-types BVOLIndex EOHSummary \
  --symbols BTCBVOLUSDT \
  --start-date 2025-06-01 \
  --end-date 2025-06-07
```
## Command-Line Arguments

| Argument | Description | Default | Example |
| :--- | :--- | :--- | :--- |
| `--data-markets` | The data markets to download from | All markets | `spot futures-um` |
| `--data-types` | The data types to download | All valid types | `klines aggTrades` |
| `--frequencies` | The frequencies for K-line data | All frequencies | `1m 5m 1h 1d` |
| `--symbols` | The trading pair symbols | `BTCUSDT` | `BTCUSDT ETHUSDT` |
| `--start-date` | Start date (YYYY-MM-DD) | 30 days ago | `2025-01-01` |
| `--end-date` | End date (YYYY-MM-DD) | Today | `2025-01-31` |
| `--output-directory` | Output directory for downloaded data | `./downloaded_data` | `./my_data` |
| `--log-level` | The logging level | `INFO` | `DEBUG` |

## 📊 Data Support Details

### Supported Markets

-   **spot**: Spot Market
-   **futures-cm**: COIN-Margined Futures
-   **futures-um**: USDT-Margined Futures
-   **option**: Options

### Supported Data Types

| Data Type | Description | Supported Markets | Period | Requires Freq.? |
| :--- | :--- | :--- | :--- | :--- |
| `klines` | Candlestick data | spot, futures-cm, futures-um | Daily, Monthly | ✅ Yes |
| `aggTrades` | Aggregated Trades | spot, futures-cm, futures-um | Daily, Monthly | No |
| `trades` | Raw Trades | spot, futures-cm, futures-um | Daily, Monthly | No |
| `bookTicker` | Best Bid/Ask | futures-cm, futures-um | Daily, Monthly | No |
| `bookDepth` | Order Book Depth | futures-cm, futures-um | Daily | No |
| `fundingRate` | Funding Rates | futures-cm, futures-um | Monthly | No |
| `liquidationSnapshot`| Liquidation Snapshots | futures-cm, futures-um | Daily | No |
| `metrics` | 24hr Ticker Stats | futures-cm, futures-um | Daily | No |
| `indexPriceKlines`| Index Price K-lines | futures-cm, futures-um | Daily, Monthly | ✅ Yes |
| `markPriceKlines` | Mark Price K-lines | futures-cm, futures-um | Daily, Monthly | ✅ Yes |
| `premiumIndexKlines`| Premium Index K-lines | futures-cm, futures-um | Daily, Monthly | ✅ Yes |
| `BVOLIndex` | Volatility Index | option | Daily | No |
| `EOHSummary` | End-of-Hour Summary | option | Daily | No |

### Supported Frequencies (for K-line data)

`1s`, `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1mo`

## 💻 Development & Testing

### Running Tests

Run the complete test suite:
```bash
$ pytest tests.py -v
```
Run tests with coverage report:
```bash
$ pytest tests.py --cov=main --cov-report=html
```
Run a specific test class:
# Run unit tests for the URL Builder only
```bash
$ pytest tests.py::TestBinanceURLBuilder -v
```
# Run integration tests only
```bash
$ pytest tests.py::TestIntegration -v
```
### Code Quality

Format code with Black:
```bash
$ black main.py tests.py
```
Lint with Flake8:
```bash
$ flake8 main.py
```
Check types with MyPy:
```bash
$ mypy main.py
```
---
---

# 幣安歷史數據下載器 (Chinese Version)

一個強健、高效能的 Python 腳本，用於從幣安數據門戶 (Binance Data Vision) 下載歷史數據。此腳本具備完整的校驗和 (Checksum) 驗證、並行下載以及廣泛的錯誤處理機制。

## ✨ 功能亮點

-   **高效能並行下載**: 基於 asyncio 實現異步 I/O，可配置並行下載數量以最大化網路頻寬利用率。
-   **保證數據完整性**: 自動下載對應的 .CHECKSUM 檔案，並透過 SHA256 算法驗證每個下載檔案的完整性，確保數據無損。
-   **全自動解壓縮**: 下載完成並驗證通過的 .zip 檔案會被自動解壓縮到對應的資料夾中，方便直接使用 CSV 檔案。
-   **全面的數據支援**: 完整支援幣安現貨、U本位合約、幣本位合約及選擇權市場的所有數據類型與頻率。
-   **強健的錯誤處理**: 處理了網路請求失敗、檔案找不到 (404) 及校驗失敗等情況，並會在校驗失敗後自動刪除無效檔案。
-   **現代化的命令列介面**: 提供簡單易用的 CLI，並為所有參數提供合理的預設值，開箱即用。
-   **經過完整測試**: 具備基於 pytest 的廣泛測試套件，確保程式碼的穩定性與可靠性。

## 🚀 安裝指南

本專案使用 Poetry 進行依賴管理，但您也可以透過 pip 進行安裝。

### A) 開發者 (使用 Poetry)

這是推薦給開發者的方式，可以輕鬆管理開發與測試環境。

1.  **複製專案**:
    ```bash   
    $ git clone <your-repo-url>
    $ cd <project-directory>
    ```

2.  **安裝依賴**:
    此指令會安裝所有主要依賴、開發工具 (black, flake8) 及測試套件 (pytest)。
    ```bash
    $ poetry install --with dev,test
    ```

3.  **啟用虛擬環境**:
    ```bash
    $ poetry shell
    ```

4.  **驗證安裝**:
    ```bash
    $ python main.py --help
    ```

### B) 使用者 (使用 pip)

如果您只是想使用此腳本，可以按照以下步驟操作。

1.  **準備 requirements.txt**:
    將以下內容儲存為 `requirements.txt` 檔案。
    ```bash
    aiohttp>=3.9.0
    aiofiles>=23.2.1
    ```

2.  **安裝依賴**:
    ```bash
    $ pip install -r requirements.txt
    ```

3.  **驗證安裝**:
    ```bash
    $ python main.py --help
    ```

## 💡 使用範例

### 基本用法

下載 BTCUSDT 現貨市場過去 30 天的日線 K 線數據：
```bash
$ python main.py --symbols BTCUSDT --data-markets spot --data-types klines --frequencies 1d
```
### 進階用法

下載多個交易對、多個市場、多種類型及頻率的數據:
```bash
$ python main.py \
  --symbols BTCUSDT ETHUSDT \
  --data-markets spot futures-um \
  --data-types klines aggTrades \
  --frequencies 1h 4h 1d \
  --start-date 2025-01-01 \
  --end-date 2025-01-31
```
下載 U本位合約指定日期範圍的 5分鐘 K 線數據:
```bash
$ python main.py \
  --data-markets futures-um \
  --data-types klines \
  --symbols BTCUSDT ETHUSDT \
  --frequencies 5m \
  --start-date 2025-06-01 \
  --end-date 2025-06-07 \
  --output-directory ./my_futures_data
```
下載選擇權數據:
```bash
$ python main.py \
  --data-markets option \
  --data-types BVOLIndex EOHSummary \
  --symbols BTCBVOLUSDT \
  --start-date 2025-06-01 \
  --end-date 2025-06-07
```
## 命令行參數

| Argument | Description | Default | Example |
| :--- | :--- | :--- | :--- |
| `--data-markets` | 要下載的數據市場 | 所有市場 | `spot futures-um` |
| `--data-types` | 要下載的數據類型 | 所有類型 | `klines aggTrades` |
| `--frequencies` | K 線數據的頻率 | 所有頻率 | `1m 5m 1h 1d` |
| `--symbols` | 交易對符號 | `BTCUSDT` | `BTCUSDT ETHUSDT` |
| `--start-date` | 開始日期 (YYYY-MM-DD) | 30 天前 | `2025-01-01` |
| `--end-date` | 結束日期 (YYYY-MM-DD) | 今天 | `2025-01-31` |
| `--output-directory` | 下載數據的輸出目錄 | `./downloaded_data` | `./my_data` |
| `--log-level` | 日誌記錄級別 | `INFO` | `DEBUG` |

## 📊 數據支援詳情

### 支援的市場

-   **spot**: 現貨市場
-   **futures-cm**: 幣本位合約
-   **futures-um**: U本位合約
-   **option**: 選擇權

### 支援的數據類型

| Data Type | Description | Supported Markets | Period | Requires Freq.? |
| :--- | :--- | :--- | :--- | :--- |
| `klines` | K線/蠟燭圖數據 | spot, futures-cm, futures-um | Daily, Monthly | ✅ 是 |
| `aggTrades` | 聚合交易數據 | spot, futures-cm, futures-um | Daily, Monthly | 否 |
| `trades` | 原始交易數據 | spot, futures-cm, futures-um | Daily, Monthly | 否 |
| `bookTicker` | 最優買賣盤 | futures-cm, futures-um | Daily, Monthly | 否 |
| `bookDepth` | 訂單簿深度 | futures-cm, futures-um | Daily | 否 |
| `fundingRate` | 資金費率 | futures-cm, futures-um | Monthly | 否 |
| `liquidationSnapshot`| 強平快照 | futures-cm, futures-um | Daily | 否 |
| `metrics` | 24小時行情統計 | futures-cm, futures-um | Daily | 否 |
| `indexPriceKlines`| 指數價格K線 | futures-cm, futures-um | Daily, Monthly | ✅ 是 |
| `markPriceKlines` | 標記價格K線 | futures-cm, futures-um | Daily, Monthly | ✅ 是 |
| `premiumIndexKlines`| 溢價指數K線 | futures-cm, futures-um | Daily, Monthly | ✅ 是 |
| `BVOLIndex` | 波動率指數 | option | Daily | 否 |
| `EOHSummary` | 每小時結算摘要 | option | Daily | 否 |

### 支援的頻率 (用於 K 線相關數據)

`1s`, `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1mo`

## 💻 開發與測試

### 執行測試

執行完整的測試套件:
```bash
$ pytest tests.py -v
```
執行測試並產生覆蓋率報告:
```bash
$ pytest tests.py --cov=main --cov-report=html
```
僅執行特定類別的測試:
# 僅執行 URL Builder 的單元測試
```bash
$ pytest tests.py::TestBinanceURLBuilder -v
```
# 僅執行整合測試
```bash
$ pytest tests.py::TestIntegration -v
```
### 代碼品質

使用 Black 自動格式化程式碼:
```bash
$ black main.py tests.py
```
使用 Flake8 進行靜態程式碼檢查:
```bash
$ flake8 main.py
```
使用 MyPy 進行類型檢查:
```bash
$ mypy main.py
```