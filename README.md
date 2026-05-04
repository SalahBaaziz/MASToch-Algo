# MASToch-Algo

A quantitative trading algorithm that generates buy/sell signals for the NASDAQ 100 index (^NDX) by analyzing technical indicators across its constituent stocks.

## Purpose

- Analyze multiple NASDAQ 100 stocks simultaneously
- Generate aggregated trading signals based on technical analysis
- Provide Long/Short position recommendations for the NASDAQ 100 index
- Support various algorithmic trading strategies

## How It Works

### Core Algorithm (NDX_Hybrid_Algo.py - Main Script)

1. **Data Collection**: Downloads 730 days of hourly price data for all NASDAQ 100 stocks using Yahoo Finance.

2. **Signal Generation per Stock**: For each stock, calculates:
   - **RSI (Relative Strength Index)** with 14-period lookback
     - Buy signal when RSI < 30 (oversold)
     - Sell signal when RSI > 70 (overbought)
   - **Moving Average Crossovers**:
     - Fast MA: 100-period rolling average
     - Slow MA: 250-period rolling average
     - Buy when Fast MA crosses above Slow MA
     - Sell when Fast MA crosses below Slow MA

3. **Signal Aggregation**: Counts total buy and sell signals across all 84 NASDAQ 100 stocks for each hour.

4. **Position Generation**: 
   - Calculates net signal (Buy Count - Sell Count)
   - User-defined thresholds determine when to enter positions
   - Generates "Long" or "Short" positions for the overall NASDAQ index

5. **Output**: Exports combined data with timestamps (converted to London timezone), buy/sell counts, NASDAQ prices, and position recommendations to CSV.

## Key Features

- **Multi-stock Analysis**: Processes all NASDAQ 100 constituent stocks simultaneously
- **Hybrid Strategy**: Combines RSI and Moving Average crossover signals
- **Customizable Thresholds**: Users input buy/sell thresholds for position generation
- **Timezone Handling**: Converts all timestamps to London time (Europe/London)
- **Data Export**: Outputs to CSV format for further analysis
- **Visualization**: Multiple scripts include matplotlib plotting capabilities
- **Multiple Strategies**: Includes alternative algorithms (K-means clustering, Linear Regression Oscillator)

## Technology Stack

| Technology | Purpose |
|------------|---------|
| **Python 3** | Core programming language |
| **pandas** | Data manipulation and analysis |
| **yfinance** | Downloading stock market data from Yahoo Finance |
| **pytz** | Timezone conversions |
| **matplotlib** | Plotting and visualization |
| **numpy** | Numerical computations |
| **scikit-learn** | K-means clustering (in K Means Algo.py) |
| **scipy** | Signal processing - Savitzky-Golay filter |
| **backtrader** | Backtesting framework (in Linear Regression.py) |

## Project Structure

### Main Python Scripts (Root Directory)
- **NDX_Hybrid_Algo.py** - Main algorithm for NASDAQ 100 analysis (primary script)
- **Any Signal.py** - Single ticker analysis version
- **GoldenCode.py** - Original version outputting specific ticker signals
- **Signal Count.py** - Signal aggregation with price plotting
- **ARIMA Forecasting.py** - Linear Regression Oscillator strategy (different approach)
- **NDX Price Pull.py** - Simple NASDAQ price data downloader

### Data Directory (./Data/)
- **K Means Algo.py** - K-means clustering for market turning point detection
- **Linear Regression.py** - Linear Regression Oscillator with Backtrader backtesting
- **Test.py** - Testing version with plotting functionality
- Multiple CSV/Excel files with historical signal data and price information

## Getting Started

### Prerequisites
```bash
pip install pandas yfinance pytz matplotlib numpy scikit-learn scipy backtrader
```

### Running the Main Algorithm
```bash
cd /Users/salahbaaziz/Desktop/Projects/MASToch-Algo
python NDX_Hybrid_Algo.py
```

When prompted:
- Enter Sell Threshold (e.g., -20 for short positions)
- Enter Buy Threshold (e.g., 20 for long positions)

Output: `NASDAQ_Signals_Test.csv` with timestamps, buy/sell counts, NASDAQ prices, and position recommendations.

### Alternative Scripts
- **Single ticker analysis**: `python Any Signal.py` (then enter ticker symbol)
- **K-means clustering**: `python Data/K Means Algo.py` (then enter ticker)
- **Signal counts with plot**: `python Signal Count.py`
- **Linear Regression strategy**: `python ARIMA Forecasting.py`

## NASDAQ 100 Tickers Analyzed

The algorithm processes 84 stocks including: AAPL, MSFT, AMZN, GOOGL, NVDA, TSLA, META, AVGO, ADBE, NFLX, INTC, AMD, and many others.

## Notes

- The README.md is minimal (just the project name)
- The project appears to be a personal/hobby algorithmic trading system
- Multiple iterations of similar code exist (GoldenCode.py, Signal Count.py, Test.py, Any Signal.py)
- Data files include both CSV and Excel formats with historical signal generations
- The system uses hourly intervals for analysis over a 730-day (2-year) period
