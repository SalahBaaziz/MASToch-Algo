import pandas as pd
import yfinance as yf
import pytz
import matplotlib.pyplot as plt

# Function to calculate RSI
def calculate_rsi(data, period=14):
    delta = data['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Function to calculate buy/sell signals
def get_signals(data, fast_ma, slow_ma):
    # Calculate moving averages
    data["FastMA"] = data["Close"].rolling(fast_ma).mean()
    data["SlowMA"] = data["Close"].rolling(slow_ma).mean()

    # Calculate RSI
    data["RSI"] = calculate_rsi(data)

    # Identify buy and sell signals based on MA crossovers
    cross_up = (data["FastMA"] > data["SlowMA"]) & (data["FastMA"].shift(1) <= data["SlowMA"].shift(1))  # Buy signal
    cross_down = (data["FastMA"] < data["SlowMA"]) & (data["FastMA"].shift(1) >= data["SlowMA"].shift(1))  # Sell signal

    # Identify buy and sell signals based on RSI
    rsi_buy = (data["RSI"] < 30)  # RSI Oversold condition
    rsi_sell = (data["RSI"] > 70)  # RSI Overbought condition

    # Initialize signal column with 0 (no signal)
    signals = pd.Series("0", index=data.index)

    # Mark Buy (B) and Sell (S)
    signals[cross_up | rsi_buy] = "B"
    signals[cross_down | rsi_sell] = "S"

    return signals

# Function to create the signal matrix for multiple tickers
def create_signal_matrix(tickers, fast_ma, slow_ma):
    signal_matrix = pd.DataFrame()

    for ticker in tickers:
        print(f"Processing {ticker}...")
        # Download stock data for each ticker
        data = yf.download(ticker, period="730d", interval="1h")

        # Get signals for the ticker
        signals = get_signals(data, fast_ma, slow_ma)

        # Add signals to the matrix with the ticker as the column name
        signal_matrix[ticker] = signals

    return signal_matrix

# Function to align dates and handle missing data
def align_dates(signal_matrix):
    # Fill any missing data (NaN) with '0' (no signal)
    signal_matrix = signal_matrix.fillna('0')
    return signal_matrix

# Function to aggregate counts of signals for each hour
def aggregate_signals(signal_matrix):
    signal_summary = pd.DataFrame(index=signal_matrix.index)
    signal_summary['Buy_Count'] = (signal_matrix == 'B').sum(axis=1)
    signal_summary['Sell_Count'] = (signal_matrix == 'S').sum(axis=1)

    # Convert index to London time if it's timezone-naive
    london_tz = pytz.timezone('Europe/London')
    if signal_summary.index.tz is None:  # Check if index is timezone-naive
        signal_summary.index = signal_summary.index.tz_localize('UTC').tz_convert(london_tz)
    else:
        signal_summary.index = signal_summary.index.tz_convert(london_tz)

    return signal_summary

# Function to generate long/short positions based on the net signals
def generate_positions(signal_summary, sell_threshold, buy_threshold):
    signal_summary['B_S'] = signal_summary['Buy_Count'] - signal_summary['Sell_Count']
    signal_summary['Position'] = "No Signal"  # Default position is no signal

    # Create variables to track the current position (long or short)
    long_position_active = False
    short_position_active = False

    # Loop over each row and generate long/short positions based on B_S
    for i in range(len(signal_summary)):
        b_s_value = signal_summary.iloc[i]['B_S']

        # Check if we have a long signal and no active long position
        if b_s_value <= sell_threshold and not long_position_active:
            signal_summary.iloc[i, signal_summary.columns.get_loc('Position')] = "Short"
            long_position_active = True
            short_position_active = False  # Reset short position

        # Check if we have a short signal and no active short position
        elif b_s_value >= buy_threshold and not short_position_active:
            signal_summary.iloc[i, signal_summary.columns.get_loc('Position')] = "Long"
            short_position_active = True
            long_position_active = False  # Reset long position

    return signal_summary

# Function to get NASDAQ hourly prices
def get_nasdaq_prices():
    nasdaq_data = yf.download("^NDX", period="730d", interval="1h")
    return nasdaq_data[['Close']]

# Function to plot NASDAQ prices with buy/sell signals
def plot_nasdaq_with_signals(nasdaq_prices, signal_summary):
    plt.figure(figsize=(14, 8))
    plt.plot(nasdaq_prices.index, nasdaq_prices['Close'], label='NASDAQ Price', color='blue')

    # Plot Buy signals
    buy_signals = signal_summary[signal_summary['Position'] == "Long"]
    plt.scatter(buy_signals.index, nasdaq_prices.loc[buy_signals.index]['Close'], marker='^', color='green', label='Buy Signal', alpha=1)

    # Plot Sell signals
    sell_signals = signal_summary[signal_summary['Position'] == "Short"]
    plt.scatter(sell_signals.index, nasdaq_prices.loc[sell_signals.index]['Close'], marker='v', color='red', label='Sell Signal', alpha=1)

    plt.title('NASDAQ Price with Buy/Sell Signals')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid()
    plt.show()

# List of NASDAQ 100 companies (reduced for demonstration)
tickers = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "GOOG", "NVDA", "TSLA", "META", "AVGO", "ADBE", "PEP", "COST", "CSCO", "AMD",
    "NFLX", "INTC", "TMUS", "HON", "TXN", "QCOM", "AMGN", "INTU", "AMAT", "SBUX", "BKNG", "PYPL", "ADP", "GILD", "MU",
    "MDLZ", "ISRG", "LRCX", "SNPS", "REGN", "FISV", "MRVL", "ASML", "ORLY", "ATVI", "KLAC", "ADI", "MCHP", "PANW"
]

# Create the signal matrix for all tickers
fast_ma = 100
slow_ma = 250
signal_matrix = create_signal_matrix(tickers, fast_ma, slow_ma)

# Align dates and handle missing data
signal_matrix = align_dates(signal_matrix)

# Aggregate counts of signals for each hour
signal_summary = aggregate_signals(signal_matrix)

# Generate long/short positions based on B-S
sell_threshold = int(input("Enter Sell Threshold: "))
buy_threshold = int(input("Enter Buy Threshold: "))
signal_summary_with_positions = generate_positions(signal_summary, sell_threshold, buy_threshold)

# Get NASDAQ hourly prices
nasdaq_prices = get_nasdaq_prices()

# Combine NASDAQ prices with signal summary
combined_df = signal_summary_with_positions.join(nasdaq_prices, how='outer', rsuffix='_NASDAQ')

# Plot NASDAQ prices with buy/sell signals
plot_nasdaq_with_signals(nasdaq_prices, signal_summary_with_positions)
