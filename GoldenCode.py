import pandas as pd
import yfinance as yf


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


# Function to aggregate signals for each day
def aggregate_signals(signal_matrix):
    signal_summary = pd.DataFrame(index=signal_matrix.index)
    signal_summary['B'] = ''
    signal_summary['S'] = ''

    for date in signal_matrix.index:
        # Get the tickers for Buy (B) signals
        buy_tickers = signal_matrix.loc[date][signal_matrix.loc[date] == 'B'].index.tolist()
        sell_tickers = signal_matrix.loc[date][signal_matrix.loc[date] == 'S'].index.tolist()

        # Join tickers with commas
        signal_summary.at[date, 'B'] = ','.join(buy_tickers)
        signal_summary.at[date, 'S'] = ','.join(sell_tickers)

    return signal_summary


# List of NASDAQ 100 companies
tickers = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "GOOG", "NVDA", "TSLA", "META", "AVGO", "ADBE", "PEP", "COST", "CSCO", "AMD",
    "NFLX", "INTC", "TMUS", "HON", "TXN", "QCOM", "AMGN", "INTU", "AMAT", "SBUX", "BKNG", "PYPL", "ADP", "GILD", "MU",
    "MDLZ", "ISRG", "LRCX", "SNPS", "REGN", "FISV", "MRVL", "ASML", "ORLY", "ATVI", "KLAC", "ADI", "MCHP", "PANW",
    "CDNS", "MAR", "FTNT", "AEP", "CRWD", "DXCM", "VRTX", "XEL", "MNST", "EA", "WDAY", "PDD", "BIDU", "ILMN", "LULU",
    "IDXX", "ABNB", "NXPI", "PCAR", "KDP", "EXC", "PAYX", "EBAY", "ODFL", "CSGP", "TEAM", "AZN", "VRSK", "ENPH",
    "SGEN", "CHTR", "CPRT", "FAST", "MRNA", "SWKS", "CTAS", "VRSN", "MELI", "SPLK", "BIIB", "MRNA", "OKTA", "ZM",
    "ALGN", "MTCH", "DDOG", "NTES", "ANSS", "CDW", "DXCM", "ZS", "SE", "LCID", "PDD", "JD", "LPLA", "BMRN"
]

# Create the signal matrix for all tickers
fast_ma = 100
slow_ma = 250
signal_matrix = create_signal_matrix(tickers, fast_ma, slow_ma)

# Align dates and handle missing data
signal_matrix = align_dates(signal_matrix)

# Aggregate signals for each day
signal_summary = aggregate_signals(signal_matrix)

# Export the signal summary to a single CSV file
signal_summary.to_csv('NAS100_MAStoch_Aggregated.csv')

print("Signal summary successfully exported to 'NAS100_MAStoch_Aggregated.csv'.")
