import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from scipy.signal import savgol_filter


# Function to calculate RSI
def calculate_rsi(data, window=14):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


# Function to fetch stock data and calculate features
def fetch_stock_data(ticker, start_date, end_date):
    # Download stock data
    data = yf.download(ticker, start=start_date, end=end_date)

    # Calculate RSI
    data['RSI'] = calculate_rsi(data)

    # Apply Savitzky-Golay filter to smooth RSI for reduced noise
    # Fill NaN values with the previous valid observation before smoothing
    data['RSI_Smooth'] = savgol_filter(data['RSI'].fillna(method='bfill'), window_length=5, polyorder=2)

    # Maintain the same length by filling NaN values that are at the start of the series
    data['RSI_Smooth'] = data['RSI_Smooth'].where(data['RSI'].notna(), np.nan)

    return data


# Function to perform K-means clustering
def perform_kmeans(data, n_clusters=4):
    features = data[['Close', 'Volume', 'RSI_Smooth']].dropna()

    # Standardizing the features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    # K-means clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    data['Cluster'] = np.nan  # Initialize column
    data.loc[features.index, 'Cluster'] = kmeans.fit_predict(X_scaled)

    return data


# Function to label clusters
def label_clusters(data):
    conditions = [
        (data['Cluster'] == 2),  # Uptrend Turning Point
    ]
    labels = ['Uptrend Turning Point']
    data['Signal'] = np.select(conditions, labels, default='Neutral')

    return data


# Function to plot the signals
def plot_signals(data, ticker):
    plt.figure(figsize=(14, 7))

    # Price plot
    plt.subplot(2, 1, 1)
    plt.plot(data['Close'], label='Close Price', color='blue')

    # Plot signals
    for signal in data['Signal'].unique():
        subset = data[data['Signal'] == signal]
        plt.scatter(subset.index, subset['Close'], label=signal, s=100, alpha=0.7)

    plt.title(f'{ticker} Price and Trading Signals')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid()

    # RSI plot
    plt.subplot(2, 1, 2)
    plt.plot(data['RSI_Smooth'], label='RSI (smoothed)', color='orange')
    plt.axhline(25, color='red', linestyle='--', label='Buy Threshold (30)')
    plt.axhline(75, color='green', linestyle='--', label='Sell Threshold (70)')
    plt.title(f'{ticker} RSI Indicator')
    plt.xlabel('Date')
    plt.ylabel('RSI')
    plt.legend()
    plt.grid()

    plt.tight_layout()
    plt.show()


# Main function to run the analysis
def main():
    # User input
    ticker = input("Enter stock ticker symbol (e.g., AAPL): ").strip().upper()
    start_date = "2021-01-01"
    end_date = "2024-10-06"

    # Step 1: Fetch stock data
    data = fetch_stock_data(ticker, start_date, end_date)

    # Step 2: Perform K-means clustering
    data = perform_kmeans(data)

    # Step 3: Label clusters
    data = label_clusters(data)

    # Step 4: Plot signals
    plot_signals(data, ticker)


# Run the main function
if __name__ == "__main__":
    main()
