import numpy as np
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
from datetime import datetime


class SalahLSStrategy:
    def __init__(self, ticker="^NDX", length=20, upper=1.5, lower=-1.5, ma_length=6):
        self.ticker = ticker
        self.length = length
        self.upper = upper
        self.lower = lower
        self.ma_length = ma_length
        self.data = self.fetch_data()
        self.data['linear_regression'] = self.calculate_linear_regression()
        self.data['oscillator'] = self.normalize_oscillator()
        self.data['moving_avg'] = self.calculate_moving_average()
        self.generate_signals()

    def fetch_data(self):
        # Fetch 30-minute interval data for the past month
        end = datetime.now()
        start = end - pd.Timedelta(days=30)
        data = yf.download(self.ticker, start=start, end=end, interval="30m")
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
        data.columns = data.columns.str.lower()
        return data

    def calculate_linear_regression(self):
        # Calculate linear regression based on historical close prices for given length
        x = np.arange(self.length)
        lr_values = []

        for i in range(self.length, len(self.data)):
            y = self.data['close'].iloc[i - self.length:i].values
            slope, intercept = np.polyfit(x, y, 1)
            regression = (slope * i + intercept) * -1
            lr_values.append(regression)

        # Fill initial NA values to maintain dataframe length consistency
        lr_values = [np.nan] * self.length + lr_values
        return pd.Series(lr_values, index=self.data.index)

    def normalize_oscillator(self):
        # Normalize the linear regression values for oscillator
        sma = self.data['linear_regression'].rolling(window=100).mean()
        stdev = self.data['linear_regression'].rolling(window=100).std()
        return (self.data['linear_regression'] - sma) / stdev

    def calculate_moving_average(self):
        # Simple Moving Average over 'oscillator'
        return self.data['oscillator'].rolling(window=self.ma_length).mean()

    def generate_signals(self):
        # Generate buy and sell signals based on oscillator crossing zero and moving average
        self.data['signal'] = 0
        self.data.loc[(self.data['oscillator'] > 0) & (self.data['moving_avg'] > 0), 'signal'] = 1
        self.data.loc[(self.data['oscillator'] < 0) & (self.data['moving_avg'] < 0), 'signal'] = -1

        # Capture the price at buy/sell signal points
        self.data['buy_price'] = np.where(self.data['signal'] == 1, self.data['close'], np.nan)
        self.data['sell_price'] = np.where(self.data['signal'] == -1, self.data['close'], np.nan)

    def plot_strategy(self):
        plt.figure(figsize=(14, 10))

        # Plot the closing price with buy/sell signals
        plt.subplot(2, 1, 1)
        plt.plot(self.data['close'], color='black', label='NDX Price')
        plt.scatter(self.data.index, self.data['buy_price'], color='green', marker='^', alpha=1, label='Buy Signal')
        plt.scatter(self.data.index, self.data['sell_price'], color='red', marker='v', alpha=1, label='Sell Signal')
        plt.title(f"{self.ticker} Price with Buy/Sell Signals")
        plt.xlabel("Date")
        plt.ylabel("Price")
        plt.legend(loc='best')

        # Plot the oscillator and moving average
        plt.subplot(2, 1, 2)
        plt.plot(self.data['oscillator'], color='blue', label='Oscillator')
        plt.plot(self.data['moving_avg'], color='yellow', label='Moving Average of Oscillator')
        plt.axhline(y=self.upper, color='green', linestyle='--', label='Upper Threshold')
        plt.axhline(y=self.lower, color='red', linestyle='--', label='Lower Threshold')
        plt.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        plt.legend(loc='best')
        plt.xlabel("Date")
        plt.ylabel("Oscillator Value")
        plt.title("Salah L/S Strategy Oscillator & Moving Average")

        plt.tight_layout()
        plt.show()


# Run the strategy
strategy = SalahLSStrategy()
strategy.plot_strategy()
