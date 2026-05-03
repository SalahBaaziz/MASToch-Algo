import backtrader as bt
import numpy as np
from datetime import datetime
import yfinance as yf  # Add yfinance import


class LinearRegressionOscillator(bt.Indicator):
    lines = ('linear_regression',)
    params = (('length', 20), ('upper', 1.5), ('lower', -1.5))


    def __init__(self):
        self.addminperiod(self.params.length)  # Ensure at least `length` bars are available

    def next(self):
        length = self.params.length

        # Ensure that there are at least `length` bars available
        if len(self.data) < length:
            return  # Skip calculation if not enough data

        # Try to extract the required close prices
        try:
            close_prices = np.array(self.data.close.get(size=length))

            # Ensure close_prices has enough data points
            if len(close_prices) < length:
                print(f"Skipping due to insufficient data. Got {len(close_prices)} but need {length}.")
                return

            x = np.arange(length)

            # Linear regression calculation
            sum_x = np.sum(x)
            sum_y = np.sum(close_prices)
            sum_xy = np.sum(x * close_prices)
            sum_x_squared = np.sum(x ** 2)

            n = length
            m = (n * sum_xy - sum_x * sum_y) / (n * sum_x_squared - sum_x ** 2)
            c = (sum_y - m * sum_x) / n

            # Linear regression oscillator value
            linear_regression_value = m * len(self.data) + c

            # Normalize linear regression
            if len(self.data) >= 100:
                sma = np.mean(self.data.close.get(size=100))
                stdev = np.std(self.data.close.get(size=100))
            else:
                sma = np.mean(self.data.close.get(size=len(self.data)))
                stdev = np.std(self.data.close.get(size=len(self.data)))

            normalized_lr = (linear_regression_value - sma) / stdev

            self.lines.linear_regression[0] = normalized_lr

        except Exception as e:
            print(f"Error in linear regression calculation: {e}")
            self.lines.linear_regression[0] = 0  # Default value in case of error


class Strategy(bt.Strategy):
    params = (('length', 20), ('upper', 1.5), ('lower', -1.5))

    def __init__(self):
        self.lro = LinearRegressionOscillator(self.data, length=self.params.length)
        self.buy_signal = False
        self.sell_signal = False

    def next(self):
        # Conditions for buy/sell based on the linear regression oscillator
        cond1 = self.lro.lines.linear_regression[0] < -10 and self.lro.lines.linear_regression[-1] > 0
        cond2 = self.lro.lines.linear_regression[0] > 10 and self.lro.lines.linear_regression[-1] < 0

        if cond2:
            if not self.position:
                self.sell()
        elif cond1:
            if self.position:
                self.buy()


# Setup backtesting
if __name__ == '__main__':
    cerebro = bt.Cerebro()

    # Add the strategy
    cerebro.addstrategy(Strategy)

    # Download data using yfinance
    data = yf.download('^NDX', start='2023-05-01', end='2023-10-10', interval='1h')

    # Convert data to Backtrader format
    data_feed = bt.feeds.PandasData(dataname=data)

    # Add data to Cerebro
    cerebro.adddata(data_feed)

    # Run backtest
    cerebro.run()
    cerebro.plot()
