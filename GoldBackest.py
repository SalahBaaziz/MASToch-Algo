import yfinance as yf
import pandas as pd

nas100 = yf.download(tickers="^NDX", period="730d", interval="1h")

nas100.to_csv("Hello")