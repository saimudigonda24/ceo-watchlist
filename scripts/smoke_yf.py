import sys, yfinance as yf
t = (sys.argv[1] if len(sys.argv)>1 else "AAPL").upper()
info = yf.Ticker(t).info
print("name:", info.get("longName") or info.get("shortName"))
print("website:", info.get("website"))
print("price:", yf.Ticker(t).history(period="1d")["Close"].iloc[-1])