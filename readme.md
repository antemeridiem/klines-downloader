# Klines downloader

Downloads klines history from Binance cryptocurrency exchange

Define needed parameters in the `config.yaml` file and run `python/run.py` file:
- interval - klines interval to download data for, as of the moment of writing, the following intervals are available from Binance and are supported:
    - 1s
    - 1m
    - 3m
    - 5m
    - 15m
    - 30m
    - 1h
    - 2h
    - 4h
    - 6h
    - 8h
    - 12h
    - 1d
    - 3d
    - 1w
- quotes - an array of quotes to download data for (BTC, USDT, etc)
- spot_only - if True, download only pairs for which spot trading is allowed
- tradable_only - if True, download only pairs that are currently being traded
- limit_requests - how many requests does the Binanace API allow to make over the limit_period
- limit_period - the length of the period over which the Binance API counts request within the defined limit
