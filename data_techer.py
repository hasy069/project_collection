import requests

import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

import gc

#%%

session = requests.Session()  
# all your session.get(...) calls will reuse the same TCP/TLS socket

def get_binance_minute_data(symbol: str, start_ms: str, end_ms: str, interval_time: str) -> pd.DataFrame:
    url = "https://api.binance.com/api/v3/klines"
    interval = interval_time
    frames = []
    
    while start_ms < end_ms:
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_ms,
            'endTime': end_ms,
            'limit': 1000
        }
        resp = requests.get(url, params=params)
        #print(symbol, resp.status_code, "→", len(resp.json()), "records")
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        
        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'trades',
            'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
        ])
        
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        
        frames.append(df[['close']].astype(float).rename(columns={'close': symbol}))
        
        # advance start to one ms after last candle
        last_ts = int(data[-1][0])
        start_ms = last_ts + 60_000
        print(f"Requested {symbol} for the time startting {pd.to_datetime(start_ms, unit = "ms")}")
        time.sleep(0.5)  # throttle to avoid API limits
    
    print(len(frames))
    
    if frames:
        return pd.concat(frames)
    else:
        return pd.DataFrame(columns=[symbol])

#%%

def get_top_20_minute_closes(start_: str, end_: str, tickers: list) -> pd.DataFrame:
    interval = "5m"
    all_dfs = []
    start = int(pd.to_datetime(start_).timestamp() * 1000)   #changed start_ms to start
    end_ms = int(pd.to_datetime(end_).timestamp() * 1000)

    #start = start_ms
    mid_ms = start + 500 * 15 * 60 * 1000
    current_end = mid_ms

    i = 0
    
    
    for symbol in tickers:
        start = int(pd.to_datetime(start_).timestamp() * 1000)   #changed start_ms to start
        current_end = start + 500 * 15 * 60 * 1000
        
        i += 1
        m = len(tickers)
        print(f"Starting {symbol}")
        symbol_list = []

        while start < end_ms:
            current_end = min(current_end + 500 * 60 * 60 * 1000, end_ms)
            print(f"Fetching {symbol}, [{i} / {m}] from {pd.to_datetime(start, unit = "ms")} to {pd.to_datetime(current_end, unit="ms")}")
            df_symbol = get_binance_minute_data(symbol, start, current_end, str(interval))
            symbol_list.append(df_symbol)
            start = current_end 
            
            if current_end >= end_ms:
                break
        
#hier fehlt der Block 
        non_empty = [a for a in symbol_list if not a.empty ]
        symbol_total = pd.concat(non_empty, axis = 0)
        symbol_total = symbol_total[~symbol_total.index.duplicated(keep='first')]  # FIX HIER
        symbol_total.sort_index(inplace = True)
        symbol_total.to_csv(f"{symbol}.csv")
        all_dfs.append(symbol_total)
        print(f"Done for {symbol}")

        
    # merge on index (timestamp), outer join to keep all times
    if all_dfs:
        merged = pd.concat(all_dfs, axis=1)
        # optionally sort the index
        merged.sort_index(inplace=True)
        return merged
    else:
        return pd.DataFrame()

#%%

            non_empty = [a for a in symbol_list if not a.empty ]
            symbol_total = pd.concat(non_empty, axis = 0)
            symbol_total = symbol_total[~symbol_total.index.duplicated(keep='first')]  # FIX HIER
            symbol_total.sort_index(inplace = True)
            symbol_total.to_csv(f"{symbol}.csv")
            all_dfs.append(symbol_total)
            print(all_dfs)
            print(f"Done for {symbol}")
#%%

top_symbols = [
        "BNBUSDT"
    ]

top_symbols2 = [
    "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "SOLUSDT", "MATICUSDT", "DOTUSDT", "LINKUSDT",
    "LTCUSDT", "BCHUSDT", "TRXUSDT", "UNIUSDT", "AVAXUSDT",
    "FILUSDT", "NEARUSDT", "APTUSDT", "APEUSDT", "ARBUSDT",
    "MANAUSDT", "SANDUSDT", "ATOMUSDT", "OPUSDT", "FTMUSDT"
]

top_symbols3 = [
    "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT"
]


short_data = top_symbols3

df_merged = get_top_20_minute_closes("2025-07-01 00:00:00", "2025-07-10 00:00:00", short_data)
df_merged.to_csv("raw_data.csv", index = True)

#df_merged = pd.read_csv("raw_data.csv")
#df_merged.head(3)

#%%

