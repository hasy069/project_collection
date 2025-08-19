import requests

import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

import gc

#%%

session = requests.Session()  


def get_binance_minute_data(symbol: str, start_ms: str, end_ms: str, interval_time: str, mode: str) -> pd.DataFrame:
    
    if mode == "spot":
        url = "https://api.binance.com/api/v3/klines"
        addon_ = ""
        
    elif mode == "future":
        url = "https://fapi.binance.com/fapi/v1/klines"
        addon_ = "FUT_"
        
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
        
        frames.append(df.astype(float))
        #frames.append(df[['close']].astype(float).rename(columns={'close': addon_+symbol}))    Old Line above, only Close
        
        # advance start to one ms after last candle
        last_ts = int(data[-1][0])
        start_ms = last_ts + 60_000
        print(f"Requested {symbol} for the time startting {pd.to_datetime(start_ms, unit = 'ms')}")
        time.sleep(0.5)  # throttle to avoid API limits
    
    print(f"Bulk Downloaded for time-sub-index in {len(frames)} chunks")
    
    return pd.concat(frames) if frames else None

#%%

def get_total_data(start_: str, end_: str, tickers: list, mode = "spot") -> pd.DataFrame:
    
    if mode == "future":
        addon_ = "FUT_"
    else:
        addon_ = ""
        
    
    interval = "1m"
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
        print("----------------------------------------------------------------------------------------")
        print(f"Starting {symbol}")
        symbol_list = []

        while start < end_ms:
            current_end = min(current_end + 500 * 60 * 60 * 1000, end_ms)
            print(f"Fetching {symbol}, [{i} / {m}] from {pd.to_datetime(start, unit = 'ms')} to {pd.to_datetime(current_end, unit='ms')}")
            df_symbol = get_binance_minute_data(symbol, start, current_end, str(interval), mode)
            
            if df_symbol is not None:
                symbol_list.append(df_symbol)
        

            start = current_end 
            
            if current_end >= end_ms:
                break
        
        non_empty = [a for a in symbol_list if not a.empty ]
        print("Downloaded all data over time for: ", symbol)
        
        if non_empty:
            symbol_total = pd.concat(non_empty, axis = 0)
            symbol_total.index = pd.to_datetime(symbol_total.index)
            symbol_total = symbol_total[~symbol_total.index.duplicated(keep='first')]  # FIX HIER
            symbol_total.sort_index(inplace = True)
            symbol_total.to_csv(f"{symbol}.csv")
            symbol_closes = symbol_total["close"].rename({'close': addon_+symbol})
            
            all_dfs.append(symbol_closes)
        else:
            1 == 1
        print(f"Done for {symbol}")
        print("----------------------------------------------------------------------------------------")


    print("----------------------------------------------------------------------------------------")
    print("Done for all Symbls, dataframes will be merged and saved into one csv :) Have a happy Day")
    print("----------------------------------------------------------------------------------------")

        
    # merge on index (timestamp), outer join to keep all times
    if all_dfs:
        merged = pd.concat(all_dfs, axis=1)
        merged.sort_index(inplace=True)
        return merged
    else:
        return pd.DataFrame()


#%%

top_symbols = [
        "ETHUSDT"
    ]

top_symbols2 = [
    "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "SOLUSDT", "MATICUSDT", "DOTUSDT", "LINKUSDT",
    "LTCUSDT", "BCHUSDT", "TRXUSDT", "UNIUSDT", "AVAXUSDT",
    "FILUSDT", "NEARUSDT", "APTUSDT", "APEUSDT", "ARBUSDT",
    "MANAUSDT", "SANDUSDT", "ATOMUSDT", "OPUSDT", "FTMUSDT"
]

top_symbols3 = [
    "LTCUSDC"
]

top_100_pairs = [
    "BTCUSDT", "ETHUSDT", "XRPUSDT", "USDCUSDT", "BUSDUSDT",
    "ADAUSDT", "SOLUSDT", "DOGEUSDT", "MATICUSDT", "DOTUSDT",
    "TRXUSDT", "LINKUSDT", "UNIUSDT", "WBTCUSDT", "AVAXUSDT",
    "SHIBUSDT", "MANAUSDT", "SANDUSDT", "ATOMUSDT", "LUNA2USDT",
    "LTCUSDT", "NEARUSDT", "ICPUSDT", "FILUSDT", "FTTUSDT",
    "AAVEUSDT", "ALGOUSDT", "XLMUSDT", "AXSUSDT", "EOSUSDT",
    "THETAUSDT", "XTZUSDT", "VETUSDT", "HBARUSDT", "CHZUSDT",
    "FTMUSDT", "EGLDUSDT", "SUSHIUSDT", "CRVUSDT", "MKRUSDT",
    "SNXUSDT", "COMPUSDT", "KSMUSDT", "ZECUSDT", "BTTCUSDT",
    "MINAUSDT", "ENJUSDT", "GRTUSDT", "QNTUSDT", "KNCUSDT",
    "CELRUSDT", "RUNEUSDT", "ZILUSDT", "CAKEUSDT", "NEOUSDT",
    "DASHUSDT", "NEXOUSDT", "IOTAUSDT", "FLOWUSDT", "GALAUSDT",
    "AMPUSDT", "RVNUSDT", "CHSBUSDT", "MELEOUSDT", "ANKRUSDT",
    "EOSUSDT", "ARUSDT", "ONTUSDT", "TWTUSDT", "QTUMUSDT",
    "XEMUSDT", "BATUSDT", "HOTUSDT", "LSKUSDT", "SCRTUSDT",
    "OMGUSDT", "BCHUSDT", "STXUSDT", "ZRXUSDT", "1INCHUSDT",
    "YFIUSDT", "DCRUSDT", "XVGUSDT", "BTGUSDT", "NMRUSDT",
    "ICXUSDT", "KAVAUSDT", "ZENUSDT", "WAVESUSDT", "PAXGUSDT",
    "XVGUSDT", "ADXUSDT", "UMAUSDT", "BALUSDT", "ARPAUSDT",
    "RENUSDT", "NANOUSDT", "BTSUSDT", "SKLUSDT", "ONTUSDT"
]



short_data = top_symbols3

future = get_total_data("2025-07-01 00:00:00", "2025-07-10 00:00:00", top_symbols, "future")
future.to_csv("future_data.csv", index = True)

#spot = get_total_data("2025-06-01 00:00:00", "2025-07-10 00:00:00", short_data, "spot")
#spot.to_csv("spot_data.csv", index = True)

#df_merged = pd.read_csv("raw_data.csv")
#df_merged.head(3)

#%%

import matplotlib.pyplot as plt

spot_v = spot.values
future_v = future.values

plt.plot(future_v-spot_v)
plt.show()































