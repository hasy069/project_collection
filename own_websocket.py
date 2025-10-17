import asyncio
import nest_asyncio

import json
from collections import deque
from datetime import datetime

import threading
import time

from urllib.parse import urlencode
import websockets

import hmac
import hashlib

nest_asyncio.apply()

#%%
#creates the class to create multiple listeners later on

class BinanceMarketData:
    def __init__(self, symbols, max_que = 100):                             #initializes the Function
        self.symbols = [symbol.lower() for symbol in symbols]               #Lower all tickers to make them the right format
        self.websocket_url = "wss://stream.binance.com:9443/ws/"            #Base URL, chnage for Futures data
        self.latest_prices = {}   
        self.data_collection = {}#dict of latest prices added 
        self.price_que = deque(maxlen=max_que)  
        self.is_running = False                                             #Base case status for the class
        self.websocket = None
        self.min = 1                                                        #The Interval in Minutes of data collection
        
        
        for symbol in self.symbols:
            self.latest_prices[symbol] = {                                  #creates an object for each ticker to later append on
                "price": None,
                "timestamp": None,
                "symbol": symbol.upper(),
                }
            self.data_collection[symbol] = deque(maxlen=10000)
        
    
    def write_stream_name(self):                                            #creates the Full URL to suscribe to
        streams = [f"{symbol}@ticker" for symbol in self.symbols]
        return "/".join(streams)
    
    
    async def connect_websocket(self):
        stream_name = self.write_stream_name()
        full_url = self.websocket_url + stream_name
        
        print("Connecting to: ", full_url)
        print(f"Connecting to Symbols: {[symbol.upper() for symbol in self.symbols]}", )
        
        try:
            self.websocket = await websockets.connect(full_url)             #Waits for connection; may expand on error handeling
            print("Connected successfully")
            return True
        
        except Exception as e:
            print("Error occured: ", e)
            return False
    
    
    
    def process_ticker_data(self, data):
        try:
            symbol = data.get("s", " ").lower()
            current_price = float(data.get("c", 0))
            timestamp = data.get("E", 0)
            
            readable_time = datetime.fromtimestamp(timestamp / 1000).strftime("%H:%M:%S.%f")[:-3]
            

            #Adds to the dictionary with tickers a Dict with the data for each tick recived, may build up backlog to long
         
            if symbol in self.latest_prices:                         
                self.latest_prices[symbol] = {
                    "symbol": symbol.upper(),
                    "close_price": float(data.get("c", 0)),
                    "time_index": data.get("E", 0),
                    "VWAP": float(data.get("w", 0)),
                    "Volume": float(data.get("Q", 0)),
                    "24h Volume": float(data.get("v", 0)),
                    "ask_price": float(data.get("a", 0)),
                    "ask_Vol": float(data.get("A", 0)),
                    "bid_price": float(data.get("b", 0)),
                    "bid_Vol": float(data.get("B", 0)),
                    }
            
            #collects que of latest prices, may be useless and performance slowing
                self.price_que.append({
                    "symbol": symbol.upper(),
                    "price": current_price,
                    "times": readable_time,
                    "time_index": timestamp})
                
        except Exception as e:
            print(f"Error Occured: {e}")
        
        
        
    async def listen_to_stream(self):
        try:
            while self.is_running and self.websocket:
                message = await self.websocket.recv()
                data = json.loads(message)
                self.process_ticker_data(data)
                
        except websockets.exceptions.ConnectionClosed:
            print("Connection Closed: Unknown Reason")
            
        except Exception as e:
            print(f"An Error occured: {e}")
    
    #Newest added Module, scans for a new minute and adds every symbol into a new que (minute data)
    async def collect_time_data(self):
        divisor = self.min * 60 * 1000      # Expands defined Minute Interval >> Seconds >> Microseconds
        await asyncio.sleep(60*1)           # Wait for initial data buildup (delay in seconds)
        
        last_recorded_minute = 0  # Track which minute we last recorded
        
        try:
            while self.is_running:
                current_time = int(time.time() * 1000)
                current_minute = current_time // divisor  # if we divide by the Interval, if the Rest changes, we have a new minute
                
                # New minute detected!
                if current_minute > last_recorded_minute:
                    for symbol in self.symbols:
                        self.data_collection[symbol].append(self.latest_prices[symbol])
                    last_recorded_minute = current_minute
                    print(f"Collected data for minute {current_minute}")
                
                await asyncio.sleep(0.1)  # Check every 100ms, don't burn CPU
                
        except Exception as e:
            print(f"An error occurred: {e}")
            
          
            
          
    async def start_streaming(self):
        self.is_running = True
        
        if await self.connect_websocket():
            print("Connected succesfully")
            await asyncio.gather(self.listen_to_stream(), self.collect_time_data())
            # Co Runs the listen to stream 
            
        else:                                   
            print("Could not Connect")
         
            
         
    def stop_streaming(self):
        self.is_running = False
        if self.websocket:
            asyncio.create_task(self.websocket.close())
        print("Streaming Stopped")
        
        
    def get_latest_price(self):
        return self.latest_prices.copy()
    
    
    def get_que(self):
        return list(self.data_collection)
    


#%%

def run_market_data_stream_threaded():
    symbols_to_track = ["BTCUSDT"]
    market_data = BinanceMarketData(symbols_to_track, max_que=50)
    
    def run_async():
        try:
            asyncio.run(market_data.start_streaming())
        except KeyboardInterrupt:
            market_data.stop_streaming()
        except Exception as e:
            print(f"Error occured: {e}")
    
    # Start the websocket in a separate thread
    thread = threading.Thread(target=run_async, daemon=True)
    thread.start()
    
    return market_data

market_data = run_market_data_stream_threaded()

#%%

x = market_data.data_collection

x

#%%


import pandas as pd

df = pd.DataFrame(x["btcusdt"])
df

df.to_csv("btc_second_data.csv")





#%%

import nest_asyncio
import time

class BinanceOrderWebsocket:
    def __init__(self, spot = False):
        self.api_key = "SZpsTUV0Z8rfaum9frbZKraIafxUld456MNgPYFCVMPepe5lxIr3yZNYBreJYmbG"
        self.api_secret = "4q0ZUkAZh7hdTHPQL2WbLwUP3E1QXoIgsRN5VkM2TtJaOnOuCa8N3L3TBzXj7Cg8"
        self.spot = spot
        
        
        if self.spot:
            self.websocket_url = "wss://ws-api.binance.com:443/ws-api/v3"
        else:
            # Futures testnet endpoint
            self.websocket_url = "wss://testnet.binancefuture.com/ws-api/v3"
            
        self.websocket = None
        self.is_running = False
        self.order_responses = deque(maxlen=100)  # FIXED: Added maxlen parameter
        self.request_id = 0
        self.loop = None
        self.thread = None  # FIXED: Changed Thread to thread (lowercase)
        
    def _generate_signature(self, params):
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256).hexdigest()
        return signature
    
    async def connect_websocket(self):
        market_type = "SPOT" if self.spot else "FUTURES TESTNET"
        print(f"Connecting to Websocket of type {market_type}")
        
        try: 
            self.websocket = await websockets.connect(self.websocket_url)
            print("Connected succesfully to Websocket for Orders")
            return True
        
        except Exception as e:
            print(f"An Error Occured for the Orders Websocket: {e}")
            return False
        
    async def _send_orders_async(self, order_params):
        try:
            if not self.websocket or not self.is_running:
                print("Error Occured, no Connection")
                return None
            
            order_params["timestamp"] = int(time.time() * 1000)
            order_params["signature"] = self._generate_signature(order_params)
            
            self.request_id += 1
            
            message = {
                "id": f"order_{self.request_id}",
                "method": "order.place",
                "params": order_params
                }
            
            await self.websocket.send(json.dumps(message))
            print(f"Order Sent of ID: {message['id']}")
            
            resp = await self.websocket.recv()
            resp_data = json.loads(resp)
            
            self.order_responses.append(resp_data)
            print(f"Order Respone: {resp_data}")
            return resp_data
        
        except Exception as e:
            print(f"Error occured sending orders: {e}")
            
    
    async def _listen_loop(self):
        while self.is_running and self.websocket:
            try:
                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"An Error occured in the Connection of Oders WS: {e}")
                break
            
    async def _run_async(self):
        if await self.connect_websocket():
            await self._listen_loop()
            
    def start(self):
        if self.is_running:
            print("Websocket is already Running")
            return None
        
        self.is_running = True
        
        def run_in_thread():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._run_async())
        
        self.thread = threading.Thread(target=run_in_thread, daemon=True)  # FIXED: Removed ()
        self.thread.start()
        
        time.sleep(1)
        print("Websocket started in a new Thread")
        
        
        
    def stop(self):
        self.is_running = False
        if self.websocket and self.loop:
            asyncio.run_coroutine_threadsafe(self.websocket.close(), self.loop)
        print("Order Webscoket Stopped and Closed")
    
    
    
    def send_market_order(self, symbol, side, quantity):
        order_params = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": "MARKET",
            "quantity": quantity,
            "apiKey": self.api_key
            }
        if not self.loop:
            print("No Websocket for Orders is currentl running")
            return None
        
        future = asyncio.run_coroutine_threadsafe(
            self._send_orders_async(order_params), self.loop)  # FIXED: Changed to _send_orders_async
        
        return future
    
    
    
    def send_limit_order(self, symbol, side, quantity, price, time_in_force = "GTC"):
        
        order_params = {
            'symbol': symbol.upper(),
            'side': side.upper(),
            'type': 'LIMIT',
            'quantity': quantity,
            'price': price,
            'timeInForce': time_in_force,
            'apiKey': self.api_key
        }
        
        if not self.loop:
            print("No Websocket for Orders is currentl running")
            return None
        
        future = asyncio.run_coroutine_threadsafe(
            self._send_orders_async(order_params), self.loop)
        
        return future
    
    
#%%


client = BinanceOrderWebsocket()
client.start()

#%%

client.send_market_order("BTCUSDC", "BUY", 0.01)











