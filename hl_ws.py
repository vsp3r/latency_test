import websockets
from websockets.exceptions import ConnectionClosedError
import asyncio
import orjson
import os
import time
import signal
from queue import Full
from async_logger import AsynchronousLogger
RED = '\033[91m'
GREEN = '\033[92m'
RESET = '\033[0m'  # Reset the color

class HyperliquidConnector:
    def __init__(self, symbols, queues):
        self.symbols = symbols
        self.queues = queues
        self.running = True
        self.counter = 0

        self.ws_url = 'wss://api.hyperliquid.xyz/ws'
        self.exchange = 'HYPERLIQUID'


    def start(self):
        """Starts Hyperliquid websocket. In charge of doing its job, and reporting errors"""
        self.logger = AsynchronousLogger('hl_connector.log')
        self.logger.log("timestamp,coin,exch_to_server,msg_to_process")
        asyncio.run(self.run())

    async def run(self):
        await self.connect()

    async def connect(self):
        # print('start hl connect')
        async with websockets.connect(self.ws_url) as ws:
            self.ws = ws
            # try:
            await asyncio.gather(*(self.subscribe(ws, coin)
                                for coin in self.symbols))
        
            while self.running:
                message = await ws.recv()
                self.counter +=1
                air_time = time.time() * 1_000_000
                ts = time.perf_counter_ns()
                asyncio.create_task(self.process_data(message, air_time, ts))
            # except KeyboardInterrupt:
            #     await self.shutdown(ws)
            # except Exception as e:
            #     print(f'{e}')
                
            # await self.shutdown(ws)
        
    async def subscribe(self, ws, coin):
        subscription_message = {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin":coin}
        }
        await ws.send(orjson.dumps(subscription_message).decode('utf-8'))

    async def process_data(self, message, air_time, ts):
        t2 = time.perf_counter_ns()
        data = orjson.loads(message)
        try:
            if 'channel' in data:
                coin = data['data']['coin']
                self.queues.put_nowait(('hyperliquid', coin, data['data'], ts))
                exch_ts = data['data']['time'] * 1000
                print(GREEN + f'[HYP {coin[:4]}] {self.counter}: Exch to server: {air_time - exch_ts}us ({(air_time-exch_ts)/1000}ms). Msg to process: {(t2 - ts)/1000}us' + RESET)
                self.logger.log(f"{exch_ts},{coin},{air_time - exch_ts},{(t2-ts)/1000}")


            else:
                pass
        except Exception as e:
            print(f"(HL WS) {self.symbols} ERROR PROCESSING MESSAGE: {e}\nMessage: {message}")
            


    async def shutdown(self):
        try:
            await self.logger.stop()
            await asyncio.gather(*(self.unsubscribe(self.ws, coin)
                                    for coin in self.symbols))
            await self.ws.close()
        except ConnectionClosedError:
            pass
        
    async def unsubscribe(self, ws, coin):
        subscription_message = {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin":coin}
        }
        unsub = {
            "method": "unsubscribe",
            "subscription": subscription_message
        }
        await ws.send(orjson.dumps(unsub).decode('utf-8'))

