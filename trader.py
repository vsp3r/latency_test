import asyncio
import queue
import time
RED = '\033[91m'
GREEN = '\033[92m'
RESET = '\033[0m'  # Reset the color

class AutoTrader:
    def __init__(self, coins, hl_queues, bin_queues):
        self.coins = coins
        self.hl_queues = hl_queues
        self.bin_queues = bin_queues
        self.running = True

        self.dex_books = {}
        self.bin_books = {}

    def start(self):
        try:
            asyncio.run(self.run()) 
        except Exception as e:
            # logging.error("Exception occurred", exc_info=e)
            self.shutdown()
            raise

    async def run(self):
        for coin in self.coins:
            self.dex_books[coin] = {"bid_px":0, "bid_sz":0, "ask_px":0, "ask_sz":0}
            self.bin_books[coin] = {"bid_px":0, "bid_sz":0, "ask_px":0, "ask_sz":0}

        while self.running:
            await asyncio.gather(
                self.process_queue(self.bin_queues, 1),
                self.process_queue(self.hl_queues, 0)
            )

    async def process_queue(self, msg_queue, exchange):
        try:
            msg=msg_queue.get_nowait()
            if exchange:
                await self.process_binance(msg)
            else:
                await self.process_hl(msg)
        except queue.Empty:
            await asyncio.sleep(0)

    
    async def process_binance(self, msg):
        _, coin, data, ts = msg
        bid_px = float(data["b"][0][0])
        bid_sz = float(data["b"][0][1])
        ask_px = float(data["a"][0][0])
        ask_sz = float(data["a"][0][1])
        self.bin_books[coin].update({
            "bid_px": bid_px,
            "bid_sz": bid_sz,
            "ask_px": ask_px,
            "ask_sz": ask_sz
        })
        self.bin_books[coin]['bp'] = (bid_px*ask_sz + ask_px*bid_sz) / (ask_sz+bid_sz)
        # self.bin_books[coin]['offset'] = self.dex_books[coin]['bp']/self.bin_books[coin]['bp']
        finish_time = time.perf_counter_ns()
        print(RED + f'BIN {coin[:4]}: Wire to wire: {(finish_time - ts) / 1000}us ({(finish_time - ts) / 10000000}ms)' + RESET)

        # if self.dex_books[coin]:
    async def process_hl(self, msg):
        _, coin, data, ts = msg
        levels = data['levels']
        bid_px = float(levels[0][0]['px'])
        bid_sz = float(levels[0][0]['sz']) 
        ask_px = float(levels[1][0]['px'])
        ask_sz = float(levels[1][0]['sz'])
        self.dex_books[coin].update({
            "bid_px": bid_px,
            "bid_sz": bid_sz,
            "ask_px": ask_px,
            "ask_sz": ask_sz
        })
        self.dex_books[coin]['bp'] = (bid_px*ask_sz + ask_px*bid_sz) / (ask_sz+bid_sz)
        # print(f'HYPERLIQUID {coin}: bid:{bid_px} ask:{ask_px}')
        finish_time = time.perf_counter_ns()
        print(GREEN + f'HYP {coin[:4]}: Wire to wire: {(finish_time - ts) / 1000}us ({(finish_time - ts) / 10000000}ms)' + RESET)


    async def shutdown(self):
        try:
            self.running = False
            print('STOPPING TRADER')
        except Exception as e:
            print(f'Error shutting down trader: {e}')
            raise
