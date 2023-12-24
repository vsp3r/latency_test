import asyncio
import os
import multiprocessing
import sys
import signal
import json

from hl_ws import HyperliquidConnector
from bin_ws import BinanceConnector
from trader import AutoTrader

CONFIG_FILE = 'config.json'

def config_parse(config_file):
    config_path = os.path.join(os.path.dirname(__file__), config_file)
    with open(config_path) as f:
        return json.load(f)

def start_ws(connector, coins, queues):
    try:
        c = connector(coins, queues)
        c.start()
    except SystemExit:
        asyncio.run(c.shutdown())
    except Exception as e:
        # logging.error("Exception occurred", exc_info=e)
        asyncio.run(c.shutdown())

def start_trader(trader_obj, coins, hl_queues, bin_queues):
    try:
        at = trader_obj(coins, hl_queues, bin_queues)
        at.start()
    except SystemExit:
        asyncio.run(at.shutdown())
    except Exception as e:  
        # logging.error("Exception occurred", exc_info=e)
        asyncio.run(at.shutdown())

def shutdown_handler(signum, frame):
    raise SystemExit("Shutdown signal received")

def main():
    config = config_parse(CONFIG_FILE)
    print(config)
    coins = config['Coins']
    coins_per_feed = 10

    # hl_queues = {coin:multiprocessing.Queue() for coin in coins}
    # binance_queues = {coin:multiprocessing.Queue() for coin in coins}
    hl_queues = multiprocessing.Queue()
    binance_queues = multiprocessing.Queue()

    ws_processes = []
    trader_processes = []

    # bin_ws = multiprocessing.Process(target=start_ws, args=(BinanceConnector, coins, binance_queues))
    # bin_ws.start()
    # ws_processes.append(bin_ws)

    hl_ws = multiprocessing.Process(target=start_ws, args=(HyperliquidConnector, coins, hl_queues))
    hl_ws.start()
    ws_processes.append(hl_ws)

    trader_p = multiprocessing.Process(target=start_trader, args=(AutoTrader, coins, hl_queues, binance_queues))
    trader_p.start()
    trader_processes.append(trader_p)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        for p in ws_processes + trader_processes:
            p.join()
    except (SystemExit, KeyboardInterrupt):
        print('Shutdown signal recieved')
        # Send a signal to child processes to terminate
        for p in ws_processes + trader_processes:
            if p.is_alive():
                os.kill(p.pid, signal.SIGTERM)
                p.join(timeout=5)  # Give it a chance to terminate
                if p.is_alive():
                    p.kill()
                    p.join()



if __name__ == '__main__':
    if sys.platform == "darwin":
        multiprocessing.set_start_method("spawn")
    main()
    