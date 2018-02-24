#!/usr/bin/env python

import signal
import time
import pybitflyer


def open_deal(config, n=10, interval=0, dry_run=False):
    api = pybitflyer.API(
        api_key=config['bF']['api_key'], api_secret=config['bF']['api_secret']
    )
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    for i in range(n):
        boards = {
            pc: api.board(product_code=pc) for pc in ['BTC_JPY', 'FX_BTC_JPY']
        }
        print(boards)
        if i == n - 1:
            break
        else:
            time.sleep(interval)
