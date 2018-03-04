#!/usr/bin/env python

import signal
import sqlite3
import pandas as pd
from pubnub.callbacks import SubscribeCallback
from pubnub.pnconfiguration import PNConfiguration, PNReconnectionPolicy
from pubnub.pubnub_tornado import PubNubTornado
from tornado import gen


class BfAsyncSubscriber:
    def __init__(self, channels):
        self.channels = channels
        pnc = PNConfiguration()
        pnc.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
        pnc.reconnect_policy = PNReconnectionPolicy.LINEAR
        self.pubnub = PubNubTornado(pnc)

    @gen.coroutine
    def subscribe(self):
        return self.pubnub.subscribe().channels(self.channels).execute()


class BfSubscribeCallback(SubscribeCallback):
    def __init__(self, sqlite_path=None, quiet=False):
        self.db = sqlite3.connect(sqlite_path) if sqlite_path else None
        self.quiet = quiet

    def message(self, pubnub, message):
        if self.db:
            pd.DataFrame(
                [message.message]
            ).assign(
                timestamp=lambda d: pd.to_datetime(d['timestamp'])
            ).set_index(
                'timestamp'
            ).to_sql(
                name='ticker', con=self.db, if_exists='append'
            )
        if not self.quiet:
            print(message.message)


def stream_rate(products=['FX_BTC_JPY'], ch_type='ticker', sqlite_path=None,
                quiet=False):
    bas = BfAsyncSubscriber(
        channels=['lightning_{0}_{1}'.format(ch_type, p) for p in products]
    )
    bas.pubnub.add_listener(
        BfSubscribeCallback(sqlite_path=sqlite_path, quiet=quiet)
    )
    bas.subscribe()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    bas.pubnub.start()
