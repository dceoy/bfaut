#!/usr/bin/env python

import logging
import signal
import sqlite3
import pandas as pd
from pubnub.callbacks import SubscribeCallback
from pubnub.pnconfiguration import PNConfiguration, PNReconnectionPolicy
from pubnub.pubnub_tornado import PubNubTornado
from tornado import gen


class BfSubscribeCallback(SubscribeCallback):
    def __init__(self, sqlite_path=None, queue_length=None, quiet=False):
        self.db = sqlite3.connect(sqlite_path) if sqlite_path else None
        self.ql = queue_length
        self.quiet = quiet
        self.df = pd.DataFrame()

    def message(self, pubnub, message):
        if self.ql or self.db:
            df_new = pd.DataFrame(
                [message.message]
            ).assign(
                timestamp=lambda d: pd.to_datetime(d['timestamp'])
            ).set_index('timestamp')
            if self.ql:
                self.df = (
                    self.df if len(self.df) < self.ql else self.df.iloc[1:]
                ).append(df_new)
            if self.db:
                df_new.to_sql(name='ticker', con=self.db, if_exists='append')
        if not self.quiet:
            print(message.message)


@gen.coroutine
def _subscribe_ch(channels, pubnub):
    pubnub.subscribe().channels(channels).execute()


def stream_rate(products=['FX_BTC_JPY'], sqlite_path=None, quiet=False):
    pnc = PNConfiguration()
    pnc.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
    pnc.reconnect_policy = PNReconnectionPolicy.LINEAR
    pubnub = PubNubTornado(pnc)

    channels = ['lightning_ticker_' + p for p in products]
    logging.debug('channels: {}'.format(channels))

    pubnub.add_listener(
        BfSubscribeCallback(sqlite_path=sqlite_path, quiet=quiet)
    )
    _subscribe_ch(channels=channels, pubnub=pubnub)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    pubnub.start()
