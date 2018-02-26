#!/usr/bin/env python

import logging
import signal
from pubnub.callbacks import SubscribeCallback
from pubnub.pnconfiguration import PNConfiguration, PNReconnectionPolicy
from pubnub.pubnub_tornado import PubNubTornado
from tornado import gen


class BfSubscribeCallback(SubscribeCallback):
    def message(self, pubnub, message):
        print(message.message)


@gen.coroutine
def _subscribe_ch(channels, pubnub):
    pubnub.subscribe().channels(channels).execute()


def stream_rate(products=['FX_BTC_JPY']):
    pnc = PNConfiguration()
    pnc.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
    pnc.reconnect_policy = PNReconnectionPolicy.LINEAR
    pubnub = PubNubTornado(pnc)

    channels = ['lightning_ticker_' + p for p in products]
    logging.debug('channels: {}'.format(channels))

    pubnub.add_listener(BfSubscribeCallback())
    _subscribe_ch(channels=channels, pubnub=pubnub)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    pubnub.start()
