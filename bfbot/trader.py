#!/usr/bin/env python

import logging
import signal
import numpy as np
import pandas as pd
from pubnub.callbacks import SubscribeCallback
import pybitflyer
from .streamer import BfAsyncSubscriber


class BfStreamTrader(SubscribeCallback):
    def __init__(self, pair, config, queue_length=10, quiet=False):
        self.pair = pair
        self.fx_pair = 'FX_{}'.format(pair)
        self.trade = config['trade']
        self.ql = queue_length
        self.quiet = quiet
        self.sfd_pins = np.array([- 0.2, - 0.15, - 0.1, 0.1, 0.15, 0.2])
        self.bF = pybitflyer.API(
            api_key=config['bF']['api_key'],
            api_secret=config['bF']['api_secret']
        )
        self.df = pd.DataFrame()
        self.tick = {}
        self.logger = logging.getLogger(__name__)

    def message(self, pubnub, message):
        if not self.tick:
            self._print('OPEN DEAL')
        self.df = (
            self.df if len(self.df) < self.ql else self.df.iloc[1:]
        ).append(
            pd.DataFrame(
                [message.message]
            ).assign(
                timestamp=lambda d: pd.to_datetime(d['timestamp'])
            ).set_index('timestamp')
        )
        self.tick[message.message['product_code']] = message.message
        if self.pair in self.tick and self.fx_pair in self.tick:
            self._trade()

    def _print(self, message):
        text = '>>>\t{}'.format(message)
        if self.quiet:
            self.logger.debug(text)
        else:
            print(text, flush=True)

    def _calc_deviation(self):
        mp = {
            k: (v['best_bid'] + v['best_ask']) / 2
            for k, v in self.tick.items() if k in [self.fx_pair, self.pair]
        }
        deviation = (mp[self.fx_pair] - mp[self.pair]) / mp[self.pair]
        self.logger.info('rate: {0}, deviation: {1}'.format(mp, deviation))
        abs_diff = abs(self.sfd_pins - deviation)
        sfd_near_dist = abs_diff.min()
        sfd_near_pin = self.sfd_pins[abs_diff == sfd_near_dist][0]
        self.logger.info('sfd_near_pin: {}'.format(sfd_near_pin))
        return deviation, sfd_near_pin, sfd_near_dist

    def _fetch_state(self):
        collateral = self.bF.getcollateral()
        self.logger.debug('collateral: {}'.format(collateral))
        keep_rate = collateral.get('keep_rate')
        self.logger.info('keep_rate: {}'.format(keep_rate))

        positions = self.bF.getpositions(product_code=self.fx_pair)
        self.logger.info('positions: {}'.format(positions))
        pos_sizes = {
            s: np.sum([p.get('size') for p in positions if p.get('side') == s])
            for s in ['SELL', 'BUY']
        }
        pos_size = max(pos_sizes.values())
        pos_side = (
            [k for k, v in pos_sizes.items() if v == pos_size][0]
            if pos_size > 0 else None
        )
        self.logger.info('pos_side: {0}, pos_size: {1}'.format(
            pos_side, pos_size
        ))
        return keep_rate, pos_side, pos_size

    def _trade(self):
        try:
            keep_rate, pos_side, pos_size = self._fetch_state()
        except Exception as e:
            self.logger.error(e)
            raise e
        else:
            deviation, sfd_near_pin, sfd_near_dist = self._calc_deviation()

        order_sides = (
            {'open': 'SELL', 'close': 'BUY'}
            if sfd_near_pin < deviation else
            {'open': 'BUY', 'close': 'SELL'}
        )
        mid_price = (
            lambda h: (h['best_bid'] + h['best_ask']) / 2
        )(self.tick[self.fx_pair])
        order_prices = {
            'limit': int(
                mid_price * (
                    1 + self.trade['sfd']['limit_spread'] * {
                        'BUY': - 1, 'SELL': 1
                    }[order_sides['open']]
                )
            ),
            'take_profit': int(
                mid_price * (
                    1 + self.trade['sfd']['take_profit'] * {
                        'BUY': 1, 'SELL': - 1
                    }[order_sides['open']]
                )
            ),
            'stop_loss': int(
                mid_price * (
                    1 + self.trade['sfd']['stop_loss'] * {
                        'BUY': - 1, 'SELL': 1
                    }[order_sides['open']]
                )
            )
        }

        if (
                sfd_near_dist <= self.trade['sfd']['max_dist_open'] and
                sfd_near_dist >= self.trade['sfd']['min_dist_open'] and
                pos_size <= self.trade['size']['max'] and
                (
                    pos_size == 0 or
                    pos_side != order_sides['open'] or
                    keep_rate >= self.trade['min_keep_rate']
                )
        ):
            try:
                order = (
                    self.bF.sendchildorder(
                        product_code=self.fx_pair,
                        child_order_type='MARKET',
                        side=order_sides['open'],
                        size=self.trade['size']['unit'],
                        minute_to_expire=self.trade['sfd']['minute_to_expire'],
                        time_in_force='IOC'
                    )
                    if pos_side and pos_side != order_sides['open'] else
                    self.bF.sendparentorder(
                        order_method='IFDOCO',
                        time_in_force='GTC',
                        minute_to_expire=self.trade['sfd']['minute_to_expire'],
                        parameters=[
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'LIMIT',
                                'side': order_sides['open'],
                                'price': order_prices['limit'],
                                'size': self.trade['size']['unit']
                            },
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'LIMIT',
                                'side': order_sides['close'],
                                'price': order_prices['take_profit'],
                                'size': self.trade['size']['unit']
                            },
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'STOP',
                                'side': order_sides['close'],
                                'trigger_price': order_prices['stop_loss'],
                                'size': self.trade['size']['unit']
                            }
                        ]
                    )
                )
            except Exception:
                return
            else:
                self.logger.debug(order)
                if (
                        not self.quiet and
                        ('status' not in order or order['status'] != - 205)
                ):
                    self._print(
                        '{0} {1} {2} with {3}'.format(
                            order_sides['open'],
                            self.trade['size']['unit'],
                            self.fx_pair,
                            (
                                'MARKET'
                                if pos_side and pos_side != order_sides['open']
                                else 'IFDOCO (IFD: {0} => OCO: {1})'.format(
                                    order_prices['limit'],
                                    sorted([
                                        order_prices['stop_loss'],
                                        order_prices['take_profit']
                                    ])
                                )
                            )
                        )
                    )
        else:
            self.logger.info('Skipped by the criteria.')


def open_deal(config, pair='BTC_JPY', quiet=False):
    bas = BfAsyncSubscriber(
        channels=[
            'lightning_ticker_{1}{2}'.format(p, pair) for p in ['FX_', '']
        ]
    )
    bas.pubnub.add_listener(
        BfStreamTrader(pair=pair, config=config, quiet=quiet)
    )
    bas.subscribe()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    bas.pubnub.start()
