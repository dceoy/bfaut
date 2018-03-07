#!/usr/bin/env python

from datetime import datetime, timedelta
import logging
import signal
import numpy as np
import pandas as pd
from pubnub.callbacks import SubscribeCallback
import pybitflyer
from .streamer import BfAsyncSubscriber


class BfStreamTrader(SubscribeCallback):
    def __init__(self, pair, config, wait=0, quiet=False):
        self.pair = pair
        self.fx_pair = 'FX_{}'.format(pair)
        self.trade = config['trade']
        self.wait = float(wait)
        self.quiet = quiet
        self.sfd_pins = np.array([0.1, 0.15, 0.2])
        self.bF = pybitflyer.API(
            api_key=config['bF']['api_key'],
            api_secret=config['bF']['api_secret']
        )
        self.start_datetime = None
        self.weighted_volumes = None
        self.prefix_msg = None
        self.logger = logging.getLogger(__name__)

    def message(self, pubnub, message):
        new_volumes = pd.DataFrame(
            message.message
        )[['side', 'size']].append(
            pd.DataFrame({'side': ['BUY', 'SELL'], 'size': [0, 0]})
        ).groupby('side')['size'].sum()
        if self.start_datetime:
            self.weighted_volumes = (
                self.trade['volume']['ewma_alpha'] * new_volumes +
                (1 - self.trade['volume']['ewma_alpha']) *
                self.weighted_volumes
            )
            self.prefix_msg = '[ BUY: {0:.2f}, SELL: {1:.2f} ]'.format(
                self.weighted_volumes['BUY'], self.weighted_volumes['SELL']
            )
            time_left = (
                self.start_datetime + timedelta(seconds=self.wait) -
                datetime.now()
            )
            if time_left < timedelta(seconds=0):
                volume_diff = abs(np.diff(self.weighted_volumes)[0])
                self.logger.info('volume_diff: {}'.format(volume_diff))
                if volume_diff > self.trade['volume']['min_diff']:
                    self._trade()
                else:
                    self._print(
                        'Skip by volume balance. '
                        '(EWMA difference: {:.6f})'.format(volume_diff)
                    )
            else:
                self.logger.info('time_left: {}'.format(time_left))
            self.prefix_msg = None
        else:
            self.start_datetime = datetime.now()
            self.weighted_volumes = new_volumes
            self._print('Wait for loading...')

    def _print(self, message, prompt='>>>'):
        text = '\t'.join([s for s in [prompt, self.prefix_msg, message] if s])
        if self.quiet:
            self.logger.info(text)
        else:
            print(text, flush=True)

    def _fetch_state(self):
        collateral = self.bF.getcollateral()
        self.logger.debug(collateral)
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

        tickers = {
            p: self.bF.ticker(product_code=p)
            for p in [self.pair, self.fx_pair]
        }
        self.logger.debug(tickers)
        return keep_rate, pos_side, pos_size, tickers

    def _calc_sfd_dist(self, tickers):
        mp = {
            k: (v['best_bid'] + v['best_ask']) / 2
            for k, v in tickers.items() if k in [self.fx_pair, self.pair]
        }
        deviation = (mp[self.fx_pair] - mp[self.pair]) / mp[self.pair]
        self.logger.info('rate: {0}, deviation: {1}'.format(mp, deviation))
        sfd_near_dist = np.abs(self.sfd_pins - abs(deviation)).min()
        self.logger.info('sfd_near_dist: {}'.format(sfd_near_dist))
        return sfd_near_dist

    def _design_order(self, tickers):
        open_side = self.weighted_volumes.idxmax()
        order_sides = {
            'open': open_side,
            'close': {'BUY': 'SELL', 'SELL': 'BUY'}[open_side]
        }
        self.logger.info('order_sides: {}'.format(order_sides))
        open_price = tickers[self.fx_pair][
            {'BUY': 'best_ask', 'SELL': 'best_bid'}[open_side]
        ]
        order_prices = {
            'limit': int(
                open_price * (
                    1 + self.trade['order']['limit_spread'] * {
                        'BUY': - 1, 'SELL': 1
                    }[open_side]
                )
            ),
            'take_profit': int(
                open_price * (
                    1 + self.trade['order']['take_profit'] * {
                        'BUY': 1, 'SELL': - 1
                    }[open_side]
                )
            ),
            'stop_loss': int(
                open_price * (
                    1 + self.trade['order']['stop_loss'] * {
                        'BUY': - 1, 'SELL': 1
                    }[open_side]
                )
            )
        }
        self.logger.info('order_prices: {}'.format(order_prices))
        return order_sides, order_prices

    def _trade(self):
        try:
            keep_rate, pos_side, pos_size, tickers = self._fetch_state()
        except Exception as e:
            self.logger.error(e)
            return
        else:
            order_sides, order_prices = self._design_order(tickers=tickers)
            sfd_near_dist = self._calc_sfd_dist(tickers=tickers)

        if sfd_near_dist < self.trade['skip_sfd_dist']:
            self._print(
                'Skip by sfd boundary. '
                '(distance: {:.6f})'.format(sfd_near_dist)
            )
        elif (
            pos_side == order_sides['open'] and
            keep_rate < self.trade['min_keep_rate']
        ):
            self._print(
                'Skip by margin retention rate. '
                '(current rate: {:.6f})'.format(keep_rate)
            )
        else:
            try:
                order = (
                    self.bF.sendchildorder(
                        product_code=self.fx_pair,
                        child_order_type='MARKET',
                        side=order_sides['open'],
                        size=self.trade['unit_size'],
                        time_in_force='IOC'
                    )
                    if pos_side and pos_side != order_sides['open'] else
                    self.bF.sendparentorder(
                        order_method='IFDOCO',
                        time_in_force='GTC',
                        parameters=[
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'LIMIT',
                                'side': order_sides['open'],
                                'price': order_prices['limit'],
                                'size': self.trade['unit_size']
                            },
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'LIMIT',
                                'side': order_sides['close'],
                                'price': order_prices['take_profit'],
                                'size': self.trade['unit_size']
                            },
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'STOP',
                                'side': order_sides['close'],
                                'trigger_price': order_prices['stop_loss'],
                                'size': self.trade['unit_size']
                            }
                        ]
                    )
                )
            except Exception as e:
                self.logger.error(e)
                return
            else:
                self.logger.debug(order)
                if order.get('status') != - 205:
                    self._print(
                        '{0} {1} {2} with {3}.'.format(
                            order_sides['open'], self.trade['unit_size'],
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


def open_deal(config, pair='BTC_JPY', wait=0, quiet=False):
    bas = BfAsyncSubscriber(
        channels=['lightning_executions_FX_{}'.format(pair)]
    )
    bas.pubnub.add_listener(
        BfStreamTrader(pair=pair, config=config, wait=wait, quiet=quiet)
    )
    bas.subscribe()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if not quiet:
        print('>>>\tOPEN DEAL !!!')
    bas.pubnub.start()
