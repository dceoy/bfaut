#!/usr/bin/env python

from datetime import datetime, timedelta
import logging
import os
import signal
import numpy as np
import pandas as pd
from pubnub.callbacks import SubscribeCallback
import pybitflyer
from .info import BfAsyncSubscriber
from .util import BfbotError, dump_yaml


class BfStreamTrader(SubscribeCallback):
    def __init__(self, pair, config, timeout, ifdoco=False, quiet=False):
        self.pair = pair
        self.fx_pair = 'FX_{}'.format(pair)
        self.trade = config['trade']
        self.timeout_delta = timedelta(seconds=int(timeout))
        self.ifdoco = ifdoco
        self.quiet = quiet
        self.n_to_load = 10
        self.sfd_pins = np.array([0.1, 0.15, 0.2])
        self.bF = pybitflyer.API(
            api_key=config['bF']['api_key'],
            api_secret=config['bF']['api_secret']
        )
        self.start_datetime = None
        self.vd_ewm = None
        self.vd_bollinger = None
        self.prefix_msg = None
        self.reserved_side = None
        self.reserved_size = None
        self.order_datetime = None
        self.last_open_size = None
        self.last_collat = None
        self.logger = logging.getLogger(__name__)

    def message(self, pubnub, message):
        volumes = pd.DataFrame(
            message.message
        )[['side', 'size']].append(
            pd.DataFrame({'side': ['BUY', 'SELL'], 'size': [0, 0]})
        ).groupby('side')['size'].sum()
        self.prefix_msg = {'VOLUME': {k: v for k, v in volumes.items()}}
        volume_diff = volumes['BUY'] - volumes['SELL']
        if self.vd_ewm is None:
            self.vd_ewm = {
                'mean': volume_diff, 'var': np.square(volume_diff)
            }
            self._print('Wait for loading...')
            self.n_to_load -= 1
        else:
            self.vd_ewm = {
                'mean': (
                    self.trade['volume_ewm']['alpha'] * volume_diff +
                    (1 - self.trade['volume_ewm']['alpha']) *
                    self.vd_ewm['mean']
                ),
                'var': (
                    (1 - self.trade['volume_ewm']['alpha']) * (
                        self.vd_ewm['var'] +
                        self.trade['volume_ewm']['alpha'] *
                        np.square(volume_diff - self.vd_ewm['mean'])
                    )
                )
            }
            self.logger.info(
                'self.vd_ewm: {}'.format(self.vd_ewm)
            )
            self.vd_bollinger = (
                np.array([- 1, 1]) *
                self.trade['volume_ewm']['trigger_sigma'] +
                np.sqrt(self.vd_ewm['var']) *
                self.vd_ewm['mean']
            )
            if self.n_to_load == 0:
                self._trade()
            else:
                self.n_to_load -= 1
                self.logger.info('self.n_to_load: {}'.format(self.n_to_load))

    def _print(self, message, prompt='>>>'):
        text = '\t'.join([s for s in [prompt, self.prefix_msg, message] if s])
        if self.quiet:
            self.logger.info(text)
        else:
            print(text, flush=True)

    def _fetch_state(self):
        collateral = self.bF.getcollateral()
        if isinstance(collateral, dict):
            self.logger.debug(collateral)
            self.logger.info('collateral: {}'.format(collateral))
        else:
            raise BfbotError(collateral)

        positions = self.bF.getpositions(product_code=self.fx_pair)
        if isinstance(positions, list):
            self.logger.info('positions: {}'.format(positions))
            pos_sizes = {
                s: np.sum([p['size'] for p in positions if p['side'] == s])
                for s in ['SELL', 'BUY']
            }
            pos_size = max(pos_sizes.values())
            pos_side = (
                [k for k, v in pos_sizes.items() if v == pos_size][0]
                if pos_size > 0 else None
            )
            self.logger.info(
                'pos_side: {0}, pos_size: {1}'.format(pos_side, pos_size)
            )
        else:
            raise BfbotError(positions)

        tickers = {
            p: self.bF.ticker(product_code=p)
            for p in [self.pair, self.fx_pair]
        }
        for t in tickers.values():
            if not isinstance(t, dict):
                raise BfbotError(t)
        self.logger.debug(tickers)
        return collateral, pos_side, pos_size, tickers

    def _calc_sfd_stat(self, tickers):
        mp = {
            k: (v['best_bid'] + v['best_ask']) / 2
            for k, v in tickers.items() if k in [self.fx_pair, self.pair]
        }
        deviation = (mp[self.fx_pair] - mp[self.pair]) / mp[self.pair]
        self.logger.info('rate: {0}, deviation: {1}'.format(mp, deviation))
        penal_side = (
            ('BUY' if mp[self.fx_pair] >= mp[self.pair] else 'SELL')
            if abs(deviation) >= self.sfd_pins.min() else None
        )
        sfd_near_dist = np.abs(self.sfd_pins - abs(deviation)).min()
        self.logger.info('penal_side: {0}, sfd_near_dist: {1}'.format(
            penal_side, sfd_near_dist
        ))
        return penal_side, sfd_near_dist

    def _calc_order_targets(self, tickers, order_side):
        base_price = tickers[self.fx_pair][
            {'BUY': 'best_ask', 'SELL': 'best_bid'}[order_side]
        ]
        order_targets = {
            'limit': int(
                base_price * (
                    1 + self.trade['order']['limit_spread'] *
                    {'BUY': - 1, 'SELL': 1}[order_side]
                )
            ),
            'take_profit': int(
                base_price * (
                    1 + self.trade['order']['take_profit'] *
                    {'BUY': 1, 'SELL': - 1}[order_side]
                )
            ),
            'stop_loss': int(
                base_price * (
                    1 + self.trade['order']['stop_loss'] *
                    {'BUY': - 1, 'SELL': 1}[order_side]
                )
            )
        }
        self.logger.info('order_targets: {}'.format(order_targets))
        return order_targets

    def _trade(self):
        try:
            collateral, pos_side, pos_size, tickers = self._fetch_state()
        except Exception as e:
            self.logger.error(e)
            return
        else:
            if (
                    self.reserved_size is not None and
                    self.order_datetime and
                    abs(self.reserved_size - pos_size) * 1000 >= 1 and
                    datetime.now() - self.order_datetime < self.timeout_delta
            ):
                self.logger.info('Wait for execution.')
            else:
                self.logger.info('Calibrate reserved size.')
                self.reserved_size = pos_size
                self.reserved_side = pos_side
                self.order_datetime = None
            self.logger.info(
                'self.reserved_side: {0}, self.reserved_size: {1}'.format(
                    self.reserved_side, self.reserved_size
                )
            )
            penal_side, sfd_near_dist = self._calc_sfd_stat(tickers=tickers)
            order_is_open = (self.reserved_size < self.trade['size']['unit'])
            if order_is_open:
                if max(self.vd_bollinger) < 0:
                    order_side = 'SELL'
                elif min(self.vd_bollinger) > 0:
                    order_side = 'BUY'
                else:
                    order_side = None
                order_size = (
                    min(
                        self.last_open_size + self.trade['size']['unit'],
                        self.trade['size']['max']
                    ) if (
                        self.last_open_size and
                        self.last_collat and
                        self.last_collat > collateral['collateral']
                    ) else self.trade['size']['unit']
                )
                order_targets = (
                    self._calc_order_targets(
                        tickers=tickers, order_side=order_side
                    ) if self.ifdoco else None
                )
            else:
                if self.reserved_side == 'BUY' and self.vd_ewm['mean'] < 0:
                    order_side = 'SELL'
                elif self.reserved_side == 'SELL' and self.vd_ewm['mean'] > 0:
                    order_side = 'BUY'
                else:
                    order_side = None
                order_size = self.reserved_size
                order_targets = None
            reverse_order_side = (
                {'BUY': 'SELL', 'SELL': 'BUY'}[order_side]
                if order_side else None
            )

        if order_side == penal_side:
            self._print(
                'Skip by sfd penalty. '
                '(penalized side: {})'.format(penal_side)
            )
        elif sfd_near_dist < self.trade['skip_sfd_dist']:
            self._print(
                'Skip by sfd boundary. '
                '(distance to a sfd pin: {:.6f})'.format(sfd_near_dist)
            )
        elif order_is_open and self.reserved_size >= self.trade['size']['max']:
            self._print(
                'Skip by position limit. '
                '(position size: {:.3f}, reserved size: {:.3f})'.format(
                    pos_size, self.reserved_size
                )
            )
        elif abs(self.reserved_size - pos_size) * 1000 >= 1:
            self._print(
                'Skip by queued execution. '
                '(position size: {:.3f}, reserved size: {:.3f})'.format(
                    pos_size, self.reserved_size
                )
            )
        elif self.reserved_side:
            self._print(
                'Skip by reserved position. '
                '(reserved side: {:.3f}, reserved size: {:.3f})'.format(
                    self.reserved_size, self.reserved_size
                )
            )
        elif order_side is None:
            if order_is_open:
                self._print(
                    'Skip by volume balance. (EWM volume '
                    'diff bollinger: [{0:.3f}, {1:.3f}])'.format(
                        self.vd_bollinger[0], self.vd_bollinger[1]
                    )
                )
            else:
                self._print(
                    'Skip by volume balance. (EWM volume '
                    'diff: {:.3f})'.format(self.vd_ewm['mean'])
                )
        else:
            try:
                order = (
                    self.bF.sendparentorder(
                        order_method='IFDOCO',
                        time_in_force='GTC',
                        parameters=[
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'LIMIT',
                                'side': order_side,
                                'price': order_targets['limit'],
                                'size': order_size
                            },
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'LIMIT',
                                'side': reverse_order_side,
                                'price': order_targets['take_profit'],
                                'size': order_size
                            },
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'STOP',
                                'side': reverse_order_side,
                                'trigger_price': order_targets['stop_loss'],
                                'size': order_size
                            }
                        ]
                    ) if self.ifdoco and order_is_open else
                    self.bF.sendchildorder(
                        product_code=self.fx_pair,
                        child_order_type='MARKET',
                        side=order_side,
                        size=order_size,
                        time_in_force='GTC'
                    )
                )
            except Exception as e:
                self.logger.error(e)
                return
            else:
                self.logger.info(order)
                order_is_accepted = (
                    isinstance(order, dict) and (
                        'child_order_acceptance_id' in order or
                        'parent_order_acceptance_id' in order
                    )
                )
                if order_is_accepted:
                    self._print(
                        'Accepted: {0} {1} {2} with {3}.'.format(
                            order_side, order_size,
                            self.fx_pair,
                            (
                                'IFDOCO (IFD: {0} => OCO: {1})'.format(
                                    order_targets['limit'],
                                    sorted([
                                        order_targets['stop_loss'],
                                        order_targets['take_profit']
                                    ])
                                ) if self.ifdoco and order_is_open else
                                'MARKET'
                            )
                        )
                    )
                    self.order_datetime = datetime.now()
                    if order_is_open:
                        self.reserved_size += order_size
                        self.reserved_side = order_side
                        self.last_open_size = order_size
                        self.last_collat = collateral['collateral']
                    else:
                        self.reserved_size -= order_size
                        if abs(self.reserved_size) < 0.001:
                            self.reserved_side = None
                        elif self.reserved_size <= - 0.001:
                            self.reserved_side = order_side
                        else:
                            self.reserved_side = reverse_order_side
                else:
                    self._print(
                        'Rejected: {0} {1} {2} with {3}.'.format(
                            order_side, order_size,
                            self.fx_pair,
                            (
                                'IFDOCO (IFD: {0} => OCO: {1})'.format(
                                    order_targets['limit'],
                                    sorted([
                                        order_targets['stop_loss'],
                                        order_targets['take_profit']
                                    ])
                                ) if self.ifdoco and order_is_open else
                                'MARKET'
                            )
                        )
                    )
                    self.logger.warning(os.linesep + dump_yaml(order))


def open_deal(config, pair, ifdoco=False, timeout=3600, quiet=False):
    bas = BfAsyncSubscriber(
        channels=['lightning_executions_FX_{}'.format(pair)]
    )
    bas.pubnub.add_listener(
        BfStreamTrader(
            config=config, pair=pair, ifdoco=ifdoco, timeout=timeout,
            quiet=quiet
        )
    )
    bas.subscribe()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if not quiet:
        print('>>>\t!!! OPEN DEAL !!!')
    bas.pubnub.start()
