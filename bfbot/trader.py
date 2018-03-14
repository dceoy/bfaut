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
    def __init__(self, pair, config, timeout, quiet=False):
        self.pair = pair
        self.fx_pair = 'FX_{}'.format(pair)
        self.trade = config['trade']
        self.timeout_delta = timedelta(seconds=int(timeout))
        self.quiet = quiet
        self.n_to_load = 20
        self.sfd_pins = np.array([0.05, 0.1, 0.15, 0.2])
        self.bF = pybitflyer.API(
            api_key=config['bF']['api_key'],
            api_secret=config['bF']['api_secret']
        )
        self.start_datetime = None
        self.vd_ewm = None
        self.bollinger = None
        self.msg_prefix = '||||'
        self.reserved_side = None
        self.reserved_size = None
        self.order_datetime = None
        self.last_open_size = None
        self.last_collat = None
        self.oversized_alert = False
        self.logger = logging.getLogger(__name__)

    def message(self, pubnub, message):
        volumes = pd.DataFrame(
            message.message
        )[['side', 'size']].append(
            pd.DataFrame({'side': ['BUY', 'SELL'], 'size': [0, 0]})
        ).groupby('side')['size'].sum()
        volume_diff = volumes['BUY'] - volumes['SELL']
        if self.vd_ewm is None:
            self.vd_ewm = {
                'mean': volume_diff, 'var': np.square(volume_diff)
            }
            self.n_to_load -= 1
        else:
            self.vd_ewm = {
                'mean': (
                    self.trade['volume_ewm_alpha'] * volume_diff +
                    (1 - self.trade['volume_ewm_alpha']) * self.vd_ewm['mean']
                ),
                'var': (
                    (1 - self.trade['volume_ewm_alpha']) * (
                        self.vd_ewm['var'] + self.trade['volume_ewm_alpha'] *
                        np.square(volume_diff - self.vd_ewm['mean'])
                    )
                )
            }
            self.logger.info(
                'self.vd_ewm: {}'.format(self.vd_ewm)
            )
            self.bollinger = np.sort(np.concatenate([
                np.array([- m, m]) * np.sqrt(self.vd_ewm['var']) +
                self.vd_ewm['mean']
                for m in self.trade['sigma_trigger']
            ]))
            self.msg_prefix = (
                '| BUY:' + (
                    '{:8.3f}'.format(volumes['BUY'])
                    if volumes['BUY'] else ' ' * 8
                ) + ' | SELL:' + (
                    '{:8.3f}'.format(volumes['SELL'])
                    if volumes['SELL'] else ' ' * 8
                ) + ' |\tEWM{}'.format(np.round(self.bollinger * 10) / 10)
            )
            if self.n_to_load == 0:
                self._trade()
            else:
                self.n_to_load -= 1
                self.logger.info('self.n_to_load: {}'.format(self.n_to_load))
                self._print('Wait for loading...')

    def _print(self, message):
        text = '{0}\t> {1}'.format(self.msg_prefix, message)
        if self.quiet:
            self.logger.info(text)
        else:
            print(text, flush=True)

    def _determine_order_side(self):
        if self.bollinger[0] > 0:
            order_side = 'BUY'
        elif self.bollinger[- 1] < 0:
            order_side = 'SELL'
        elif self.bollinger[1] > 0:
            order_side = 'SELL'
        elif self.bollinger[- 2] < 0:
            order_side = 'BUY'
        else:
            order_side = None
        self.logger.info('order_side: {}'.format(order_side))
        return order_side

    def _fetch_state(self):
        collateral = self.bF.getcollateral()
        if isinstance(collateral, dict) and 'collateral' in collateral:
            self.logger.debug(collateral)
            collat = collateral['collateral']
            self.logger.info('collat: {}'.format(collat))
        else:
            raise BfbotError(collateral)

        positions = self.bF.getpositions(product_code=self.fx_pair)
        if isinstance(positions, list):
            self.logger.info('positions: {}'.format(positions))
            pos_sizes = {
                s: sum([p['size'] for p in positions if p['side'] == s])
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
        return collat, pos_side, pos_size, tickers

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

    def _calc_bet_size(self, won=True):
        if self.trade['bet'] == 'Martingale':
            bet_size = (
                self.trade['size']['unit'] if won else round(
                    self.last_open_size * (
                        self.trade['size'].get('multiplier') or 2
                    ) * 1000
                ) / 1000
            )
        elif self.trade['bet'] == "d'Alembert":
            bet_size = (
                self.trade['size']['unit'] if won
                else self.last_open_size + self.trade['size']['unit']
            )
        elif self.trade['bet'] == "Oscar's grind":
            bet_size = (
                self.last_open_size + self.trade['size']['unit'] if won
                else self.trade['size']['unit']
            )
        else:
            bet_size = self.trade['size']['unit']
        return min(bet_size, self.trade['size'].get('max') or bet_size)

    def _trade(self):
        try:
            collat, pos_side, pos_size, tickers = self._fetch_state()
        except Exception as e:
            self.logger.error(e)
            return
        else:
            if (
                    self.reserved_size is not None and
                    self.order_datetime and
                    abs(self.reserved_size - pos_size) >= 0.001 and
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
            order_side = self._determine_order_side()
            order_is_open = (self.reserved_size < self.trade['size']['unit'])
            if order_side is None:
                order_size = 0
            if order_is_open:
                order_size = (
                    self._calc_bet_size(won=(self.last_collat < collat)) if (
                        self.last_open_size and self.last_collat and
                        not self.oversized_alert
                    ) else self.trade['size']['unit']
                )
            else:
                order_size = self.reserved_size
            self.oversized_alert = False

        if abs(self.reserved_size - pos_size) >= 0.001:
            self._print(
                'Skip by queue. (side: {0}, size: {1})'.format(
                    self.reserved_side, self.reserved_size
                )
            )
        elif order_side is None:
            self._print('Skip by volume difference.')
        elif order_side == self.reserved_side:
            self._print(
                'Skip by position. (side: {})'.format(self.reserved_side)
            )
        elif order_side == penal_side:
            self._print('Skip by sfd penalty. (side: {})'.format(penal_side))
        elif sfd_near_dist < self.trade['skip_sfd_dist']:
            self._print(
                'Skip by sfd boundary. (distance: {:.4f})'.format(
                    sfd_near_dist
                )
            )
        else:
            try:
                order = self.bF.sendchildorder(
                    product_code=self.fx_pair,
                    child_order_type='MARKET',
                    side=order_side,
                    size=order_size,
                    time_in_force='GTC'
                )
            except Exception as e:
                self.logger.error(e)
                return
            else:
                self.logger.info(order)
                order_is_accepted = (
                    isinstance(order, dict) and (
                        'child_order_acceptance_id' in order
                    )
                )
                if order_is_accepted:
                    self._print(
                        '{0} {1} {2}. => Accepted.'.format(
                            order_side, order_size, self.fx_pair
                        )
                    )
                    self.order_datetime = datetime.now()
                    if order_is_open:
                        self.reserved_size += order_size
                        self.reserved_side = order_side
                        self.last_open_size = order_size
                        self.last_collat = collat
                    else:
                        self.reserved_size -= order_size
                        if abs(self.reserved_size) < 0.001:
                            self.reserved_side = None
                        elif self.reserved_size <= - 0.001:
                            self.reserved_side = order_side
                        else:
                            self.reserved_side = {
                                'BUY': 'SELL', 'SELL': 'BUY'
                            }[order_side]
                else:
                    self._print(
                        '{0} {1} {2}. => Rejected.'.format(
                            order_side, order_size, self.fx_pair
                        )
                    )
                    if order.get('status') == - 205:
                        self.oversized_alert = True
                    self.logger.warning(os.linesep + dump_yaml(order))


def open_deal(config, pair, timeout=3600, quiet=False):
    bas = BfAsyncSubscriber(
        channels=['lightning_executions_FX_{}'.format(pair)]
    )
    bas.pubnub.add_listener(
        BfStreamTrader(
            config=config, pair=pair, timeout=timeout,
            quiet=quiet
        )
    )
    bas.subscribe()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if not quiet:
        print('>>  !!! OPEN DEAL !!!')
    bas.pubnub.start()
