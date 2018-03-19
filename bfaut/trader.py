#!/usr/bin/env python

from datetime import datetime, timedelta
import logging
import math
import os
import signal
import numpy as np
import pandas as pd
from pubnub.callbacks import SubscribeCallback
import pybitflyer
from .info import BfAsyncSubscriber
from .util import BfautError, dump_yaml


class BfStreamTrader(SubscribeCallback):
    def __init__(self, config, pair, timeout, quiet=False):
        self.logger = logging.getLogger(__name__)
        self.bF = pybitflyer.API(
            api_key=config['bF']['api_key'],
            api_secret=config['bF']['api_secret']
        )
        self.trade = config['trade']
        self.mode = (self.trade.get('mode') or 'pivot')
        self.pair = pair
        self.timeout_delta = timedelta(seconds=int(timeout))
        self.quiet = quiet
        self.sfd_pins = np.array([0.05, 0.1, 0.15, 0.2])
        self.contrary = (self.mode == 'negative')               # mutable
        self.ticks = {}                                         # mutable
        self.open = True                                        # mutable
        self.won = False                                        # mutable
        self.n_load = 20                                        # mutable
        self.ewm_dv = {'mean': 0, 'var': 1}                     # mutable
        self.ewm_pl = {'mean': 0, 'var': 1}                     # mutable
        self.order_side = None                                  # mutable
        self.margin = None                                      # mutable
        self.position = {}                                      # mutable
        self.sfd_penal_side = None                              # mutable
        self.volumes = pd.DataFrame()                           # mutable
        self.reserved = {}                                      # mutable
        self.order_datetime = None                              # mutable
        self.last_open = {}                                     # mutable
        self.anchor_margin = 0                                  # mutable
        self.n_size_over = 0                                    # mutable
        self.logger.debug(vars(self))

    def message(self, pubnub, message):
        if message.channel.startswith('lightning_executions_FX_'):
            self.volumes = pd.DataFrame(
                message.message
            )[['side', 'size']].append(
                pd.DataFrame({'side': ['BUY', 'SELL'], 'size': [0, 0]})
            ).groupby('side')['size'].sum()
            self.ewm_dv = self._compute_ewm_delta_volume()
            try:
                self.margin = self._fetch_margin()
            except Exception as e:
                self.logger.error(e)
            else:
                if self.n_load <= 0:
                    self.order_side = self._determine_order_side()
                    if self.order_side is None:
                        self._print('Skip by volume difference.')
                    elif self.order_side == self.reserved.get('side'):
                        self._print(
                            'Skip by position. (side: {0}, size: {1})'.format(
                                self.reserved['side'], self.reserved['size']
                            )
                        )
                    else:
                        try:
                            self.position = self._fetch_position()
                            self.sfd_penal_side = self._fetch_sfd_penal_side()
                        except Exception as e:
                            self.logger.error(e)
                        else:
                            self.logger.debug(self.position)
                            self._trade()
                else:
                    self.n_load -= 1
                    self._print(
                        'Wait for loading. (left: {})'.format(self.n_load)
                    )
        elif message.channel.startswith('lightning_ticker_'):
            self.ticks[message.channel] = message.message
            self.logger.debug(self.ticks)
        else:
            self.logger.error('message.channel: {}'.format(message.channel))

    @staticmethod
    def _reverse_side(side):
        return side and {'BUY': 'SELL', 'SELL': 'BUY'}[side]

    def _print(self, message):
        text = (
            '| BUY:{0} | SELL:{1} | EWM DELTA:{2:8.3f} | MARGIN:  {3} | ' +
            'EWM PL:{4:>8d} | ANCHOR:{5:>' + str(len(str(int(self.margin))) + 2) +
            'd} |\t> {6}'
        ).format(
            *[
                (
                    '{:8.3f}'.format(self.volumes[s]) if self.volumes[s]
                    else ' ' * 8
                ) for s in ['BUY', 'SELL']
            ],
            self.ewm_dv['mean'], round(self.margin),
            round(self.ewm_pl['mean']),
            (self.anchor_margin and round(self.margin - self.anchor_margin)),
            message
        )
        if self.quiet:
            self.logger.info(text)
        else:
            print(text, flush=True)

    def _fetch_margin(self):
        collateral = self.bF.getcollateral()
        if isinstance(collateral, dict) and 'status' not in collateral:
            self.logger.debug(collateral)
            margin = collateral['collateral'] + collateral['open_position_pnl']
            self.logger.info('margin: {}'.format(margin))
        else:
            raise BfautError(collateral)
        return margin

    def _fetch_position(self):
        positions = self.bF.getpositions(product_code=('FX_' + self.pair))
        if isinstance(positions, list) and 'status' not in positions:
            self.logger.info('positions: {}'.format(positions))
            ps_sizes = {
                s: sum([p['size'] for p in positions if p['side'] == s])
                for s in ['SELL', 'BUY']
            }
            ps_size = max(ps_sizes.values())
            ps_side = (
                [k for k, v in ps_sizes.items() if v == ps_size][0]
                if ps_size > 0 else None
            )
            position = {'side': ps_side, 'size': round(ps_size * 1000) / 1000}
            self.logger.info('position: {}'.format(position))
        else:
            raise BfautError(positions)
        return position

    def _fetch_sfd_penal_side(self):
        mp = {
            k.replace('lightning_ticker_', ''):
            (v['best_bid'] + v['best_ask']) / 2
            for k, v in self.ticks.items()
        }
        self.logger.info('mp: {}'.format(mp))
        deviation = (mp['FX_' + self.pair] - mp[self.pair]) / mp[self.pair]
        self.logger.info('deviation: {}'.format(deviation))
        sfd_penal_side = (
            ('BUY' if deviation >= 0 else 'SELL')
            if abs(deviation) >= self.sfd_pins.min() else None
        )
        self.logger.info('sfd_penal_side: {}'.format(sfd_penal_side))
        return sfd_penal_side

    def _compute_ewm_delta_volume(self):
        delta_volume = self.volumes['BUY'] - self.volumes['SELL']
        self.logger.info('delta_volume: {}'.format(delta_volume))
        ewm_dv = {
            'mean': (
                self.trade['ewm_alpha'] * delta_volume +
                (1 - self.trade['ewm_alpha']) * self.ewm_dv['mean']
            ),
            'var': (
                (1 - self.trade['ewm_alpha']) * (
                    self.ewm_dv['var'] + self.trade['ewm_alpha'] *
                    np.square(delta_volume - self.ewm_dv['mean'])
                )
            )
        }
        self.logger.info('ewm_dv: {}'.format(ewm_dv))
        return ewm_dv

    def _compute_ewm_pl(self):
        pl = (
            (self.margin - self.last_open['margin']) / self.last_open['size']
            if self.last_open else 0
        )
        self.logger.info('pl: {}'.format(pl))
        ewm_pl = {
            'mean': (
                self.trade['ewm_alpha'] * pl +
                (1 - self.trade['ewm_alpha']) * self.ewm_pl['mean']
            ),
            'var': (
                (1 - self.trade['ewm_alpha']) * (
                    self.ewm_pl['var'] + self.trade['ewm_alpha'] *
                    np.square(pl - self.ewm_pl['mean'])
                )
            )
        }
        self.logger.info('ewm_pl: {}'.format(ewm_pl))
        return ewm_pl

    def _determine_order_side(self):
        bollinger_band = (
            self.ewm_dv['mean'] + np.array([- 1, 1]) *
            np.sqrt(self.ewm_dv['var']) *
            (self.trade.get('sigma_trigger') or 0)
        )
        if min(bollinger_band) > 0:
            fw_side = 'BUY'
        elif max(bollinger_band) < 0:
            fw_side = 'SELL'
        elif self.ewm_dv['mean'] > 0:
            fw_side = 'SELL'
        elif self.ewm_dv['mean'] < 0:
            fw_side = 'BUY'
        else:
            fw_side = None
        order_side = (
            self._reverse_side(fw_side) if self.contrary else fw_side
        )
        self.logger.info(
            'bollinger_band: {0}, order_side: {1}'.format(
                bollinger_band, order_side
            )
        )
        return order_side

    def _compute_order_size(self):
        if not self.open:
            bet_size = self.reserved['size']
        elif self.last_open and self.n_size_over == 1:
            bet_size = self.last_open['size']
        elif self.last_open and self.n_size_over == 0:
            betting_system = self.trade.get('bet')
            if not self.won and betting_system == 'Martingale':
                m = (self.trade['size'].get('multiplier') or 2)
                bet_size = (
                    self.last_open['size'] * m
                    if self.margin < self.last_open['margin'] else
                    self.last_open['size'] / m
                )
            elif not self.won and betting_system == "d'Alembert":
                a = self.trade['size']['unit'] * (
                    self.trade['size'].get('multiplier') or 1
                )
                bet_size = (
                    self.last_open['size'] + a
                    if self.margin < self.last_open['margin'] else
                    self.last_open['size'] - a
                )
            else:
                bet_size = self.trade['size']['unit']
        else:
            bet_size = self.trade['size']['unit']
        order_size = math.ceil(
            (
                min(bet_size, self.trade['size']['max'])
                if self.open and 'max' in self.trade['size'] else bet_size
            ) * 1000
        ) / 1000
        self.logger.info(
            'bet_size: {0}, order_size: {1}'.format(bet_size, order_size)
        )
        return order_size

    def _trade(self):
        queue_is_left = (
            abs(self.reserved['size'] - self.position['size']) >= 0.001
            if self.reserved else False
        )
        if (
                self.order_datetime and queue_is_left and
                datetime.now() - self.order_datetime < self.timeout_delta
        ):
            self.logger.info('Wait for execution.')
        else:
            self.logger.info('Calibrate reserved size.')
            self.reserved = self.position
            self.order_datetime = None
        self.logger.info('self.reserved: {}'.format(self.reserved))
        if self.anchor_margin:
            self.logger.info(
                'pl in round: {}'.format(self.margin - self.anchor_margin)
            )
            self.won = (self.margin >= self.anchor_margin)
            self.logger.info('self.won: {}'.format(self.won))
            if self.won and self.reserved['size'] == 0 and not queue_is_left:
                self.anchor_margin = 0
            else:
                pass
            self.logger.info(
                'self.anchor_margin: {}'.format(self.anchor_margin)
            )
        else:
            self.logger.debug('self.won: {}'.format(self.won))
        self.open = (self.reserved['size'] < self.trade['size']['unit'])
        order_size = self._compute_order_size()

        if queue_is_left:
            self._print(
                'Skip by queue. (side: {0}, size: {1})'.format(
                    self.reserved['side'], self.reserved['size']
                )
            )
        elif self.order_side == self.reserved['side']:
            self._print(
                'Skip by position. (side: {0}, size: {1})'.format(
                    self.reserved['side'], self.reserved['size']
                )
            )
        elif self.order_side == self.sfd_penal_side:
            self._print(
                'Skip by sfd penalty. (side: {})'.format(self.sfd_penal_side)
            )
        else:
            try:
                order = self.bF.sendchildorder(
                    product_code=('FX_' + self.pair),
                    child_order_type='MARKET',
                    side=self.order_side,
                    size=order_size,
                    time_in_force='GTC'
                )
            except Exception as e:
                self.logger.error(e)
            else:
                self.logger.info(order)
                order_is_accepted = (
                    isinstance(order, dict) and (
                        'child_order_acceptance_id' in order
                    )
                )
                self._print(
                    '{0} {1} {2}. => {3}.'.format(
                        self.order_side, order_size,
                        self.pair.replace('_', '-FX/'),
                        'Accepted' if order_is_accepted else 'Rejected'
                    )
                )
                if order_is_accepted:
                    self.order_datetime = datetime.now()
                    if self.open:
                        self.last_open = {
                            'side': self.order_side, 'size': order_size,
                            'margin': self.margin
                        }
                        self.reserved = {
                            'side': self.order_side,
                            'size': self.reserved['size'] + order_size
                        }
                        if self.anchor_margin:
                            pass
                        else:
                            self.anchor_margin = self.margin
                            self.logger.info(
                                'self.anchor_margin: {}'.format(
                                    self.anchor_margin
                                )
                            )
                    else:
                        updated_size = self.reserved['size'] - order_size
                        if abs(updated_size) < 0.001:
                            self.reserved = {
                                'side': None, 'size': updated_size
                            }
                        elif updated_size >= 0.001:
                            self.reserved = {
                                'side': self.order_side, 'size': updated_size
                            }
                        else:
                            self.reserved = {
                                'side': self._reverse_side(self.order_side),
                                'size': abs(updated_size)
                            }
                        self.ewm_pl = self._compute_ewm_pl()
                        if self.mode == 'pivot' and self.ewm_pl['mean'] < 0:
                            self.contrary = not self.contrary
                            self.logger.info(
                                'self.contrary: {}'.format(self.contrary)
                            )
                        else:
                            pass
                    self.n_size_over = 0
                else:
                    self.n_size_over += int(
                        'status' in order and order['status'] == - 205
                    )
                    self.logger.warning(os.linesep + dump_yaml(order))


def open_deal(config, pair, timeout=3600, quiet=False):
    bas = BfAsyncSubscriber(
        channels=[
            'lightning_executions_FX_{}'.format(pair),
            'lightning_ticker_FX_{}'.format(pair),
            'lightning_ticker_{}'.format(pair)
        ]
    )
    bas.pubnub.add_listener(
        BfStreamTrader(
            config=config, pair=pair, timeout=timeout, quiet=quiet
        )
    )
    bas.subscribe()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if not quiet:
        print('!!! OPEN DEAL !!!')
    bas.pubnub.start()
