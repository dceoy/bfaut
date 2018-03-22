#!/usr/bin/env python

from datetime import datetime, timedelta
import logging
from pprint import pprint
import signal
import numpy as np
import pandas as pd
from pubnub.callbacks import SubscribeCallback
import pybitflyer
from .info import BfAsyncSubscriber
from .util import BfautError


class BfStreamTrader(SubscribeCallback):
    def __init__(self, config, pair, timeout, quiet=False):
        self.logger = logging.getLogger(__name__)
        self.bF = pybitflyer.API(
            api_key=config['bF']['api_key'],
            api_secret=config['bF']['api_secret']
        )
        self.trade = config['trade']
        self.pair = pair
        self.timeout_delta = timedelta(seconds=int(timeout))
        self.quiet = quiet
        self.sfd_pins = np.array([0.05, 0.1, 0.15, 0.2])
        self.betting_system = self.trade.get('bet')
        self.contrary = self.trade.get('contrary')
        self.ticks = {}                                         # mutable
        self.open = None                                        # mutable
        self.won = False                                        # mutable
        self.n_load = 20                                        # mutable
        self.ewm_dv = {'mean': 0, 'var': 1}                     # mutable
        self.bollinger = []                                     # mutable
        self.order_side = None                                  # mutable
        self.init_margin = None                                 # mutable
        self.margin = None                                      # mutable
        self.position = {}                                      # mutable
        self.sfd_penal_side = None                              # mutable
        self.volumes = pd.DataFrame()                           # mutable
        self.reserved = {}                                      # mutable
        self.order_datetime = None                              # mutable
        self.last_open = {}                                     # mutable
        self.retried_side = None                                # mutable
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
            self.ewm_dv = self._calculate_ewm_delta_volume()
            self.bollinger_band = self._calculate_bollinger_bands()
            try:
                self.margin = self._fetch_margin()
            except Exception as e:
                self.logger.error(e)
            else:
                if self.n_load <= 0:
                    self.order_side = self._determine_order_side()
                    if self.order_side:
                        try:
                            self.position = self._fetch_position()
                            self.sfd_penal_side = self._fetch_sfd_penal_side()
                        except Exception as e:
                            self.logger.error(e)
                        else:
                            self.logger.debug(self.position)
                            self._trade()
                    else:
                        self._print(
                            'Skip by delta volume.{}'.format(
                                ' (bb: {})'.format(
                                    np.array2string(
                                        self.bollinger_band,
                                        formatter={
                                            'float_kind':
                                            lambda f: '{:5.1f}'.format(f)
                                        }
                                    )
                                ) if self.trade.get('bollinger') else ''
                            )
                        )
                elif self.n_load == 1:
                    try:
                        self.reserved = self._fetch_position()
                    except Exception as e:
                        self.logger.error(e)
                    else:
                        self.n_load = 0
                        self._print('Complete loading.')
                elif self.init_margin:
                    self.n_load -= 1
                    self._print(
                        'Wait for loading. (left: {})'.format(self.n_load)
                    )
                else:
                    self.init_margin = self.margin
                    self._print(
                        'Start loading. (left: {})'.format(self.n_load)
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
            '| BUY:{0} | SELL:{1} | EWM DELTA:{2:8.3f} | MARGIN:  {3} |' +
            ' PL:{4:' + str(len(str(int(self.margin))) + 1) + 'd} |\t> {5}'
        ).format(
            *[
                (
                    '{:8.3f}'.format(self.volumes[s]) if self.volumes[s]
                    else ' ' * 8
                ) for s in ['BUY', 'SELL']
            ],
            self.ewm_dv['mean'], round(self.margin),
            round(self.margin - self.init_margin),
            message
        )
        if self.quiet:
            self.logger.info(text)
        else:
            print(text, flush=True)

    def _determine_order_side(self):
        if self.bollinger_band.size > 2:
            if self.bollinger_band[0] < 0 and self.bollinger_band[1] > 0:
                fw_side = 'BUY'
            elif self.bollinger_band[- 1] > 0 and self.bollinger_band[- 2] < 0:
                fw_side = 'SELL'
            else:
                fw_side = self._reverse_side(self.reserved.get('side'))
        else:
            if self.bollinger_band[0] > 0:
                fw_side = 'BUY'
            elif self.bollinger_band[- 1] < 0:
                fw_side = 'SELL'
            else:
                fw_side = self._reverse_side(self.reserved.get('side'))
        order_side = self.retried_side or (
            self._reverse_side(fw_side) if self.contrary else fw_side
        )
        self.logger.info('order_side: {}'.format(order_side))
        return order_side

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

    def _calculate_ewm_delta_volume(self):
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

    def _calculate_bollinger_bands(self):
        b = (self.trade.get('bollinger') or [0])
        multipliers = (b if b and isinstance(b, list) else [b])
        return np.sort(np.concatenate([
            self.ewm_dv['mean'] + np.array([- m, m]) *
            np.sqrt(self.ewm_dv['var'])
            for m in multipliers
        ]))

    def _calculate_order_size(self):
        if self.open:
            init_size = (
                self.trade['size'].get('init') or
                self.trade['size'].get('unit') or 0.001
            )
            self.logger.info('init_size: {}'.format(init_size))
            if self.last_open and self.n_size_over == 1:
                bet_size = self.last_open['size']
            elif self.last_open and self.n_size_over == 0:
                if self.betting_system == 'Martingale':
                    if self.won:
                        bet_size = init_size
                    else:
                        bet_size = self.last_open['size'] * 2
                elif self.betting_system == "d'Alembert":
                    if self.won:
                        bet_size = init_size
                    else:
                        bet_size = (
                            self.last_open['size'] + self.trade['size']['unit']
                        )
                elif self.betting_system == 'Pyramid':
                    if self.won:
                        bet_size = (
                            self.last_open['size'] - self.trade['size']['unit']
                        )
                    else:
                        bet_size = (
                            self.last_open['size'] + self.trade['size']['unit']
                        )
                elif self.betting_system == "Oscar's Grind":
                    if self.margin >= self.anchor_margin:
                        bet_size = init_size
                    elif self.won:
                        bet_size = (
                            self.last_open['size'] + self.trade['size']['unit']
                        )
                    else:
                        bet_size = self.last_open['size']
                else:
                    bet_size = init_size
            else:
                bet_size = init_size
            self.logger.info('bet_size: {}'.format(bet_size))
            size_range = sorted([
                (self.trade['size'].get('min') or bet_size),
                (self.trade['size'].get('max') or bet_size)
            ])
            if bet_size < size_range[0]:
                order_size = size_range[0]
            elif bet_size > size_range[1]:
                order_size = size_range[1]
            else:
                order_size = round(bet_size * 1000) / 1000
        else:
            order_size = self.reserved['size']
        self.logger.info('order_size: {}'.format(order_size))
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
        if self.last_open and not queue_is_left:
            self.won = (self.margin > self.last_open['margin'])
        else:
            pass
        self.logger.info('self.won: {}'.format(self.won))
        if self.betting_system == "Oscar's Grind" and self.anchor_margin:
            anchor_pl = self.margin - self.anchor_margin
            self.logger.info('pl in round: {}'.format(anchor_pl))
            no_position = (self.reserved['size'] == 0 and not queue_is_left)
            if anchor_pl >= 0 and no_position:
                self.anchor_margin = 0
            else:
                pass
            self.logger.info(
                'self.anchor_margin: {}'.format(self.anchor_margin)
            )
        else:
            pass
        self.open = (self.reserved['size'] < 0.001)
        order_size = self._calculate_order_size()

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
                        if self.betting_system == "Oscar's Grind":
                            if self.anchor_margin:
                                self.logger.debug(self.anchor_margin)
                            else:
                                self.anchor_margin = self.margin
                                self.logger.info(
                                    'self.anchor_margin: {}'.format(
                                        self.anchor_margin
                                    )
                                )
                        else:
                            pass
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
                    self.retried_side = None
                    self.n_size_over = 0
                else:
                    if order.get('status') == - 205:
                        self.n_size_over += 1
                    elif order.get('status') in [- 1, - 208]:
                        self.retried_side = self.order_side
                    else:
                        pass
                    pprint(order)


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
