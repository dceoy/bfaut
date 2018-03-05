#!/usr/bin/env python

from datetime import timedelta
import logging
import signal
import numpy as np
import pandas as pd
from pubnub.callbacks import SubscribeCallback
import pybitflyer
from .streamer import BfAsyncSubscriber


class BfStreamTrader(SubscribeCallback):
    def __init__(self, pair, config, quiet=False):
        self.pair = pair
        self.fx_pair = 'FX_{}'.format(pair)
        self.trade = config['trade']
        self.start_datetime = None
        self.quiet = quiet
        self.sfd_pins = np.array([0.1, 0.15, 0.2])
        self.bF = pybitflyer.API(
            api_key=config['bF']['api_key'],
            api_secret=config['bF']['api_secret']
        )
        self.df = pd.DataFrame()
        self.logger = logging.getLogger(__name__)

    def message(self, pubnub, message):
        new_df = pd.DataFrame(
            message.message
        ).assign(
            exec_date=lambda d: pd.to_datetime(d['exec_date'])
        ).set_index('exec_date')[['side', 'size', 'price']]
        if not self.start_datetime:
            self.start_datetime = new_df.index.min()
            self._print('Collecting execution data...')
        window_start_datetime = new_df.index.max() - timedelta(
            seconds=self.trade['window']['size_seconds']
        )
        self.df = self.df.append(new_df).pipe(
            lambda d: d[d.index >= window_start_datetime]
        )
        logging.debug(self.df)
        if len(self.df) and self.start_datetime < window_start_datetime:
            volumes = self.df.append(
                pd.DataFrame(data={'side': ['BUY', 'SELL'], 'size': [0, 0]})
            ).groupby('side')['size'].sum()
            if (
                    abs(np.diff(volumes)[0]) <
                    self.trade['window']['min_volume_diff']
            ):
                self._print(
                    'Skipped for a volume balance.\t'
                    '[ BUY: {0:.2f}, SELL: {1:.2f} ]'.format(
                        volumes['BUY'], volumes['SELL']
                    )
                )
            else:
                self._trade(volumes=volumes)

    def _print(self, message):
        text = '>>>\t{}'.format(message)
        if self.quiet:
            self.logger.debug(text)
        else:
            print(text, flush=True)

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

        tickers = {
            p: self.bF.ticker(product_code=p)
            for p in [self.pair, self.fx_pair]
        }
        self.logger.info('tickers: {}'.format(tickers))
        return keep_rate, pos_side, pos_size, tickers

    def _calc_deviation(self, tickers):
        mp = {
            k: (v['best_bid'] + v['best_ask']) / 2
            for k, v in tickers.items() if k in [self.fx_pair, self.pair]
        }
        deviation = (mp[self.fx_pair] - mp[self.pair]) / mp[self.pair]
        self.logger.info('rate: {0}, deviation: {1}'.format(mp, deviation))
        penalized_side = (
            ('BUY' if mp[self.fx_pair] >= mp[self.pair] else 'SELL')
            if abs(deviation) >= self.sfd_pins.min() else None
        )
        sfd_near_dist = np.abs(self.sfd_pins - abs(deviation)).min()
        self.logger.info('penalized_side: {0}, sfd_near_dist: {1}'.format(
            penalized_side, sfd_near_dist
        ))
        return penalized_side, sfd_near_dist

    def _trade(self, volumes):
        try:
            keep_rate, pos_side, pos_size, tickers = self._fetch_state()
        except Exception as e:
            self.logger.error(e)
            return
        else:
            open_side = volumes.idxmax()
            close_side = {'BUY': 'SELL', 'SELL': 'BUY'}[open_side]
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
            penalized_side, sfd_near_dist = self._calc_deviation(
                tickers=tickers
            )

        if (
                open_side != penalized_side and
                sfd_near_dist > self.trade['skip_sfd_dist'] and (
                    pos_side != open_side or (
                        pos_size <= self.trade['size']['max'] and
                        keep_rate >= self.trade['min_keep_rate']
                    )
                )
        ):
            try:
                order = (
                    self.bF.sendchildorder(
                        product_code=self.fx_pair,
                        child_order_type='MARKET',
                        side=open_side,
                        size=self.trade['size']['unit'],
                        minute_to_expire=self.trade['order_exp_minutes'],
                        time_in_force='IOC'
                    )
                    if pos_side and pos_side != open_side else
                    self.bF.sendparentorder(
                        order_method='IFDOCO',
                        time_in_force='GTC',
                        minute_to_expire=self.trade['order_exp_minutes'],
                        parameters=[
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'LIMIT',
                                'side': open_side,
                                'price': order_prices['limit'],
                                'size': self.trade['size']['unit']
                            },
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'LIMIT',
                                'side': close_side,
                                'price': order_prices['take_profit'],
                                'size': self.trade['size']['unit']
                            },
                            {
                                'product_code': self.fx_pair,
                                'condition_type': 'STOP',
                                'side': close_side,
                                'trigger_price': order_prices['stop_loss'],
                                'size': self.trade['size']['unit']
                            }
                        ]
                    )
                )
            except Exception as e:
                self.logger.error(e)
                return e
            else:
                self.logger.debug(order)
                if 'status' not in order or order['status'] != - 205:
                    self._print(
                        (
                            '{0} {1} {2} with {3}.\t'
                            '[ BUY: {4:.2f}, SELL: {5:.2f} ]'
                        ).format(
                            open_side, self.trade['size']['unit'],
                            self.fx_pair,
                            (
                                'MARKET' if pos_side and pos_side != open_side
                                else 'IFDOCO (IFD: {0} => OCO: {1})'.format(
                                    order_prices['limit'],
                                    sorted([
                                        order_prices['stop_loss'],
                                        order_prices['take_profit']
                                    ])
                                )
                            ),
                            volumes['BUY'], volumes['SELL']
                        )
                    )
        else:
            self._print(
                'Skipped by the criteria.\t'
                '[ BUY: {0:.2f}, SELL: {1:.2f} ]'.format(
                    volumes['BUY'], volumes['SELL']
                )
            )


def open_deal(config, pair='BTC_JPY', quiet=False):
    bas = BfAsyncSubscriber(
        channels=['lightning_executions_FX_{}'.format(pair)]
    )
    bas.pubnub.add_listener(
        BfStreamTrader(pair=pair, config=config, quiet=quiet)
    )
    bas.subscribe()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if not quiet:
        print('>>>\t!!! OPEN DEAL !!!')
    bas.pubnub.start()
