#!/usr/bin/env python

import logging
import os
import signal
import time
import numpy as np
import pybitflyer
from .util import ConsoleHelper


def open_deal(config, interval=0, quiet=False):
    sfd_pin = np.array([- 0.2, - 0.15, - 0.1, 0.1, 0.15, 0.2])
    bF = pybitflyer.API(
        api_key=config['bF']['api_key'], api_secret=config['bF']['api_secret']
    )
    board_state = bF.getboardstate()
    logging.debug('board_state: {}'.format(board_state))
    if board_state['health'] in ['NO ORDER', 'STOP']:
        return
    cf = config['trade']
    ch = ConsoleHelper(quiet=quiet)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if not quiet:
        ch.print_log('OPEN DEALS')

    while True:
        logging.debug('(linesep){}'.format(os.linesep * 2))
        try:
            collateral = bF.getcollateral()
        except Exception:
            continue
        else:
            if 'keep_rate' in collateral:
                keep_rate = collateral['keep_rate']
                logging.info('keep_rate: {}'.format(keep_rate))
            else:
                continue

        try:
            positions = bF.getpositions(product_code='FX_BTC_JPY')
        except Exception:
            continue
        else:
            if all([('side' in p) and ('size' in p) for p in positions]):
                pos_sizes = {
                    s: np.sum([p['size'] for p in positions if p['side'] == s])
                    for s in ['SELL', 'BUY']
                }
            else:
                pos_sizes = {'SELL': 0, 'BUY': 0}
            pos_size = max(pos_sizes.values())
            pos_side = (
                [k for k, v in pos_sizes.items() if v == pos_size][0]
                if pos_size > 0 else None
            )
            logging.info('pos_side: {0}, pos_size: {1}'.format(
                pos_side, pos_size
            ))

        try:
            boards = {
                pc: bF.board(product_code=pc)
                for pc in ['BTC_JPY', 'FX_BTC_JPY']
            }
        except Exception:
            continue
        else:
            mp = {
                pc: int(b['mid_price'])
                for pc, b in boards.items() if 'mid_price' in b
            }
            if all(mp.values()):
                deviation = (mp['FX_BTC_JPY'] - mp['BTC_JPY']) / mp['BTC_JPY']
                ch.print_log(
                    'BTC/JPY: {0}, BTC-FX/JPY: {1}, Deviation: {2}'.format(
                        mp['BTC_JPY'], mp['FX_BTC_JPY'], deviation
                    )
                )
                abs_diff = abs(sfd_pin - deviation)
                min_abs_diff = abs_diff.min()
                nearest_pin = sfd_pin[abs_diff == min_abs_diff][0]
                logging.info(
                    'min_abs_diff: {0}, nearest_pin: {1}'.format(
                        min_abs_diff, nearest_pin
                    )
                )
                order_sides = (
                    {'open': 'SELL', 'close': 'BUY'}
                    if nearest_pin < deviation else
                    {'open': 'BUY', 'close': 'SELL'}
                )
                order_prices = {
                    'limit': int(
                        mp['FX_BTC_JPY'] * (
                            1 + cf['sfd']['limit_spread'] * {
                                'BUY': - 1, 'SELL': 1
                            }[order_sides['open']]
                        )
                    ),
                    'take_profit': int(
                        mp['FX_BTC_JPY'] * (
                            1 + cf['sfd']['take_profit'] * {
                                'BUY': 1, 'SELL': - 1
                            }[order_sides['open']]
                        )
                    ),
                    'stop_loss': int(
                        mp['FX_BTC_JPY'] * (
                            1 + cf['sfd']['stop_loss'] * {
                                'BUY': - 1, 'SELL': 1
                            }[order_sides['open']]
                        )
                    )
                }
            else:
                continue

        if (
                min_abs_diff <= cf['sfd']['max_dist_open'] and
                min_abs_diff >= cf['sfd']['min_dist_open'] and
                pos_size <= cf['size']['max'] and
                (
                    pos_size == 0 or
                    pos_side != order_sides['open'] or
                    keep_rate >= cf['min_keep_rate']
                )
        ):
            try:
                order = (
                    bF.sendchildorder(
                        product_code='FX_BTC_JPY',
                        child_order_type='MARKET',
                        side=order_sides['open'],
                        size=cf['size']['unit'],
                        minute_to_expire=cf['sfd']['minute_to_expire'],
                        time_in_force='IOC'
                    )
                    if pos_side and pos_side != order_sides['open'] else
                    bF.sendparentorder(
                        order_method='IFDOCO',
                        time_in_force='GTC',
                        minute_to_expire=cf['sfd']['minute_to_expire'],
                        parameters=[
                            {
                                'product_code': 'FX_BTC_JPY',
                                'condition_type': 'LIMIT',
                                'side': order_sides['open'],
                                'price': order_prices['limit'],
                                'size': cf['size']['unit']
                            },
                            {
                                'product_code': 'FX_BTC_JPY',
                                'condition_type': 'LIMIT',
                                'side': order_sides['close'],
                                'price': order_prices['take_profit'],
                                'size': cf['size']['unit']
                            },
                            {
                                'product_code': 'FX_BTC_JPY',
                                'condition_type': 'STOP',
                                'side': order_sides['close'],
                                'trigger_price': order_prices['stop_loss'],
                                'size': cf['size']['unit']
                            }
                        ]
                    )
                )
            except Exception:
                continue
            else:
                logging.debug(order)
                if (
                        not quiet and
                        ('status' not in order or order['status'] != - 205)
                ):
                    ch.print_log(
                        '{0} {1} BTC-FX/JPY with {2}'.format(
                            order_sides['open'], cf['size']['unit'],
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
            logging.info('Skipped by the criteria.')
            time.sleep(interval)
