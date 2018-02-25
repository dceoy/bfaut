#!/usr/bin/env python

import logging
import signal
import time
import numpy as np
import pybitflyer


def open_deal(config, interval=0, quiet=False):
    sfd_pin = np.array([- 0.2, - 0.15, - 0.1, 0.1, 0.15, 0.2])
    bF = pybitflyer.API(
        api_key=config['bF']['api_key'], api_secret=config['bF']['api_secret']
    )
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    queued_orders = set()
    if not quiet:
        print('>>> !!! OPEN SFD DEALS !!!')

    while True:
        try:
            collateral = bF.getcollateral()
            if 'keep_rate' in collateral:
                keep_rate = collateral['keep_rate']
                logging.debug('keep_rate: {}'.format(keep_rate))
            else:
                continue

            positions = bF.getpositions(product_code='FX_BTC_JPY')
            if all([('side' in p) and ('size' in p) for p in positions]):
                pos_sizes = {
                    s: np.sum([p['size'] for p in positions if p['side'] == s])
                    for s in ['SELL', 'BUY']
                }
            else:
                pos_sizes = 0
            logging.debug('pos_sizes: {}'.format(pos_sizes))

            active_orders = bF.getchildorders(
                product_code='FX_BTC_JPY', child_order_state='ACTIVE'
            )
            queued_orders = queued_orders.intersection({
                o['child_order_acceptance_id'] for o in active_orders
                if 'child_order_acceptance_id' in o
            })
            logging.debug('queued_orders: {}'.format(queued_orders))

            boards = {
                pc: bF.board(product_code=pc)
                for pc in ['BTC_JPY', 'FX_BTC_JPY']
            }
            mp = {
                pc: b['mid_price']
                for pc, b in boards.items() if 'mid_price' in b
            }
            if all(mp.values()):
                deviation = (mp['FX_BTC_JPY'] - mp['BTC_JPY']) / mp['BTC_JPY']
                logging.debug(
                    'deviation: {0} (BTC/JPY: {1}, BTC-FX/JPY: {2})'.format(
                        deviation, mp['BTC_JPY'], mp['FX_BTC_JPY']
                    )
                )
                abs_diff = abs(sfd_pin - deviation)
                min_abs_diff = abs_diff.min()
                nearest_pin = sfd_pin[abs_diff == min_abs_diff][0]
                logging.debug(
                    'min_abs_diff: {0}, nearest_pin: {1}'.format(
                        min_abs_diff, nearest_pin
                    )
                )
            else:
                continue

            if (
                    min_abs_diff < config['trade']['max_dist_open'] and
                    min_abs_diff > config['trade']['min_dist_open'] and
                    max(pos_sizes.values()) <= config['trade']['max_size'] and
                    not queued_orders
            ):
                side = (
                    'SELL' if nearest_pin < deviation else 'BUY'
                )
                order = bF.sendchildorder(
                    product_code='FX_BTC_JPY',
                    child_order_type='MARKET',
                    side=side,
                    size=config['trade']['unit_size'],
                    minute_to_expire=1,
                    time_in_force='GTC'
                )
                logging.debug(order)
                if 'child_order_acceptance_id' in order:
                    queued_orders.add(order['child_order_acceptance_id'])
                if not quiet:
                    print('>>> {0} {1} BTC-FX/JPY'.format(
                        side, config['trade']['unit_size']
                    ))
            elif (
                min(abs_diff) > config['trade']['min_dist_close'] or
                0 < keep_rate < config['trade']['min_keep_rate']
            ):
                for side, size in pos_sizes.items():
                    s = {'SELL': 'BUY', 'BUY': 'SELL'}[side]
                    order = bF.sendchildorder(
                        product_code='FX_BTC_JPY',
                        child_order_type='MARKET',
                        side=s,
                        size=size,
                        minute_to_expire=1,
                        time_in_force='GTC'
                    )
                    logging.debug(order)
                    if not quiet:
                        print('>>> {0} {1} BTC-FX/JPY'.format(s, size))
            else:
                time.sleep(interval)
        except ConnectionError:
            continue
