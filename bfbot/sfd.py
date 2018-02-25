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
    collateral = bF.getcollateral()
    logging.debug('collateral: {}'.format(collateral))
    cf = config['trade']['sfd']
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if not quiet:
        print('>>> !!! OPEN SFD DEALS !!!')

    while True:
        try:
            collateral = bF.getcollateral()
        except Exception:
            continue
        else:
            if 'keep_rate' in collateral:
                keep_rate = collateral['keep_rate']
                logging.debug('keep_rate: {}'.format(keep_rate))
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
            logging.debug('pos_side: {0}, pos_size: {1}'.format(
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
                limit_price = int(mp['FX_BTC_JPY'])
                abs_diff = abs(sfd_pin - deviation)
                min_abs_diff = abs_diff.min()
                nearest_pin = sfd_pin[abs_diff == min_abs_diff][0]
                logging.debug(
                    'min_abs_diff: {0}, nearest_pin: {1}'.format(
                        min_abs_diff, nearest_pin
                    )
                )
                order_side = ('SELL' if nearest_pin < deviation else 'BUY')
            else:
                continue

        if (
                cf['min_dist_open'] < min_abs_diff < cf['max_dist_open'] and
                pos_size <= cf['max_size'] and
                (pos_size == 0 or pos_side != order_side or keep_rate >= 0.8)
        ):
            try:
                order = bF.sendchildorder(
                    product_code='FX_BTC_JPY',
                    child_order_type='LIMIT',
                    side=order_side,
                    price=limit_price,
                    size=cf['unit_size'],
                    minute_to_expire=cf['minute_to_expire'],
                    time_in_force='IOC'
                )
            except Exception:
                continue
            else:
                logging.debug(order)
                if not quiet:
                    print('>>> {0} {1} BTC-FX/JPY with LIMIT at {2}'.format(
                        order_side, cf['unit_size'], limit_price
                    ))
        elif (
            0 < keep_rate < cf['min_keep_rate'] or
            (pos_side and min(abs_diff) > cf['min_dist_close'])
        ):
            rev_pos_side = {'SELL': 'BUY', 'BUY': 'SELL'}[pos_side]
            try:
                order = bF.sendchildorder(
                    product_code='FX_BTC_JPY',
                    child_order_type='MARKET',
                    side=rev_pos_side,
                    size=pos_size,
                    minute_to_expire=cf['minute_to_expire'],
                    time_in_force='GTC'
                )
            except Exception:
                continue
            else:
                logging.debug(order)
                if not quiet:
                    print('>>> {0} {1} BTC-FX/JPY with MARKET'.format(
                        rev_pos_side, pos_size
                    ))
        else:
            logging.debug('Skip by the criteria')
            time.sleep(interval)
