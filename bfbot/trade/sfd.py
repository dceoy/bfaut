#!/usr/bin/env python

import logging
import signal
import time
import numpy as np
import pybitflyer


def open_deal(config, interval=0, quiet=False):
    sfd_pin = np.array([- 0.2, - 0.15, - 0.1, 0.1, 0.15, 0.2])
    api = pybitflyer.API(
        api_key=config['bF']['api_key'], api_secret=config['bF']['api_secret']
    )
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if not quiet:
        print('>>> !!! OPEN SFD DEALS !!!')
    pending_orders = set()
    while True:
        keep_rate = api.getcollateral()['keep_rate']
        logging.debug('keep_rate: {}'.format(keep_rate))
        pos = api.getpositions(product_code='FX_BTC_JPY')
        position = {
            s: np.sum([p['size'] for p in pos if p['side'] == s])
            for s in ['SELL', 'BUY']
        }
        logging.debug('position: {}'.format(position))
        pending_orders = pending_orders.intersection({
            o['child_order_acceptance_id']
            for o in api.getchildorders(
                product_code='FX_BTC_JPY', child_order_state='ACTIVE'
            )
            if 'child_order_acceptance_id' in o
        })
        logging.debug('pending_orders: {}'.format(pending_orders))

        mp = {
            pc: api.board(product_code=pc)['mid_price']
            for pc in ['BTC_JPY', 'FX_BTC_JPY']
        }
        deviation = (mp['FX_BTC_JPY'] - mp['BTC_JPY']) / mp['BTC_JPY']
        logging.debug(
            'deviation: {0} (BTC/JPY: {1}, BTC-FX/JPY: {2})'.format(
                deviation, mp['BTC_JPY'], mp['FX_BTC_JPY']
            )
        )
        abs_diff = abs(sfd_pin - deviation)
        nearpin = (abs_diff < config['trade']['max_dist_open'])
        farpin = (abs_diff > config['trade']['min_dist_open'])
        logging.debug(
            'abs_diff: {0}, nearpin: {1}, farpin: {2}'.format(
                abs_diff, nearpin, farpin
            )
        )

        if (
                any(nearpin) and
                all(farpin) and
                max(position.values()) <= config['trade']['max_size'] and
                not pending_orders
        ):
            side = (
                'SELL' if np.asscalar(sfd_pin[nearpin]) < deviation else 'BUY'
            )
            order = api.sendchildorder(
                product_code='FX_BTC_JPY',
                child_order_type='MARKET',
                side=side,
                size=config['trade']['unit_size'],
                minute_to_expire=1,
                time_in_force='GTC'
            )
            if 'child_order_acceptance_id' in order:
                pending_orders.add(order['child_order_acceptance_id'])
            if not quiet:
                print('>>> {0} {1} BTC-FX/JPY'.format(
                    side, config['trade']['unit_size']
                ))
        elif (
            min(abs_diff) > config['trade']['min_dist_close'] or
            (keep_rate != 0 and keep_rate < config['trade']['min_keep_rate'])
        ):
            for side, size in position.items():
                s = {'SELL': 'BUY', 'BUY': 'SELL'}[side]
                api.sendchildorder(
                    product_code='FX_BTC_JPY',
                    child_order_type='MARKET',
                    side=s,
                    size=size,
                    minute_to_expire=1,
                    time_in_force='GTC'
                )
                if not quiet:
                    print('>>> {0} {1} BTC-FX/JPY'.format(s, size))
        else:
            time.sleep(interval)
