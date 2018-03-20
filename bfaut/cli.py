#!/usr/bin/env python
"""
Autonomous trader for bF

Usage:
    bfaut stream [--debug] [--sqlite=<path>] [--quiet] [<channel>...]
    bfaut init [--debug] [--file=<yaml>]
    bfaut state [--debug] [--file=<yaml>] [--pair=<code>] [<item>...]
    bfaut auto [--debug|--info] [--file=<yaml>] [--pair=<code>]
               [--timeout=<sec>] [--quiet]
    bfaut -h|--help
    bfaut -v|--version

Options:
    -h, --help          Print help and exit
    -v, --version       Print version and exit
    --debug             Execute a command with debug messages
    --sqlite=<path>     Save data in an SQLite3 database
    --file=<yaml>       Set a path to a YAML for configurations [$BFAUT_YML]
    --pair=<code>       Set an actual currency pair [default: BTC_JPY]
    --timeout=<sec>     Set senconds for timeout [default: 3600]
    --quiet             Suppress messages

Commands:
    stream              Stream rate
    init                Generate a YAML template for configuration
    state               Print states of market and account
    auto                Open autonomous trading

Arguments:
    <channel>...        PubNub channels [default: lightning_ticker_BTC_JPY]
    <item>...           State types { balance, collateral, orders, positions }
"""

import logging
import os
from docopt import docopt
from . import __version__
from .info import print_states, stream_rate
from .trader import open_deal
from .util import set_config_yml, write_config_yml, read_yaml


def main():
    args = docopt(__doc__, version='bfaut {}'.format(__version__))
    set_log_config(args)
    logging.debug('args:{0}{1}'.format(os.linesep, args))
    config_yml = set_config_yml(path=args['--file'])

    if args['init']:
        logging.debug('Generate a config file')
        write_config_yml(path=config_yml)
    elif args['stream']:
        logging.debug('Stream rate')
        stream_rate(
            channels=(args['<channel>'] or ['lightning_ticker_BTC_JPY']),
            sqlite_path=args['--sqlite'],
            quiet=args['--quiet']
        )
    else:
        logging.debug('config_yml: {}'.format(config_yml))
        config = read_yaml(path=config_yml)
        if args['auto']:
            logging.debug('Open autonomous trading')
            open_deal(
                config=config,
                pair=args['--pair'],
                timeout=args['--timeout'],
                quiet=args['--quiet']
            )
        elif args['state']:
            logging.debug('Print states')
            print_states(
                config=config,
                pair=args['--pair'],
                items=args['<item>']
            )


def set_log_config(args):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=(
            logging.DEBUG if args['--debug'] else (
                logging.INFO if args['--info'] else logging.WARNING
            )
        )
    )
