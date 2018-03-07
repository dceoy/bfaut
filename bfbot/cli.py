#!/usr/bin/env python
"""
bF bot trader

Usage:
    bfbot stream [--debug] [--sqlite=<path>] [--quiet] [<channel>...]
    bfbot init [--debug] [--file=<yaml>]
    bfbot auto [--debug|--info] [--file=<yaml>] [--pair=<code>] [--wait=<sec>]
               [--ifdoco] [--quiet]
    bfbot -h|--help
    bfbot -v|--version

Options:
    -h, --help          Print help and exit
    -v, --version       Print version and exit
    --debug             Execute a command with debug messages
    --sqlite=<path>     Save data in an SQLite3 database
    --file=<yaml>       Set a path to a YAML for configurations [$BFBOT_YML]
    --pair=<code>       Set an actual currency pair [default: BTC_JPY]
    --wait=<sec>        Wait for loading [default: 10]
    --ifdoco            Use IFDOCO order for opening
    --quiet             Suppress messages

Commands:
    init                Generate a YAML template for configuration
    stream              Stream rate
    auto                Open autonomous trading

Arguments:
    <channel>...        PubNub channels [default: lightning_ticker_BTC_JPY]
"""

import logging
import os
from docopt import docopt
from . import __version__
from .streamer import stream_rate
from .trader import open_deal
from .util import set_config_yml, write_config_yml, read_yaml


def main():
    args = docopt(__doc__, version='bfbot {}'.format(__version__))
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
                wait=args['--wait'],
                ifdoco=args['--ifdoco'],
                quiet=args['--quiet']
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
