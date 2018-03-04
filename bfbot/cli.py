#!/usr/bin/env python
"""
bF bot trader

Usage:
    bfbot stream [--debug] [--ch=<type>] [--sqlite=<path>] [--quiet]
                 [<product>...]
    bfbot init [--debug] [--file=<yaml>]
    bfbot auto [--debug] [--pair=<code>] [--file=<yaml>] [--quiet]
    bfbot -h|--help
    bfbot -v|--version

Options:
    -h, --help          Print help and exit
    -v, --version       Print version and exit
    --debug             Execute a command with debug messages
    --ch=<type>         Set a lightning channel type [default: ticker]
    --sqlite=<path>     Save data in an SQLite3 database
    --pair=<code>       Set an actual currency pair [default: BTC_JPY]
    --file=<yaml>       Set a path to a YAML for configurations [$BFBOT_YML]
    --quiet             Suppress messages

Commands:
    init                Generate a YAML template for configuration
    stream              Stream rate
    auto                Open autonomous trading

Arguments:
    <product>           Product codes [default: FX_BTC_JPY]
"""

import logging
import os
from docopt import docopt
from . import __version__
from .streamer import stream_rate
from .trader import open_deal
from .util import set_log_config, set_config_yml, write_config_yml, \
                  read_yaml


def main():
    args = docopt(__doc__, version='bfbot {}'.format(__version__))
    set_log_config(debug=args['--debug'])
    logging.debug('args:{0}{1}'.format(os.linesep, args))
    config_yml = set_config_yml(path=args['--file'])

    if args['init']:
        logging.debug('Generate a config file')
        write_config_yml(path=config_yml)
    elif args['stream']:
        logging.debug('Stream rate')
        stream_rate(
            products=(args['<product>'] or ['FX_BTC_JPY']),
            ch_type=args['--ch'],
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
                quiet=args['--quiet']
            )
