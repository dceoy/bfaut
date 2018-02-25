#!/usr/bin/env python
"""
bF bot trader

Usage:
    bfbot init [--debug] [--file=<yaml>]
    bfbot auto [--debug] [--file=<yaml>] [--wait=<sec>] [--quiet]
    bfbot -h|--help
    bfbot -v|--version

Options:
    -h, --help      Print help and exit
    -v, --version   Print version and exit
    --debug         Execute a command with debug messages
    --file=<yaml>   Set a path to a YAML for configurations [$BFBOT_YML]
    --wait=<sec>    Wait seconds between orders [default: 0]
    --quiet         Suppress messages

Commands:
    init            Generate a YAML template for configuration
    auto            Open autonomous trading
"""

import logging
import os
from docopt import docopt
from . import __version__
from .trader import open_deal
from .util import set_log_config, set_config_yml, write_config_yml, \
                  read_yaml


def main():
    args = docopt(__doc__, version='bfbot {}'.format(__version__))
    set_log_config(debug=args['--debug'])
    logging.debug('args:{0}{1}'.format(os.linesep, args))
    config_yml = set_config_yml(path=args['--file'])

    if args['init']:
        logging.debug('Initiation')
        write_config_yml(path=config_yml)
    else:
        logging.debug('config_yml: {}'.format(config_yml))
        config = read_yaml(path=config_yml)
        if args['auto']:
            logging.debug('Open autonomous trading')
            open_deal(
                config=config,
                interval=float(args['--wait']),
                quiet=args['--quiet']
            )
