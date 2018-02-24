#!/usr/bin/env python
"""
bF bot

Usage:
    bfbot init [--debug] [--file=<yaml>]
    bfbot sfd [--debug] [--file=<yaml>] [--wait=<sec>] [--iter=<int>]
              [--dry-run]
    bfbot -h|--help
    bfbot -v|--version

Options:
    -h, --help      Print help and exit
    -v, --version   Print version and exit
    --debug         Execute a command with debug messages
    --file=<yaml>   Set a path to a YAML for configurations [$BFBOT_YML]
    --wait=<sec>    Wait seconds between orders [default: 0]
    --iter=<int>    Limit a number of executions

Commands:
    init            Generate a YAML template for configuration
    sfd             Open SFD trading
"""

import logging
import os
import sys
from docopt import docopt
from .. import __version__
from .util import set_log_config, set_config_yml, write_config_yml, \
                  read_yaml
from ..trade import sfd


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
        if args['sfd']:
            logging.debug('Open SFD trading')
            sfd.open_deal(
                config=config,
                n=(int(args['--iter']) if args['--iter'] else sys.maxsize),
                interval=float(args['--wait']),
                dry_run=args['--dry-run']
            )
