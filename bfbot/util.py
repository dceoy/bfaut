#!/usr/bin/env python

import logging
import os
import shutil
import yaml


class BfbotError(Exception):
    pass


def set_log_config(debug=False):
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.DEBUG if debug else logging.WARNING)


def read_yaml(path):
    with open(path) as f:
        d = yaml.load(f)
    return d


def dump_yaml(dict, flow=False):
    return yaml.dump(dict, default_flow_style=flow)


def set_config_yml(path=None, env='BFBOT_YML', default='bfbot.yml'):
    return os.path.expanduser(
        tuple(filter(
            lambda p: p is not None, [path, os.getenv(env), default]
        ))[0]
    )


def write_config_yml(path):
    if os.path.exists(path):
        print('The file already exists: {}'.format(path))
    else:
        logging.debug('Write {}'.format(path))
        shutil.copyfile(
            os.path.join(os.path.dirname(__file__), 'bfbot.yml'), path
        )
        print('A YAML template was generated: {}'.format(path))


class ConsoleHelper(object):
    def __init__(self, quiet):
        self.quiet = quiet

    def print_log(self, message):
        text = '>>>\t{}'.format(message)
        if self.quiet:
            logging.debug(text)
        else:
            print(text, flush=True)
