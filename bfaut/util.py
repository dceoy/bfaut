#!/usr/bin/env python

import logging
import os
import shutil
import yaml


class BfautError(Exception):
    pass


def read_yaml(path):
    with open(path) as f:
        d = yaml.load(f)
    return d


def set_config_yml(path=None, env='BFAUT_YML', default='bfaut.yml'):
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
            os.path.join(os.path.dirname(__file__), 'bfaut.yml'), path
        )
        print('A YAML template was generated: {}'.format(path))
