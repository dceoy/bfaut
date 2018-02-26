#!/usr/bin/env python

from setuptools import setup, find_packages
from bfbot import __version__


setup(
    name='bfbot',
    version=__version__,
    description='bfbot',
    packages=find_packages(),
    url='https://github.com/dceoy/bfbot',
    include_package_data=True,
    install_requires=[
        'docopt',
        'numpy',
        'pandas',
        'pubnub',
        'pyyaml',
        'pybitflyer',
        'tornado'
    ],
    entry_points={
        'console_scripts': ['bfbot=bfbot.cli:main'],
    }
)
