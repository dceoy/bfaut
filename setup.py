#!/usr/bin/env python

from setuptools import setup, find_packages
from bfaut import __version__


setup(
    name='bfaut',
    version=__version__,
    description='Bot trader for bF',
    packages=find_packages(),
    url='https://github.com/dceoy/bfaut',
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
        'console_scripts': ['bfaut=bfaut.cli:main'],
    }
)
