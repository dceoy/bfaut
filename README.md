bfaut
=====

Autonomous trader for bF

[![wercker status](https://app.wercker.com/status/024c56acaeb561b9b37f93f0fe284c56/s/master "wercker status")](https://app.wercker.com/project/byKey/024c56acaeb561b9b37f93f0fe284c56)

Installation
------------

```sh
$ pip install -U git+https://github.com/dceoy/bfaut.git
```

Usage
-----

```sh
$ bfaut --help
Autonomous trader for bF

Usage:
    bfaut stream [--debug] [--sqlite=<path>] [--quiet] [<channel>...]
    bfaut init [--debug] [--file=<yaml>]
    bfaut state [--debug] [--file=<yaml>] [--pair=<code>]
    bfaut auto [--debug|--info] [--file=<yaml>] [--pair=<code>] [--pivot]
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
    --pivot             Enable automatic trading pivot
    --timeout=<sec>     Set senconds for timeout [default: 3600]
    --quiet             Suppress messages

Commands:
    stream              Stream rate
    init                Generate a YAML template for configuration
    state               Print states of market and account
    auto                Open autonomous trading

Arguments:
    <channel>...        PubNub channels [default: lightning_ticker_BTC_JPY]
```
