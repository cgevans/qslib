# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import List

import toml
from dacite.core import from_dict

from .monitor import Collector, Config
from .version import __version__

__author__ = "Constantine Evans"
__copyright__ = "Constantine Evans"
__license__ = "BSD-3-Clause"

_logger = logging.getLogger(__name__)


# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.


def parse_args(args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="QuantStudio qPCR data monitor for InfluxDB"
    )
    parser.add_argument(
        "--version",
        action="version",
        version="qs-monitor {ver}".format(ver=__version__),
    )
    parser.add_argument(
        dest="config", help="configuration file", type=Path, metavar="CONFIG_TOML"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """

    logformat = "[%(asctime)s] %(levelname)s:%(name)s: %(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%dT%H:%M:%S"
    )


def main(args: List[str]):
    """Wrapper allowing :func:`fib` to be called with string arguments in a CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    parsed_args = parse_args(args)
    setup_logging(parsed_args.loglevel)

    config = dict(toml.load(parsed_args.config))

    collector = Collector(from_dict(Config, config))

    asyncio.run(collector.reliable_monitor())


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    # ^  This is a guard statement that will prevent the following code from
    #    being executed in the case someone imports this file instead of
    #    executing it as a script.
    #    https://docs.python.org/3/library/__main__.html

    # After installing your project with pip, users can also run your Python
    # modules as scripts via the ``-m`` flag, as defined in PEP 338::
    #
    #     python -m qpcr_data_updater.skeleton 42
    #
    run()
