# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("qslib")
except PackageNotFoundError:  # pragma: no cover
    # package is not installed
    __version__ = "dev"
