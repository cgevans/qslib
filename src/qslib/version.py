# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("qslib")
except PackageNotFoundError:  # pragma: no cover
    # package is not installed
    __version__ = "dev"
