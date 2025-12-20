# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

"""
    Dummy conftest.py for qslib.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    - https://docs.pytest.org/en/stable/fixture.html
    - https://docs.pytest.org/en/stable/writing_plugins.html
"""

# Use non-GUI backend for matplotlib (needed for CI environments without display)
import matplotlib
matplotlib.use('Agg')

# import pytest
