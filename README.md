<!--
SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>

SPDX-License-Identifier: AGPL-3.0-only
-->

[![Documentation Status](https://readthedocs.org/projects/qslib/badge/?version=latest)](https://qslib.readthedocs.io/en/latest/?badge=latest)
![Codecov](https://img.shields.io/codecov/c/github/cgevans/qslib)
![GitHub Workflow
Status](https://img.shields.io/github/workflow/status/cgevans/qslib/Python%20tests)
![PyPI](https://img.shields.io/pypi/v/qslib)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/5512/badge)](https://bestpractices.coreinfrastructure.org/projects/5512)
[![DOI](https://zenodo.org/badge/393710481.svg)](https://zenodo.org/badge/latestdoi/393710481)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/qslib)](https://pypi.org/project/qslib/)




Documentation: [Stable](https://qslib.readthedocs.io/en/stable/), [Latest](https://qslib.readthedocs.io/en/latest/)


# qslib

QSLib is a package for interacting with Applied Biosystems' QuantStudio
qPCR machines, intended for non-qPCR uses, such as DNA computing and
molecular programming systems. It allows the creation, processing, and
handling of experiments and experiment data, and interaction with
machines through their network connection and SCPI interface. It currently
functions only with QuantStudio 5 machines using a 96-well block, but
could be made to support others as well.

Amongst other features that it has:

-   Direct fluorescence data ("filter data") as Pandas dataframes, with
    times and temperature readings.

-   Running-experiment data access, status information, and control.

-   Protocol creation and manipulation, allowing functions outside of
    AB's software. Protocols can be modified and updated mid-run.

-   Temperature data at one-second resolution during experiments.

-   Machine control functions: immediate pauses and resumes, drawer
    control, power, etc.

-   With qslib-monitor: live monitoring of machine state information,
    with Matrix notifications, InfluxDB storage, and Grafana dashboards.

## Installation and Setup

QSLib is pure Python, and can be installed via pip:

    pip3 install -U qslib

Or, for the current Github version:

    pip3 install -U --pre git+https://github.com/cgevans/qslib

It requires at least version 3.9 of Python. While it uses async code at
its core for communication, it can be used conveniently in Jupyter or
IPython.

To use the library for communication with machines, you'll need a
machine access password with Observer (for reading data and statuses)
and/or Controller (for running experiments and controlling the machine)
access, and will need access to the machine on port 7000. I strongly
recommend against having the machines be accessible online: use a
restricted VPN connection or port forwarding. See the documentation for
more information.

## Contributing and issue reporting

Issue reports and enhancement requests can be submitted via Github.

Potential contributions can be submitted via Github.  These should include pytest tests, preferably
both tests that can be run without outside resources, and, if applicable, tests that directly test
any communication with a QuantStudio SCPI server.  They will also need a Contributor Licence Agreement.

Private vulnerability reports can be sent to me by
email, PGP-encrypted, or via Matrix to [@cge:matrix.org](https://matrix.to/#/@cge:matrix.org).

## Disclaimer

This package was developed for my own use. It may break your machine or
void your warranty. Data may have errors or be incorrect. When used to
send raw commands at high access levels, the machine interface could
render your machine unusable or be used to send commands that would
physically/electrically damage the machine or potentially be hazardous
to you or others.

I am not any way connected with Applied Biosystems.  I have developed this
package using the machine's documentation system and standard file formats.
