[![Documentation Status](https://readthedocs.org/projects/qslib/badge/?version=latest)](https://qslib.readthedocs.io/en/latest/?badge=latest)
![Codecov](https://img.shields.io/codecov/c/github/cgevans/qslib)
![GitHub Workflow
Status](https://img.shields.io/github/workflow/status/cgevans/qslib/Python%20tests)
![PyPI](https://img.shields.io/pypi/v/qslib)

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

## Disclaimer

This package was developed for my own use. It may break your machine or
void your warranty. Data may have errors or be incorrect. When used to
send raw commands at high access levels, the machine interface could
render your machine unusable or be used to send commands that would
physically/electrically damage the machine or potentially be hazardous
to you or others.

I am not any way connected with Applied Biosystems.  I have developed this 
package using the machine's documentation system and standard file formats.
