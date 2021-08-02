# qslib

QSLib is a package for interacting with Applied Biosystems' QuantStudio
qPCR machines, intended for non-qPCR uses, such as DNA computing and
molecular programming systems.

## Description

QSLib is a package for interacting with Applied Biosystems' QuantStudio
qPCR machines, intended for non-qPCR uses, such as DNA computing and
molecular programming systems. It allows the creation, processing, and
handling of experiments and experiment data, and interaction with
machines through their network connection and SCPI interface.

Amongst other features that :

-   Direct fluorescence data ("filter data") as Pandas dataframes, with
    times and temperature readings.

-   Running-experiment data access, status information, and control.

-   Protocol creation and manipulation, allowing functions outside of
    AB's software. The ability to update protocols and add stages
    mid-run is feasible and planned.

-   Temperature data at one-second resolution during experiments.

-   Machine control functions: immediate pauses and resumes, drawer
    control, power, etc.

-   With qslib-monitor: live monitoring of machine state information,
    with Matrix notifications, InfluxDB storage, and Grafana dashboards.

## Installation

## Setup

## Disclaimer

This package was developed for my use.
