.. SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
..
.. SPDX-License-Identifier: EUPL-1.2

.. _monitor:

Monitor system
==============

Setup
-----

Configuration
-------------

Each machine uses direct SCPI subscriptions by default. Omit ``server_port``
for this mode. Setting ``server_port`` explicitly selects the optional
qslib-server mode, which consumes semantic status and resumable SSE events
without opening a normal client-side SCPI connection. Both modes emit the same
Influx measurements.

.. code:: toml

   [[machines]]
   name = "qpcr1"
   host = "qpcr1"
   # server_port = 7500
   # server_token = "observer-token"  # optional with an unauthenticated ACL role

Command line
------------
