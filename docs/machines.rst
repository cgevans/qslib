.. SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
..
.. SPDX-License-Identifier: AGPL-3.0-only

.. _machines:

.. currentmodule:: qslib


Communicating with Machines
===========================

Access and addressing
---------------------

Connections to a machine are handled through the :class:`Machine` class, or a :any:`MachineReference`, which may be a
:class:`Machine` or, if the machine has no password or other requirements, just the hostname string.

.. autosummary::
   Machine

Connections, Access Levels, and Passwords
-----------------------------------------

By default, as of 0.5.0, Machine automatically handles connections, disconnections, and, when possible (eg, for commands
within qslib, rather than an arbitrary :func:`Machine.run_command`), access levels.

The QuantStudio SCPI interface controls access through passwords, which have no username associated; these passwords are
each associated with a maximum access level.  Each connection type (usually ethernet) also has an associated default
and maximum access level with no password.

The recommended settings for QSLib, which can be set through the QSLib CLI,
has a connection granted Observer access by default, and Controller access without a password.

When :code:`automatic=True`, which is the default, whenever code needing a connection is run, a connection will be made,
then disconnected as soon as the code is finished, if the machine wasn't already connected.  When :code:`automatic=False`,
connections can be handled manually through either context managers (safer) or individual commands. For clean access, the class
provides a context manager for the connection, and the :func:`Machine.at_level` method provides a context manager for access level:

>>> with Machine('machine', 'password', max_access_level="Controller") as m:
>>>     # Now connected
>>>     print(m.run_status())  # runs at Observer level
>>>     with m.at_level("Controller", exclusive=True):
>>>         # Runs as Controller, and fails if another Controller is connected (exclusive)
>>>         m.abort_run()
>>>         m.drawer_open()
>>>     # Now back to Observer
>>>     print(m.status())
>>> # Now disconnected.

There is also the :any:`Machine.ensured_connection` context manager, which will ensure that a connection exists at a particular
access level, or arrange one if it does not.  The connection context manager can also be used with :code:`with m:` form for a Machine
instance :code:`m` that already exists, in which case it will connect and disconnect.

If you don't want to use these, you can also use manual methods:

.. autosummary::
   Machine.connect
   Machine.set_access_level
   Machine.disconnect

Note that there is *no supported method* on the machine's server for removing hanging connections other than a reboot,
and AB's software will not start runs when other connections hold Controller level.  To correct this, you can restart the
machine with :any:`Machine.restart_system`.


Utility commands
----------------

.. autosummary::
   Machine.drawer_open
   Machine.drawer_close
   Machine.drawer_position
   Machine.cover_lower
   Machine.cover_position
   Machine.status
   Machine.run_status
   Machine.access_level
   Machine.current_run_name
   Machine.abort_current_run
   Machine.stop_current_run
   Machine.pause_current_run
   Machine.pause_current_run_at_temperature
   Machine.resume_current_run
   Machine.power


Lower-level commands
--------------------

.. autosummary::
   Machine.run_command
   Machine.run_command_to_ack
