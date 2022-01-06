.. _machines:

.. currentmodule:: qslib


Communicating with Machines
===========================

.. autoclass:: qslib.machine.Machine
   :noindex:
   :members:

Access and addressing
---------------------

By default, 

For clean access, the class provides a context manager for the connection, and the
`at_level` method provides a context manager for access level:

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

The connection context manager can also be used with :code:`with m:` form for a Machine
instance :code:`m` that already exists, in which case it will connect and disconnect.

If you don't want to use these, you can also use :any:`connect` and :any:`disconnect`.

Note that there is *no supported method* on the machine's server for removing hanging connections
other than a reboot, and AB's software will not start runs when other connections hold Controller
level.


.. autoclass:: qslib.AccessLevel
   :noindex:
   

Utility commands
----------------