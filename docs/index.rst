.. SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
..
.. SPDX-License-Identifier: AGPL-3.0-only

qslib |version|
===============


.. currentmodule:: qslib

Introduction
------------

QSLib is a package for interacting with Applied Biosystems' QuantStudio
qPCR machines, intended for non-qPCR uses, such as DNA computing and
molecular programming systems.

There are a few different ways to use QSLib:

- Through its command-line script, `qslib`, which has a number of subcommands allowing basic machine control, experiment
  file information, and data export.

- As a library in Python, often either in the REPL, or via a notebook system like Jupyter.  QSLib uses nest-asyncio to
  allow it to operate seamlessly within Jupyter notebooks.  This allows experiment creation, modification, and processing,
  data plotting, machine control, and all other features.  Machine communication, however, does require some setup.

- As a monitoring system integrated with Matrix and InfluxDB.  This allows real-time storage of machine and run state
  and fluorescence data, and almost-live synchronization of in-progress and finished experiment files (in-progress
  files may not be usable by AB's software).

Basic use within Python
-----------------------

Basic of QSLib is mostly built around the :class:`Experiment` and :class:`Machine` classes, along with several classes for
defining a :class:`PlateSetup` and temperature :class:`Protocol`.

By default (as of v0.5.0), most methods communicating with a machine are "automatic": they handle connection
and disconnection, and access levels, automatically.  They are also written so that, if there is no passsword
required, the machine's hostname as a string can be used as a reference to the machine.  So, for example, if you
want to run an experiment :code:`experiment` on machine `example-qs5`, you could use: :code:`experiment.run("example-qs5")`, or,
to load an experiment named `my-experiment` from a machine, you could use
:code:`experiment = Experiment.from_machine("example-qs5", "my-experiment")`.  Similarly, to open the machine's drawer,
you could use :code:`Machine("example-qs5").drawer_open()`.

Documention contents
--------------------

.. toctree::
    tutorial
    experiments
    machines
    commandline
    monitor
    setup

.. toctree::
   :maxdepth: 3

   API Reference <api/modules>

.. toctree::
   :maxdepth: 2

   changelog
   License <LICENSE.txt>
   authors


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
