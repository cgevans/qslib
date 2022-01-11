.. SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
..
.. SPDX-License-Identifier: AGPL-3.0-only

.. currentmodule:: qslib

Experiments
===========

Defining new experiments
------------------------

.. autosummary::
   Experiment


Protocols
^^^^^^^^^

A basic :class:`Protocol` is generally made up of :class:`Stage` s, which are made up of :class:`Step` s.
These are often compatible with AB's software.

.. autosummary::
   Protocol
   Stage
   Step

While the parameters for these classes default to seconds and degrees Celsius, when using numbers as input, they also use the
pint library to accept strings (or pint Quantity objects).  Thus, you can use :code:`"1 hr"` or :code:`"1 hour"` instead of :code:`3600`,
or :code:`"59 °C"` or :code:`"59 degC"` for a temperature.  Note that for temperature increments, pint distinguishes between a temperature
change and absolute temperature unit, so you would need to use, for example :code:`"-1 delta_degC"`.

QSLib also supports custom steps, which can contain arbitrary SCPI commands.  For common commands, it also
includes classes that allow more convenient use:

.. autosummary::
   CustomStep
   protocol.Exposure
   protocol.Hold
   protocol.Ramp
   protocol.HACFILT
   protocol.HoldAndCollect
   scpi_commands.SCPICommand

Once created, there are several useful methods:

.. autosummary::
   Protocol.info
   Protocol.plot_protocol
   Protocol.dataframe
   Protocol.all_times
   Protocol.all_filters
   Protocol.check_compatible
   Protocol.copy

Plates
^^^^^^

.. autosummary::
   PlateSetup

Loading and saving existing experiments
---------------------------------------

Loading
^^^^^^^

.. autosummary::
   Experiment.from_file
   Experiment.from_machine
   Experiment.from_running

Saving
^^^^^^

.. autosummary::
   Experiment.save_file
   Experiment.save_file_without_changes

Information, data access and plotting
-------------------------------------

Information
^^^^^^^^^^^

.. autosummary::
   Experiment.name
   Experiment.get_status
   Experiment.createdtime
   Experiment.activestarttime
   Experiment.runstarttime
   Experiment.activeendtime
   Experiment.runendtime
   Experiment.all_filters
   Experiment.sample_wells
   Experiment.info
   Experiment.info_html


Data
^^^^

.. autosummary::
   Experiment.welldata
   Experiment.temperatures

Plotting
^^^^^^^^

.. autosummary::
   Experiment.plot_anneal_melt
   Experiment.plot_over_time
   Experiment.plot_protocol
   Experiment.plot_temperatures


Normalization
^^^^^^^^^^^^^

.. autosummary::
   NormToMeanPerWell
   NormToMaxPerWell
   NormRaw


Running and controlling experiments
-----------------------------------

.. autosummary::
   Experiment.run
   Experiment.change_protocol
   Experiment.pause_now
   Experiment.resume
   Experiment.stop
   Experiment.abort
