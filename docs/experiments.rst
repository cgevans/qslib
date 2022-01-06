.. currentmodule:: qslib

Experiments
===========

Defining new experiments
------------------------

.. autosummary::
   Experiment


Protocols
^^^^^^^^^

.. autosummary::
   Protocol
   Stage
   Step



.. autosummary::
   CustomStep
   Exposure

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
   qslib.normalization.NormToMeanPerWell
   qslib.normalization.NormRaw


Running and controlling experiments
-----------------------------------

.. autosummary::
   Experiment.run
   Experiment.change_protocol
   Experiment.pause_now
   Experiment.resume
   Experiment.stop
   Experiment.abort

