.. currentmodule:: qslib

Experiments
===========

Defining new experiments
------------------------

Protocols
^^^^^^^^^

.. autoclass:: qslib.experiment.Experiment
   :noindex:

.. autoclass:: qslib.Protocol
   :members:
   :noindex:

.. autoclass:: qslib.Stage
   :members:
   :noindex:

.. autoclass:: qslib.Step
   :members:
   :noindex:

Loading and saving existing experiments
---------------------------------------

Loading
^^^^^^^

.. automethod:: qslib.experiment.Experiment.from_file
   :noindex:

.. automethod:: qslib.experiment.Experiment.from_machine
   :noindex:

Saving
^^^^^^

.. automethod:: qslib.experiment.Experiment.save_file
   :noindex:

.. automethod:: qslib.experiment.Experiment.save_file_without_changes
   :noindex:

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

.. autoattribute:: qslib.experiment.Experiment.welldata
   :noindex:

.. autoattribute:: qslib.experiment.Experiment.temperatures
   :noindex:

Plotting
^^^^^^^^

.. automethod:: qslib.experiment.Experiment.plot_anneal_melt
   :noindex:

.. automethod:: qslib.experiment.Experiment.plot_over_time
   :noindex:

.. automethod:: qslib.experiment.Experiment.plot_protocol
   :noindex:

.. automethod:: qslib.experiment.Experiment.plot_temperatures
   :noindex:


Normalization
^^^^^^^^^^^^^

.. autoclass:: qslib.normalization.NormToMeanPerWell
   :noindex:

.. autoclass:: qslib.normalization.NormRaw
   :noindex:


Running and controlling experiments
-----------------------------------

.. automethod:: qslib.experiment.Experiment.run
   :noindex:

.. automethod:: qslib.experiment.Experiment.change_protocol
   :noindex:

.. automethod:: qslib.experiment.Experiment.pause_now
   :noindex:

.. automethod:: qslib.experiment.Experiment.resume
   :noindex:

.. automethod:: qslib.experiment.Experiment.stop
   :noindex:

.. automethod:: qslib.experiment.Experiment.abort
   :noindex:

