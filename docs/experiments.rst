Experiments
===========

.. autoclass:: qslib.common.Experiment

Defining new experiments
------------------------

Loading and saving existing experiments
---------------------------------------

Loading
^^^^^^^

.. automethod:: qslib.common.Experiment.from_file

.. automethod:: qslib.common.Experiment.from_machine

Saving
^^^^^^

.. automethod:: qslib.common.Experiment.save_file

.. automethod:: qslib.common.Experiment.save_file_without_changes

Information, data access and plotting
-------------------------------------

Information
^^^^^^^^^^^

Data
^^^^

.. autoattribute:: qslib.common.Experiment.welldata

.. autoattribute:: qslib.common.Experiment.temperature

Plotting
^^^^^^^^

.. automethod:: qslib.common.Experiment.tcplot

.. automethod:: qslib.common.Experiment.plot_anneal_melt

.. automethod:: qslib.common.Experiment.plot_over_time

.. automethod:: qslib.common.Experiment.plot_temperatures


Normalization
^^^^^^^^^^^^^

.. autoclass:: qslib.common.NormalizeToMeanPerWell

Running and controlling experiments
-----------------------------------

.. automethod:: qslib.common.Experiment.run

.. automethod:: qslib.common.Experiment.change_protocol

.. automethod:: qslib.common.Experiment.pause_now

.. automethod:: qslib.common.Experiment.resume

.. automethod:: qslib.common.Experiment.stop

.. automethod:: qslib.common.Experiment.abort

