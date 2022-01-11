.. SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
..
.. SPDX-License-Identifier: AGPL-3.0-only

Tutorial
========

Defining a new experiment
-------------------------

Experiments are generally defined by two parts: a temperature :any:`Protocol`, which tells the machine how to run the experiment, and a :any:`PlateSetup`, which describes the samples in the experiment in some level of detail, but does not change how the machine runs the experiment, and can be changed at any time, including after the run has completed.

Protocols are hierarchical sets of instructions with three levels:

- A Protocol is made up of :any:`Stage`\ s, which it runs in sequence.  Each Stage repeats for one or more cycles.

- A Stage, for every cycle, runs a sequence of one or more Steps.  The Steps may have changes ('increments') every cycle, for example, making a hold longer, or lowering a temperature.  The usual :any:`Step`, which is mostly compatible with AB's software, holds for some amount of time at some temperatures, and then optionally takes a fluorescence reading at the end of the step.

- However, the QuantStudio server actually sees a step as a sequence of several SCPI (machine) commands, which may implement something other than a standard Step, for example, changing exposure settings.  QSLib implements this as a :any:`CustomStep`.  Steps may *also* repeat their commands some number of times, which AB's software calls 'points' instead of cycles.

QSLib also has convenience methods for constructing some common stages:

.. autosummary::

    Stage.stepped_ramp
    Stage.hold_for

So, to make a protocol that does a quick anneal and melt, then holds at 50°C to 45°C, with some interesting
details, we could do:

>>> protocol = Protocol(
        [
            Stage.stepped_ramp("80 °C", 60, "20 minutes"),  # A unitless temperature is degrees Celsius
            Stage.hold_for("60 degC", total_time=120),      # A unitless time is seconds
            Stage.stepped_ramp(from_temperature=60.0, to_temperature=30.0, total_time="1 hour", collect=True),
            # Collects the protocol default
            Stage.stepped_ramp(from_temperature=30, to_temperature=60.0, total_time="1 hour", temperature_step=2, collect=True),
            Stage.hold_for("60 degC", total_time="9 minutes", step_time="3 minutes",
                        filters=["x1-m4", "x3-m5"]), # Collects a different set of filters
            Stage.stepped_ramp(60, [50, 49, 48, 47, 46, 45], total_time="40 minutes", filters=["x1-m4"], n_steps=5), # Try this in AB's software!
            Stage.hold_for([50, 49, 48, 47, 46, 45], total_time="1 hour", step_time="10 minutes", filters=["x1-m4"]),
            Stage([Step("10 minutes", [50, 49, 48, 47, 46, 45], filters=["x1-m4"])], repeat=6) # An alternative
        ], filters=["x3-m5"]
    )

Plate setups have :ref:`Sample`\ s, which may be in one or more wells.  So, to create a simple plate setup, we might do:

>>> plate_setup = PlateSetup({"sample_A": "A1", "sample_B": ["A2", "A3"]})

There is also the function :ref:`PlateSetup.from_array`, if you have an plate-sized array of sample names.

However, note that the plate setup does not change the data the machine collects: all filters collected by the protocol are collected for all wells, in the same way.

With these two components, you can create an experiment, which also needs a name (spaces are supported, and unicode is somewhat supported, but discretion on these is advisable):

>>> exp = Experiment("example-experiment", protocol, plate_setup)

At this point, you might want to check that your protocol is doing what you expect it to do.  A convenient way of checking is to plot it:

>>> exp.plot_protocol()

You could also use :code:`protocol.plot_protocol()`.

You can get a summary of the experiment with

>>> print(exp)

Or, if you're running Jupyter, you might want to try just

>>> exp

or

>>> display(exp)

Running and controlling the experiment
--------------------------------------

QSLib's preferred setup for connections is to have the QuantStudio machine isolated from the internet, with no password required for Observer and Controller access, and with port 7000 accessible only from selected computers.  See :ref:`setup` for more information.  With this setup, many commands in QSLib can simply take the machine's hostname (or, if using an SSH tunnel, :code:`"localhost"`), in order to communicate with it.  So, if we have a machine with hostname example-qpcr, and we'd like to run our experiment from above on it, we could do:

>>> exp.run("example-qpcr")

But perhaps we'd like to make sure the machine is free first:

>>> Machine("example-qpcr").run_status()

As the run progresses, we can get its status from the machine.  Once we've run the experiment,
it should remember (on our computer) the machine it is runnnig on, so we could get the status with:

>>> exp.get_status()

We can, with reasonable efficiency, update the experiment on our computer with the latest data from the machine using

>>> exp.sync_from_machine()

Then, we can use any of the methods below in :ref:`data`.

At some point, we might want to pause the run, and take out a sample.

>>> exp.pause()

or, if we want to specify the machine:

>>> exp.pause("example-qpcr")

Then:

>>> Machine("example-qpcr").drawer_open()

And after handling our samples,

>>> Machine("example-qpcr").drawer_close()
>>> exp.resume()

For a long run, we may not keep Python running the whole time, and so we may need to reload the experiment from the machine.  This can be done with:

>>> exp = Experiment.from_running("example-qpcr")

Eventually, we might decide that we'd like the rest of our protocol to be different, for example, to add a new hold temperature.  We can do this by making a new protocol:

>>> prot = exp.protocol.copy()
>>> prot.stages[7].repeat = 20
>>> prot.stages[7].steps[0].temperature = [49, 48, 47, 46, 45, 44]
>>> exp.change_protocol(prot)

QSLib will ensure that the new protocol is compatible with the running protocol before replacing it.  Generally, in order to be compatible:

- Every stage that *has already done* must be the same.
- The *current* stage can't have its steps modified (one or more cycles may have already run), but can have the number of repeats/cycles changed, so long as the number is greater than or equal to the current cycle number (keep in mind that for fast stages, the machine may finish a cycle while you are writing the new protocol).  This is primarily useful for extending, or cutting short, the current cycle.  (FIXME: it may be useful to have a "I don't care what the current cycle is, just end the stage after the current one" designation, like setting the repeats to -1.)
- Anything that is *past* the current stage can be modified arbitrarily (new stages can be added, and stages can be deleted or modified in any way).

At some point, we may want to stop the run before it is finished.  There are two options.  We can use :any:`Experiment.stop` to stop the run when the current cycle is done, or we can use :any:`Experiment.abort` to stop it immediately.

After a run is finished, we can use :any:`Experiment.sync_from_machine` to update our copy, or we can get a clean copy:

>>> exp = Experiment.from_machine("example-experiment")

You can save the experiment to a file with

>>> exp.save_file("example-file-name.eds")

If you want to save power, you can also put the machine in power-save mode when you are done:

>>> Machine("example-qpcr").power = False

Loading existing experiments
----------------------------

.. autosummary::
    Experiment.from_running
    Experiment.from_machine
    Experiment.from_file

Accessing experiment data
-------------------------

QSLib can provide access to most data collected by QuantStudio machines, at a lower level and significantly beyond what AB's software provides (please contact me if there is something you'd like easier access to).  However, the two main items are temperature readings during the run, and machine-calibrated, but not fluorophore-calibrated, fluorescence measurements, roughly what you might expect as a reasonably raw reading (with some no-fluorophore background subtraction).

Temperature data are in :ref:`Experiment.temperatures`, and fluorescence data (and temperature readings during collection) are in :ref:`Experiment.welldata`.

To plot temperatures, you can use:

>>> exp.plot_temperatures()

To plot fluorescence data, you can use:

>>> exp.plot_over_time()

If your experiment included an anneal and melt, you can also use the :ref:`Experiment.plot_anneal_melt` method:

>>> exp.plot_anneal_melt()

Both of these functions are designed to be flexible in selection of samples or wells, filters, and stages, cycles, and steps.

Other topics and possibilities
------------------------------
