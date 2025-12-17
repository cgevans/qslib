<!--
SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>

SPDX-License-Identifier: EUPL-1.2
-->

# Changelog

## Version 0.14.0

A significant reorganization:

- Core communication and parsing code in Rust.
- Polars dataframe support, in addition to pandas.
- A new, rust-based qs-monitor with improved reliability and a matrix bot with commands.
- Altair plotting support.
- Numerous bug fixes and improvements.
- Python 3.10 is now the minimum supported Python.

## Version 0.13.0

- Add `PlateSetup.from_picklist` method to generate a PlateSetup from a Kithairon PickList, if Kithairon is installed.
- Improve ssl support for monitor.
- Have monitor complain to matrix if it exits unexpectedly.
- Fix compile_eds when there is no folder to compile (if, eg, something else has already compiled the EDS).

## Version 0.12.0

- `Machine.block` command to directly control block temperatures.
- Certificate support for SSL, and new connection recommendations.
- Stage lines and event spans now are now not plotted if they are outside of the selected time range / stages.
- `Step` now supports repeats.
- `Stage.stepped_ramp` can now take multiple data points per temperature.
- Fix hanging connection bug.

## Version 0.11.0

- Initial support for 384-well blocks, at least in data/file reading.
- Initial support for v2.0 EDS specification machines (eg, QS6Pro), at least in data/file reading.
- Parsing of multicomponent data for v1 and v2 machines, and partial analysis data for v1 machines.
- Available data is shown in experiment information.
- Change license to EUPL-1.2.
- Fixes for new matplotlib versions.
- Support for Python 3.12.

## Version 0.10.1

- SSL/non-SSL autoconnection speed improvements
- Drawer/cover annotation improvements

## Version 0.10.0

- Add annotations to plots for drawer and cover open and close events.
- Add support for SSL / firmware 1.3.4.

## Version 0.9.4

- Fix multi-temperature plotting bug.

## Version 0.9.3

- Update dependencies, convert to pyproject.toml.
- Fix cycle selection and direct pandas selection for normalization processes.

## Version 0.9.2

- Fix a communications bug where a packet that can cause commands to hang in certain rare situations.

## Version 0.9.1

- Minor bug fixes and dependency updates (to fix pandas errors).
- Fixes to support Pandas 2.0.
- Ensure that some invalid characters are not used in machine names.
- Check for files with and without spaces on machine when loading a new experiment (in case run was started outside of qslib).
- Parse IOError messages from the machine.

## Version 0.9.0

- Fix `Stage.stepped_ramp` when all temperature increments are the same, but temperatures are not.
- Add a `start_increment` option to `Stage.stepped_ramp` for cases where the user does not want the
  ramp to actually start at the starting temperature, but at the first increment away from it (eg,
  when continuing a previous ramp).
- Ensure that units are delta units when appropriate in protocols, regardless of whether the user
  entered them as delta units (eg, so "2°C" will work as a temperature step).

## Version 0.8.2

- Add check for existing, completed EDS files with same name, and option to `Experiment.run` to overwrite completed or working files.
- Fix XML protocol parsing bug for increment units with non-qslib EDS files.
- Fix stage lines in plots.
- Improve error messages.

## Version 0.8.1

- Fix bug with BytesIO not being recognized by `Experiment.save_file`.

## Version 0.8.0

- Ensure that protocols don't have incorrect stage index and label information when being sent to machine.
- MachineStatus now includes temperature information, as does the `qslib machine-status` CLI command.
- `Experiment.sync_from_machine` can exclude tiffs, and has better logging.
- Custom steps are now approximated in xml files, keeping them somewhat compatible with the machine's android interface.
- Several testing and documentation improvements.
- Python 3.11 fixes.

## Version 0.7.1

 - QSLib-initiated experiments should now be partially compatible with the machine's android interface.  Status, time, and the (possibly approximate) protocol should be displayed.  Data and samples will likely not.  Pause/resume/stop/open/close buttons on the interface should function properly.
 - `Processor` and plotting improvements.

## Version 0.7.0

 - `Protocol` now has `Protocol.stage`, and Stage now has `Stage.step`, to provide convenient, 1-indexed access,
   such that `protocol.stage[5]` of is stage 5 of `protocol`, not stage 6.
 - `Protocol` now supports setting PRERUN and POSTRUN stages, as a series of SCPI commands.  This allows
   the setting of things like idling temperatures and exposure times.  It is not easily usable yet, however.
 - `Experiment.change_protocol_from_now` allows convenient changes to a currently-running experiment.
 - `Normalization` has been renamed to `Processor`.  Plotting functions can take sequences of processors to
   process data.  These now include:
     - Normalization as before: `NormByMeanPerWell`, `NormByMaxPerWell`
     - `SubtractMeanPerWell`: subtracts the mean of a particular region, applied to each well.
     - `SmoothEMWMean`: smooths data using Pandas' ExponentialMovingWindow.mean.
     - `SmoothWindowMean`: smooths data using Pandas' Rolling.mean or Window.mean.
 - Some initial implementation changes to allow repeated steps (not stages).
 - Fixes bug that prevented loading of some aborted runs.
 - Fixes monitor's recording of temperatures.
 - Experiment.all_filters uses data if it exists; Experiment.filter_strings as a convenience function alternative.
 - SCPICommand parsing improvements.
 - Protocol printing improvements.

## Version 0.6.3
 - Fixes drawer check bug.

## Version 0.6.2
 - Add checks for cover and drawer position after changing positions.

## Version 0.6.1
 - Fix pyparsing 3.0.7 whitespace parsing problem (pyparsing/pyparsing#359).
 - Use Hypothesis for some tests.

## Version 0.6.0
 - Improved plots, including new `Experiment.plot_temperatures`.
 - Comments in SCPI commands, allowing save/load of default filters in protocols
 - Licensing switched to AGPL 3.0, CLA in docs.
 - Command comment parsing, also used to store whether steps are using default filters.
 - Example notebook
 - Working parsing of Exposure commands
 - Dependency version fixes

## Version 0.5.1

- `CustomStep.collect` (and subclasses, including `Step`) is now `CustomStep.collects`.  The `collect` parameter for
  `Step` can now be `None`, which will collect data if the step has a filter setting (ie, if you want to collect the
  default `Protocol` filters, you need to use `collect=True`).
- `Stage.stepped_ramp` and `Stage.hold_for` are now safer in requiring some keyword arguments, are documented, and
  are more flexible.

## Version 0.5.0

- Units throughout protocols, thanks to Pint: you can now use strings like '1 hr' or '52 °C' when defining protocols.
- Automatic connections throughout Machine and Experiment, by default.
- Change safe titles from spaces-as-"-" to spaces-as-"_", consistent with machine.
- Deprecated paramiko tunnel.
- Documentation rewrite
- Implement EDS-file (in-progress and complete) synchronization for monitor.
- Reorganization of modules: common is no longer needed

## Version 0.4.1

- Allow setting of Experiment.sample_wells.
- Improve plotting code.

## Version 0.4.0

- Much faster `Experiment.sync_from_machine`, only transferring additional log
  entries rather than the entire log.
- Common plotting routines for fluorescence data.
- More reliable monitoring.
- More reliable connections, and testing.
- `Stage.stepped_ramp` convenience function.
- Fixes to bugs inhibiting exposure setting, and some basic
  implementations for this.
- Fixes to qs-monitor for cycle counts > 999 (and other large stage / step /
  cycle counts).

## Version 0.3.1

- CLI improvements.
- Fix qs-monitor and various functions when run titles have spaces in them.
- Improve tests and typing.

## Version 0.3.0

- Add qslib CLI, with some basic utility functions.
- Include calculation using 1.6°c/s ramp (standard mode default) in
  temperature protocols, initiating from 25°c. This makes experiment
  temperature data match reasonably well with protocol calculations.
  It needs to be expanded to handle different ramp rates and run
  modes, however.
- Added some tests.

## Version 0.2.0

- Add internal SSH tunnel support, so that this doesn't need to be set up
  separately, and can be configured with parameters to Machine.
- Fix mid-run syncing bugs related to directory creation.
- Handle some different inputs for Protocol creation.
- Try to handle spaces in names better; this is still a problem, howver.

## Version 0.1.1

Various small improvements and bug fixes.

## Version 0.1.0

Initial implementation of full communication (OK/NEXT/ERRor), Experiment
files, Machine connection interface. Adaptation of QSConnectionAsync to
use new communication. Move of monitor system into qslib.

## Version 0.0.0 / etc

Initial version of QSConnectionAsync and other low-level portions to
support rawquant and qpcr\_data\_updater.
