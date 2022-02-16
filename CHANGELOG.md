<!--
SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>

SPDX-License-Identifier: AGPL-3.0-only
-->

# Changelog

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
