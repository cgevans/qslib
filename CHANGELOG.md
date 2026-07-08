<!--
SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>

SPDX-License-Identifier: EUPL-1.2
-->

# Changelog

## Unreleased

### Protocol fixes
- Fix collection-filter serialization in `tcprotocol.xml`. Filters are carried internally in hacform (`m{em},x{ex}[,quant]`), but `to_xml_pair` re-parsed them with an ad-hoc splitter that assumed an `em,row,ex` numeric layout, so `m4,x4,quant` was written as `Excitation="xquant" Emission="mm4"` (and reloading such a file warned `Invalid filter set format: xquant-mm4`). Serialization now goes through `FilterSet::from_string`, restoring the pre-0.14 output `Excitation="x4" Emission="m4"`. Only the `tcprotocol.xml` metadata was affected; `filterdata` and quant data were correct. Regression introduced in 0.14.0 with the Rust protocol port.

## Version 0.15.1

### Data fixes
- Fix `welldata` / `filter_data` time columns on v1-spec EDS files. In 0.15.0 the v1 codepath was rewritten to go through `filter_data_polars` (which emits `Datetime[ms, UTC]`) and reformat back to the legacy pandas multi-index; the timestamp conversion assumed `astype("int64")` returned nanoseconds, so it divided by 1e9 and produced timestamps 1e6× too small (and huge negative `seconds`/`hours`). Now uses a time-unit-agnostic subtraction from the Unix epoch.
- Add `temperatures_polars_wide` for callers that need the 0.14-era wide schema (`timestamp` as `Datetime[ms, UTC]`, one column per `(kind, zone)` — `sample_1..N`, `heatsink`, `cover`, `block_1..N`). `temperatures_polars` itself remains long-format (the intentional 0.15 default), now also with a `time` column derived from `timestamp`.

## Version 0.15.0

Robustness improvements to communication, consolidation of parsing into Rust, and a new quant/calibration data pipeline:

### Communication fixes
- Fix READY message read to handle partial TCP reads (TCP does not guarantee message boundaries).
- Fix double newline on command send when content already has a trailing newline.
- Fix quoted string parser to handle backslash escape sequences (`\"`, `\\`, `\n`, `\t`).
- Fix ErrorResponse parser to handle errors without brackets.
- Fix Rust parser tag character set to include underscore (e.g. `<quote_reply>`).
- Fix DRAW? returning "false" instead of "Open" by not coercing open/closed to Bool in response parser.
- Add single-quote string support to Rust parser.
- Extend boolean parsing to cover all server-recognized forms (yes/no, on/off, open/close).
- Extend command name character set to include `+`, `-`, `~`, `<` (e.g. `SUBS+`, `FLAG-`).
- Add WARNing response type to Rust parser.
- Add tag stack timeout to message receiver, preventing unclosed tags from absorbing all subsequent messages.
- Send QUIT command on disconnect for clean server-side cleanup.
- Handle auth-required gracefully in `Machine.connect()` with a clear error message for remote connections.
- Handle `ExclusiveAccess` and `AccessGiven` errors as specific exception subclasses.

### Data and protocol fixes
- Fix IncrementCycle/IncrementStep defaults to match server (default=2, not 1).
- Fix SampleTemperatures separator mismatch in multicomponent data parsing.
- Remove overly strict temperature zone and plate type assertions.
- Parameterize plate type in `Experiment._new_xml_files` instead of hardcoding 96-well.

### New features
- Consolidate Python parsing to Rust: `AccessLevel`, `FilterSet`, `SCPICommand`, `PlateSetup` all now handled in Rust; `pyparsing` dependency removed.
- Add Rust quant/calibration file parsers and filterdata reconstruction from quant + calibrations (matches filterdata.xml to <0.1 error).
- Add ROI calibration parsing and TIFF-to-quant processing pipeline (exact match to instrument output).
- Migrate `FilterDataCollection` loading and timestamp-setting to Rust.
- Dynamic zone count querying via `Machine.get_zone_count()` instead of hardcoded 6.
- Expose server random key auth mechanism via `Machine.generate_random_key()`.
- Validate READY message capabilities on connection.
- Make `subscribe_log()` actually send SUBS+ command to the server.
- Parse MESSage timestamp structurally in `LogMessage`.
- Extend `Subscribe` command builder with timestamp and multi-topic support.
- Add exclusive and stealth flags to `AccessLevelSet` command.

### Testing
- ~226 new tests across Rust and Python, covering communication, parsing, plate setup, processors, and machine logic.

### Security
- Update `rustls-webpki` 0.102 → 0.103 to fix four advisories: CRL distribution-point matching (RUSTSEC-2026-0049), URI name-constraint bypass (RUSTSEC-2026-0098), wildcard name-constraint bypass (RUSTSEC-2026-0099), and reachable panic in CRL parsing (RUSTSEC-2026-0104).
- Bump `bytes` to 1.11.1 (integer overflow in `BytesMut::reserve`, RUSTSEC-2026-0007), `quinn-proto` to 0.11.14 (QUIC endpoint DoS, RUSTSEC-2026-0037), and `time` to 0.3.47 (stack-exhaustion DoS, RUSTSEC-2026-0009).
- Port `qs-monitor` from `matrix-sdk` 0.9 to 0.16, fixing three advisories: encrypted-event sender spoofing (RUSTSEC-2025-0041), panic in `RoomMember::normalized_power_level()` (RUSTSEC-2025-0065), and DoS via custom `m.room.join_rules` events (RUSTSEC-2025-0135).

### Build
- Vendor `matrix-sdk` 0.16 with `#![recursion_limit = "512"]` added to work around [rust-lang/rust#152942](https://github.com/rust-lang/rust/issues/152942) (query-depth overflow on rustc 1.94+ due to matrix-sdk's deeply nested `#[tracing::instrument]` async chain picking up an extra `ManuallyDrop` layer).
- `qs-monitor/src/main.rs` gets the same attribute to raise the trait-solver limit for `Send`-ness of `Client::sync()` when passed to `tokio::spawn`.

### CI
- Bump GitHub Actions past the Node.js 20 deprecation: `actions/checkout` v4→v6, `setup-python` v5→v6, `upload-artifact` v4→v7, `download-artifact` v4→v8, `extractions/setup-just` v2→v4, `astral-sh/setup-uv` v7→v8.1.0, `codecov/codecov-action` v5→v6.
- Fix wheel-test jobs on Linux: `RUSTC_WRAPPER=sccache` was leaking from `maturin-action` into subsequent `uv sync` invocations where sccache isn't on PATH, failing silently on macOS (sccache pre-installed) but breaking Linux. Now overridden per-step.
- Drop macOS x86_64 (Intel) from the wheel matrix (<8 Darwin downloads/month across all arches on PyPI).

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
