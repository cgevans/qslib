# Changelog

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

## Verson 0.1.1

Various small improvements and bug fixes.

## Version 0.1.0


Initial implementation of full communication (OK/NEXT/ERRor), Experiment
files, Machine connection interface. Adaptation of QSConnectionAsync to
use new communication. Move of monitor system into qslib.

## Version 0.0.0 / etc


Initial version of QSConnectionAsync and other low-level portions to
support rawquant and qpcr\_data\_updater.
