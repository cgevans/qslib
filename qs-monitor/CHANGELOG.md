# Changelog

## Unreleased

### qs-monitor

- Render `!protocol` output using qslib's `Protocol` `Display` format (matching the Python interface) in a `<pre>` block, instead of the previous minimal per-step summary
- Highlight the current stage/step in `!protocol` output (bold in HTML, `⟵` marker in plain text) and note the current cycle, using live run status
- Add MachineState tracking for zone targets, run progress (stage/cycle/step), run name, and plate setup
- Add `refresh_state()` to query QuickStatus, run name, and plate setup on startup and after every Run message
- Include zone target temperatures in InfluxDB temperature data points
- Update state on Stage/Cycle/Step/Ramping events for accurate run tracking
- Emit `run_state` and plate setup data points to InfluxDB
- Use server-side timestamps from log messages instead of local `Utc::now()` when available
- Subscribe to log topics with `-timestamp` for server-provided timestamps
- Query zone count dynamically via `TBC:ControlZones?` instead of hardcoding 6
- Consolidate subscriptions: replace separate `Subscribe` calls + `subscribe_log` (double-subscribing) with single `subscribe_log_with_options`
- Send QUIT command (`disconnect()`) on connection teardown for clean server-side cleanup
- Fix `AccessLevelSet::level()` -> `AccessLevelSet::new()` (API rename)
- Fix missing `timestamp` field in test `LogMessage` constructions
- Remove broken `..` struct update syntax in test

### qslib

- Add `subscribe_log_with_options(topics, timestamp)` to `QSConnection` for requesting server timestamps
- Fix `ControlZonesQuery` response type: return `usize` with proper `TryFrom<OkResponse>` (was `String`, which could not work with `receive_response()`)
- Fix `RandomKeyQuery` response type: return `RandomKey` newtype with proper `TryFrom<OkResponse>` (same issue)
