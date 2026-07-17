# Installing and upgrading qs-monitor

The release bundle targets x86-64 Linux with systemd. Install a configuration
at `/etc/qs-monitor/config.toml` before the first deployment. Keep Matrix's
`session_file` (and its SQLite store) under `/var/lib/qs-monitor/` so the
hardened unit can write it. The durable delivery database defaults to
`/var/lib/qs-monitor/state.sqlite`. Deployment sets the configuration to
`root:qs-monitor` mode `0640`.

Install `qs-monitor-deploy` from a release bundle in
`/usr/local/sbin/qs-monitor-deploy`, then activate a release explicitly:

```console
sudo qs-monitor-deploy v0.15.2
```

The command downloads the bundle and its SHA-256 manifest over HTTPS, checks
the binary version and configuration, stages an immutable release under
`/opt/qs-monitor/releases/`, stops the current process gracefully, changes the
`/opt/qs-monitor/current` symlink atomically, and waits for systemd readiness.
If readiness fails within 30 seconds it restores the previous symlink and
service. It never changes or replaces `state.sqlite`.

Operational commands are:

```console
sudo qs-monitor-deploy status
sudo qs-monitor-deploy rollback
sudo qs-monitor-deploy rollback 0.15.1
```

The active release and the three preceding successful releases are retained.
Fully delivered queue rows are pruned after 30 days; dead letters and every
undelivered row remain until explicitly inspected or cleared with SQLite
administration while the service is stopped.

Deploy qslib-server with epoch-capable SSE IDs before upgrading qs-monitor.
The monitor accepts an older server's numeric IDs, but logs that restart replay
guarantees are degraded until the server is upgraded.
