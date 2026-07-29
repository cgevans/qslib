# CI ownership

QSLib deliberately splits CI by trust boundary:

- GitHub Actions owns ordinary CI, documentation, release builds, and
  publication.
- git.costi.net owns only the qPCR integration tests that need the dedicated
  simulator/machine network.
- Codeberg is a passive source mirror. Repository Actions must remain disabled
  there.

The GitHub repository requires every external action to be pinned to a full
commit SHA. Keep the human-readable release tag in a trailing comment so
Dependabot can update the pin. Release jobs do not restore or save caches, and
their intermediate wheel artifacts expire after one day.

## Trusted publishing

The `release.yml` workflow expects these trusted-publisher identities:

| Registry | Repository | Workflow | Environment |
| --- | --- | --- | --- |
| PyPI | `cgevans/qslib` | `release.yml` | `pypi` |
| crates.io | `cgevans/qslib` | `release.yml` | `crates-io` |

Neither publishing job should use a long-lived registry token. The final
`github-release` job runs only after both registry publications complete.

## qPCR runner

`.forgejo/workflows/real-machine.yml` has an explicit
`forge.server_url == 'https://git.costi.net'` guard and accepts only pushes to
`main` or manual dispatches. The `self-hosted-qpcr` runner has capacity one,
and the workflow also serializes the Rust and Python suites in one job so they
cannot alter the simulator concurrently.
