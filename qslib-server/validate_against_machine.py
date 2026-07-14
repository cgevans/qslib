#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2024 - 2026 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2
"""Validate qslib-server bulk transfer against the instrument's real filesystem.

This checks the HTTP paths whose correctness depends on the instrument's actual
on-disk layout, comparing against ground truth read straight off the device with
``find``/``md5sum`` over SCPI ``SYST:EXEC``:

  A. Directory download  -- Machine.download_dir (HTTP /list + raw /file) must
                            reproduce every file under a run directory,
                            md5-identical to the on-disk files (dotfiles
                            included, matching the ZIPREAD walk).
  B. Completed .eds       -- read_file(name, context="public_run_complete") over
                            HTTP must equal the SCPI read and the on-disk md5;
                            this exercises the /sdcard absolute-path resolution.
  C. End-to-end           -- Experiment.from_uncollected loads a run over HTTP.

qslib-server may already be running, or be deployed via QSLIB_SERVER_BINARY.
Confirmed working against qpcr2 (appletini, armv7, InstrumentServer).

Environment
-----------
  QSLIB_TEST_MACHINE    host (default: localhost)
  QSLIB_TEST_PORT       SCPI SSL port (default: 7443)
  QSLIB_TEST_PASSWORD   Controller password (default: none / passwordless)
  QSLIB_SERVER_PORT     qslib-server port (default: 7500)
  QSLIB_SERVER_TOKEN    bearer token (optional)
  QSLIB_SERVER_BINARY   cross-compiled qslib-server to deploy (optional)
  QSLIB_SERVER_LISTEN   instrument bind addr for deploy, e.g. 169.254.217.190:7500
  QSLIB_RUN_NAMES       comma-separated run dirs under experiments/ to A/B
                        (default: auto-pick a few small ones)
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
import tempfile
from pathlib import Path

from qslib.machine import Machine
from qslib.scpi_commands import AccessLevel


def _unwrap(s: str) -> str:
    m = re.search(r"<quote\.reply>\n?(.*?)</quote\.reply>", s, re.S)
    return (m.group(1) if m else s).rstrip("\n")


def main() -> int:
    host = os.environ.get("QSLIB_TEST_MACHINE", "localhost")
    port = int(os.environ.get("QSLIB_TEST_PORT", "7443"))
    password = os.environ.get("QSLIB_TEST_PASSWORD") or None
    server_port = int(os.environ.get("QSLIB_SERVER_PORT", "7500"))
    server_token = os.environ.get("QSLIB_SERVER_TOKEN") or None
    exp_root = "/data/vendor/IS/experiments"

    results: list[tuple[str, bool, str]] = []
    # Control over direct SSL so it is independent of qslib-server on 7500.
    m = Machine(host, port=port, ssl=True, password=password, server_port=None, tls_server_name="localhost")
    with m.ensured_connection():
        m.set_access_level(AccessLevel.Controller)

        def ex(c: str) -> str:
            return _unwrap(m.run_command(f'SYST:EXEC -verbose "{c}"'))

        binary = os.environ.get("QSLIB_SERVER_BINARY")
        if binary:
            m.server_port = server_port
            m._server = None
            m.server_token = server_token
            m.ensure_server(binary=binary, listen=os.environ["QSLIB_SERVER_LISTEN"], file_root="/")
        else:
            m.server_port = server_port
            m._server = None
            m.server_token = server_token
            m._prefer_server_files = True

        health = m.server.health()
        print(f"[health] {health}")
        assert health.get("file_root"), "server /health has no file_root (old build?)"

        def fs_md5(absdir: str) -> dict[str, str]:
            out = ex(f"cd {absdir} && find . -type f -exec md5sum {{}} \\;")
            d = {}
            for line in out.split("\n"):
                mm = re.match(r"([0-9a-f]{32})\s+\./(.*)$", line.strip())
                if mm:
                    d[mm.group(2)] = mm.group(1)
            return d

        runs = os.environ.get("QSLIB_RUN_NAMES")
        if runs:
            run_list = [r.strip() for r in runs.split(",") if r.strip()]
        else:
            names = [x.strip() for x in ex(f"ls {exp_root}").split("\n") if x.strip()]
            # prefer a few non-empty runs
            run_list = [n for n in names if fs_md5(f"{exp_root}/{n}")][:3]
        print(f"[runs] {run_list}")

        for run in run_list:
            truth = fs_md5(f"{exp_root}/{run}")
            with tempfile.TemporaryDirectory() as td:
                used = m.download_dir(run, td, leaf="EXP")
                got = {
                    f.relative_to(td).as_posix(): hashlib.md5(f.read_bytes()).hexdigest()
                    for f in Path(td).rglob("*")
                    if f.is_file()
                }
            ok = bool(used) and got == truth
            results.append(
                (f"dir {run}", ok, f"http={used} n_disk={len(truth)} n_http={len(got)} match={got == truth}")
            )
            if got != truth:
                print("  only-disk:", sorted(set(truth) - set(got))[:8])
                print("  only-http:", sorted(set(got) - set(truth))[:8])
                print("  md5-diff:", [k for k in truth if k in got and truth[k] != got[k]][:8])

        # /sdcard public_run_complete absolute-path serving (synthetic file).
        prc = "/sdcard/public_run_complete/qslib_validate.bin"
        ex(f"mkdir -p /sdcard/public_run_complete && head -c 300000 /dev/urandom > {prc} && echo ok")
        disk = re.match(r"([0-9a-f]{32})", ex(f"md5sum {prc}")).group(1)
        http = m.read_file("qslib_validate.bin", context="public_run_complete", fast=True)
        scpi = m.read_file("qslib_validate.bin", context="public_run_complete", fast=False)
        ex(f"rm -f {prc}")
        results.append(
            (
                "public_run_complete /sdcard A/B",
                http == scpi and hashlib.md5(http).hexdigest() == disk,
                f"http={len(http)} scpi={len(scpi)} eq={http == scpi}",
            )
        )

        if run_list:
            from qslib.experiment import Experiment

            try:
                e = Experiment.from_uncollected(m, run_list[0])
                n = sum(1 for p in Path(e._dir_base).rglob("*") if p.is_file())
                results.append((f"from_uncollected {run_list[0]}", n > 0, f"loaded {n} files, name={e.name!r}"))
            except Exception as exc:
                results.append((f"from_uncollected {run_list[0]}", False, f"{type(exc).__name__}: {exc}"))

    print("\n=== RESULTS ===")
    for name, ok, detail in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    allok = bool(results) and all(r[1] for r in results)
    print("ALL PASS" if allok else "SOME FAIL")
    return 0 if allok else 1


if __name__ == "__main__":
    sys.exit(main())
