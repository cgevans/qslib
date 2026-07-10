# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

"""Tests for stage names and positions."""

from pathlib import Path

import pytest

from qslib import Experiment
from qslib._qslib import RunStatus

_TESTS_DIR = Path(__file__).parent


def _status(stage_token: str, num_stages: int = 5, state: str = "Running") -> RunStatus:
    return RunStatus.from_bytes(f"MyRun {stage_token} {num_stages} 1 10 1 0 {state}".encode())


def test_run_status_prerun():
    s = _status("PRERUN")
    assert s.stage == 0
    assert s.stage_name == "PRERUN"


def test_run_status_numbered():
    s = _status("2")
    assert s.stage == 2
    assert s.stage_name == "2"


def test_run_status_postrun_is_after_last_stage():
    s = _status("POSTRun", num_stages=5, state="Complete")
    assert s.stage == s.num_stages + 1 == 6
    assert s.stage_name == "POSTRun"


def test_run_status_unset():
    s = _status("-1", state="Idle")
    assert s.stage == -1
    assert s.stage_name == "-1"


def test_experiment_stages_has_name_and_number():
    exp = Experiment.from_file(_TESTS_DIR / "test.eds")
    st = exp.stages
    assert "stage_name" in st.columns
    rows = {r["stage_index"]: r["stage_name"] for r in st.iter_rows(named=True)}
    assert rows[0] == "PRERUN"
    assert rows[1] == "1"
    assert rows[max(rows)].upper() == "POSTRUN"
    fd_stages = set(exp.filter_data_polars["stage"].unique().to_list())
    numbered = {i for i, name in rows.items() if name not in ("PRERUN",) and name.upper() != "POSTRUN"}
    assert fd_stages <= numbered


@pytest.mark.parametrize(
    "kwargs",
    [
        {"stages": slice(None)},
        {"stages": 2},
        {"start_time": "stage"},
        {"start_time": "stage", "stages": 2},
    ],
)
def test_plot_over_time_altair_stage_selection(kwargs):
    pytest.importorskip("altair")
    exp = Experiment.from_file(_TESTS_DIR / "test.eds")
    chart = exp.plot_over_time_altair(**kwargs)
    assert chart is not None
