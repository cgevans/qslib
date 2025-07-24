# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import pytest
import tempfile
import warnings
from pathlib import Path

from qslib import Experiment, Protocol, Stage, Step


def test_create():
    Experiment(protocol=Protocol([Stage([Step(30, 25)])]))


def test_fail_plots():
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))

    with pytest.raises(ValueError, match="no temperature data"):
        exp.plot_temperatures()

    with pytest.raises(ValueError, match="no data available"):
        exp.plot_over_time()

    with pytest.raises(ValueError, match="no data available"):
        exp.plot_anneal_melt()


@pytest.mark.parametrize("ch", ["/", "!", "}"])
def test_unsafe_names(ch):
    with pytest.raises(ValueError, match=r"Invalid characters \(" + ch + r"\)"):
        Experiment(name=f"a{ch}b")


def test_file_path_warning():
    """Test that using a file path as name triggers a warning suggesting from_file."""
    with tempfile.NamedTemporaryFile(suffix='.eds', delete=False) as tmp:
        tmp.write(b'test content')
        tmp_path = tmp.name
    
    try:
        # Test with .eds extension
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Experiment(name=tmp_path)
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "appears to be a file path" in str(w[0].message)
            assert "Experiment.from_file" in str(w[0].message)
        
        # Test with path separators
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Experiment(name=tmp_path)
            assert len(w) == 1
            assert "appears to be a file path" in str(w[0].message)
    finally:
        Path(tmp_path).unlink()


def test_no_warning_for_normal_names():
    """Test that normal experiment names don't trigger warnings."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        Experiment(name="normal_experiment_name")
        Experiment(name="experiment.name")  # dot but not .eds
        assert len(w) == 0


def test_no_warning_for_nonexistent_paths():
    """Test that non-existent paths don't trigger warnings."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        Experiment(name="/nonexistent/path/file.eds")
        Experiment(name="nonexistent_file.eds")
        assert len(w) == 0
