from pathlib import Path

import pytest

from qslib import Experiment
from qslib.normalization import NormToMaxPerWell, NormToMeanPerWell


@pytest.fixture(scope="module")
def exp() -> Experiment:
    return Experiment.from_file("tests/test.eds")


@pytest.fixture(scope="module")
def exp_reloaded(
    exp: Experiment, tmp_path_factory: pytest.TempPathFactory
) -> Experiment:
    tmp_path = tmp_path_factory.mktemp("exp")
    exp.save_file(tmp_path / "test_loaded.eds")
    return Experiment.from_file(tmp_path / "test_loaded.eds")


def test_props(exp: Experiment, exp_reloaded: Experiment):
    assert exp.name == "2020-02-20_170706"

    assert exp.info() == str(exp) == exp.summary()


def test_reload(exp: Experiment, exp_reloaded: Experiment):
    assert (exp.welldata == exp_reloaded.welldata).all().all()
    assert exp.name == exp_reloaded.name
    assert exp.protocol == exp_reloaded.protocol
    assert exp.plate_setup == exp_reloaded.plate_setup


def test_plots(exp: Experiment):
    exp.plot_over_time(
        samples="Sample 1", temperatures="axes", normalization=NormToMeanPerWell()
    )
    exp.plot_anneal_melt(samples="Sample 1")
    exp.protocol.plot_protocol()


def test_rawquant(exp: Experiment):
    exp.rawdata.loc[:, :]
