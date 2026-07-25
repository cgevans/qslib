# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import ast
from pathlib import Path

import pytest

from qslib import Experiment, Protocol, Stage, Step
from qslib.experiment import DataNotAvailableError


def test_create():
    Experiment(protocol=Protocol([Stage([Step(30, 25)])]))


def test_fail_plots_temperature():
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))

    with pytest.raises(DataNotAvailableError):
        exp.plot_temperatures()


def test_fail_plots_over_time():
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))
    with pytest.raises(DataNotAvailableError):
        exp.plot_over_time()


def test_fail_plots_anneal_melt():
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))
    with pytest.raises(DataNotAvailableError):  # FIXME: why is this inconsistent?
        exp.plot_anneal_melt()


@pytest.mark.parametrize("ch", ["/", "!", "}"])
def test_unsafe_names(ch):
    with pytest.raises(ValueError, match=r"Invalid characters \(" + ch + r"\)"):
        Experiment(name=f"a{ch}b")


def test_all_filters_no_data() -> None:
    """Test that all_filters returns protocol filters when experiment has no data."""
    from qslib import Experiment, Protocol, Stage, Step
    from qslib.data import FilterSet

    # Create a new experiment with no data - should use protocol filters
    exp_no_data = Experiment(name="test_no_data")

    # Default protocol has no filters, so all_filters should be empty
    assert len(exp_no_data.all_filters) == 0
    assert list(exp_no_data.all_filters) == []

    # Create an experiment with protocol that has filters
    step_with_filters = Step(time=60, temperature=95, filters=["x1-m1", "x2-m2"], collect=True)
    protocol_with_filters = Protocol([Stage([step_with_filters])], filters=["x3-m3"])
    exp_with_protocol_filters = Experiment(name="test_with_filters", protocol=protocol_with_filters)

    # Should return filters from protocol (both default filters and step filters)
    expected_protocol_filters = {
        FilterSet.fromstring("x1-m1"),
        FilterSet.fromstring("x2-m2"),
        FilterSet.fromstring("x3-m3"),
    }
    actual_protocol_filters = set(exp_with_protocol_filters.all_filters)

    assert actual_protocol_filters == expected_protocol_filters
    assert len(exp_with_protocol_filters.all_filters) == 3


def test_available_data_with_data():
    """Test available_data method with experiment loaded from test.eds file."""
    exp = Experiment.from_file(Path(__file__).parent / "test.eds")
    available = exp.available_data()

    # test.eds should have all these data types available
    expected_data = [
        "filter_data",
        "multicomponent_data",
        "amplification_data",
        "analysis_result",
        "temperatures",
        "quant_data",
        "calibrations",
    ]

    assert set(available) == set(expected_data)
    assert len(available) == 7


def test_available_data_no_data():
    """Test available_data method with newly-created experiment (no data)."""
    exp = Experiment(name="test_no_data", protocol=Protocol([Stage([Step(30, 25)])]))
    available = exp.available_data()

    # New experiment should have no data available
    assert available == []
    assert len(available) == 0


def test_multicomponent_sample_temperatures_whitespace():
    """Test that SampleTemperatures parsing handles both tab and space separators."""
    import numpy as np
    from qslib.data import _parse_multicomponent_data_v1
    import xml.etree.ElementTree as ET
    import zipfile

    # Parse the real multicomponent data from test.eds (tab-separated)
    with zipfile.ZipFile(Path(__file__).parent / "test.eds") as z:
        with z.open("apldbio/sds/multicomponentdata.xml") as f:
            tree = ET.parse(f)

    result_original = _parse_multicomponent_data_v1(tree)
    assert "temperature" in result_original.columns
    assert len(result_original) > 0
    # Temperatures should all be positive and reasonable
    temps = result_original["temperature"].dropna()
    assert len(temps) > 0
    assert (temps > 0).all()
    assert (temps < 150).all()

    # Now modify the XML to use space-separated values (simulating server aggregation)
    with zipfile.ZipFile(Path(__file__).parent / "test.eds") as z:
        with z.open("apldbio/sds/multicomponentdata.xml") as f:
            tree2 = ET.parse(f)

    st_elem = tree2.find("SampleTemperatures")
    original_text = st_elem.text
    # Replace tabs with spaces (what server aggregation does)
    st_elem.text = " ".join(original_text.split())

    result_space = _parse_multicomponent_data_v1(tree2)
    assert np.array_equal(
        result_original["temperature"].values,
        result_space["temperature"].values,
    )


def test_create_96_well_plate():
    """Test that default experiment creates a 96-well plate."""
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))
    assert exp.plate_type == 96


def test_create_384_well_plate():
    """Test that 384-well plate can be created via plate_setup."""
    from qslib.plate_setup import PlateSetup

    ps = PlateSetup(plate_type=384)
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]), plate_setup=ps)
    assert exp.plate_type == 384


# --- machine arguments given as host names ---

_HOST = "example.invalid"


class _MachineUsed(ValueError):
    """Raised at the first use of a stand-in machine, recording what was used.

    A ValueError, so that fallback paths taken when a machine is unusable (such
    as latest_from_machine falling back to storage) are followed rather than
    cut short.

    (LLM-generated)
    """

    def __init__(self, host, attr):
        self.host = host
        self.attr = attr
        super().__init__(f"{host}.{attr}")


class _SentinelMachine:
    """A Machine substitute that refuses to do anything but says what was asked.

    (LLM-generated)
    """

    def __init__(self, host, *args, **kwargs):
        from qslib import AccessLevel

        self.host = host
        self.max_access_level = AccessLevel.Controller

    def __getattr__(self, name):
        raise _MachineUsed(self.host, name)


def _exp():
    return Experiment(protocol=Protocol([Stage([Step(30, 25)])]))


def _a_protocol():
    return Protocol([Stage([Step(30, 25)])])


# Every public Experiment method taking a machine, called with a host name.
# test_machine_methods_all_covered checks that this list stays complete.
MACHINE_CALLS = {
    "abort": lambda m: _exp().abort(m),
    "change_protocol": lambda m: _exp().change_protocol(_a_protocol(), m),
    "change_protocol_from_now": lambda m: _exp().change_protocol_from_now([Stage([Step(30, 25)])], m),
    "from_machine": lambda m: Experiment.from_machine(m, "run"),
    "from_machine_storage": lambda m: Experiment.from_machine_storage(m, "run"),
    "from_running": lambda m: Experiment.from_running(m),
    "from_uncollected": lambda m: Experiment.from_uncollected(m, "run"),
    "get_status": lambda m: _exp().get_status(m),
    "latest_from_machine": lambda m: Experiment.latest_from_machine(m),
    "pause_now": lambda m: _exp().pause_now(m),
    "resume": lambda m: _exp().resume(m),
    "run": lambda m: _exp().run(m),
    "stop": lambda m: _exp().stop(m),
    "sync_from_machine": lambda m: _exp().sync_from_machine(m),
}


def _machine_methods():
    """Public Experiment methods with a machine parameter, by annotation.
    (LLM-generated)
    """
    found = set()
    for name, attr in vars(Experiment).items():
        if name.startswith("_"):
            continue
        func = attr.__func__ if isinstance(attr, classmethod) else attr
        annotation = getattr(func, "__annotations__", {}).get("machine")
        if annotation is not None and "MachineReference" in str(annotation):
            found.add(name)
    return found


def test_machine_methods_all_covered():
    assert _machine_methods() == set(MACHINE_CALLS)


@pytest.mark.parametrize("method", sorted(MACHINE_CALLS))
def test_machine_methods_accept_hostname(method, monkeypatch):
    """Giving a host name does the same thing as giving the Machine itself.
    (LLM-generated)
    """
    import qslib.experiment

    monkeypatch.setattr(qslib.experiment, "Machine", _SentinelMachine)
    call = MACHINE_CALLS[method]

    with pytest.raises(_MachineUsed) as from_host:
        call(_HOST)

    with pytest.raises(_MachineUsed) as from_machine:
        call(_SentinelMachine(_HOST))

    assert from_host.value.host == from_machine.value.host == _HOST
    assert from_host.value.attr == from_machine.value.attr


def test_latest_from_machine_takes_the_running_experiment(monkeypatch):
    """With a run in progress, that experiment is used, and storage is not consulted.
    (LLM-generated)
    """
    from qslib.machine import Machine

    running = _exp()
    seen = []

    def fake_from_running(machine):
        seen.append(machine)
        return running

    def no_list_runs_in_storage(self, glob="*", *, verbose=False):
        raise AssertionError("storage was consulted despite a run being in progress")

    monkeypatch.setattr(Experiment, "from_running", fake_from_running)
    monkeypatch.setattr(Machine, "list_runs_in_storage", no_list_runs_in_storage)

    assert Experiment.latest_from_machine(_HOST) is running

    assert isinstance(seen[0], Machine)
    assert seen[0].host == _HOST


def test_latest_from_machine_takes_newest_run_in_storage(monkeypatch):
    """With nothing running, the most recently modified stored run is used.
    (LLM-generated)
    """
    from qslib.machine import Machine

    seen = []

    def fake_from_running(machine):
        raise ValueError("Nothing is currently running.")

    def fake_list_runs_in_storage(self, glob="*", *, verbose=False):
        return [
            {"path": "older", "mtime": 100.0},
            {"path": "newest", "mtime": 300.0},
            {"path": "middle", "mtime": 200.0},
        ]

    def fake_from_machine_storage(machine, name):
        seen.append((machine, name))
        return _exp()

    monkeypatch.setattr(Experiment, "from_running", fake_from_running)
    monkeypatch.setattr(Machine, "list_runs_in_storage", fake_list_runs_in_storage)
    monkeypatch.setattr(Experiment, "from_machine_storage", fake_from_machine_storage)

    Experiment.latest_from_machine(_HOST)

    machine, name = seen[0]
    assert isinstance(machine, Machine)
    assert machine.host == _HOST
    assert name == "newest"


# --- access levels of commands that control the machine ---


class _Connecting(Exception):
    """Raised in place of connecting, recording what the machine would then allow."""

    def __init__(self, max_access_level):
        self.max_access_level = max_access_level
        super().__init__(max_access_level)


# Every Experiment command that asks for Controller access.
# test_controller_commands_all_covered checks that this list stays complete.
CONTROLLER_COMMANDS = {
    "abort": lambda exp: exp.abort(),
    "change_protocol": lambda exp: exp.change_protocol(_a_protocol()),
    "change_protocol_from_now": lambda exp: exp.change_protocol_from_now([Stage([Step(30, 25)])]),
    "pause_now": lambda exp: exp.pause_now(),
    "resume": lambda exp: exp.resume(),
    "run": lambda exp: exp.run(),
    "stop": lambda exp: exp.stop(),
}


def _controller_commands():
    """Public Experiment methods whose body asks for Controller access."""
    import inspect

    import qslib.experiment

    tree = ast.parse(inspect.getsource(qslib.experiment))
    found = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and node.name == "Experiment"):
            continue
        for method in node.body:
            if isinstance(method, ast.FunctionDef) and not method.name.startswith("_"):
                if "AccessLevel.Controller" in ast.unparse(method):
                    found.add(method.name)
    return found


def test_controller_commands_all_covered():
    assert _controller_commands() == set(CONTROLLER_COMMANDS)


@pytest.mark.parametrize("command", sorted(CONTROLLER_COMMANDS))
def test_controller_commands_raise_max_access_level(command, monkeypatch):
    """A command needing Controller raises the cap of a machine kept from a read-only one."""
    from qslib import AccessLevel
    from qslib.machine import Machine

    def fake_connect(self):
        raise _Connecting(self.max_access_level)

    monkeypatch.setattr(Machine, "connect", fake_connect)

    exp = Experiment(protocol=_a_protocol())
    # A read-only command, such as get_status, leaves behind a machine capped at Observer.
    exp._ensure_machine("example.invalid")
    assert exp.machine.max_access_level == AccessLevel.Observer

    with pytest.raises(_Connecting) as connecting:
        CONTROLLER_COMMANDS[command](exp)

    assert connecting.value.max_access_level >= AccessLevel.Controller
