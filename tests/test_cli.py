# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import os
import sys

import pytest
from click.testing import CliRunner

from qslib import Experiment
from qslib.cli import cli


# Check if a test machine is configured
TEST_MACHINE = os.environ.get("QSLIB_TEST_MACHINE")
requires_machine = pytest.mark.skipif(
    TEST_MACHINE is None,
    reason="No test machine configured (set QSLIB_TEST_MACHINE env var)"
)


@pytest.fixture(scope="module")
def exp():
    return Experiment.from_file("tests/test.eds")


@pytest.fixture(scope="module")
def runner():
    return CliRunner()


def test_info(exp, runner: CliRunner):
    result = runner.invoke(
        cli,
        ["info", "tests/test.eds"],
    )
    assert exp.info().rstrip() == result.output.rstrip()


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="HTML output not identical on Windows."
)
def test_html(exp, tmp_path_factory: pytest.TempPathFactory, runner: CliRunner):
    tp = tmp_path_factory.mktemp("temp_html")
    runner.invoke(
        cli,
        ["info-html", "-o", str(tp / "test.html"), "--no-open", "tests/test.eds"],
    )
    assert exp.info_html()[0:100] == open(tp / "test.html").read()[0:100]


def test_version(runner: CliRunner):
    """Test --version flag."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    # Version string should contain version number
    assert result.output.strip()


def test_help(runner: CliRunner):
    """Test --help flag."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output


def test_protocol_desc(runner: CliRunner):
    """Test protocol-desc command outputs protocol info."""
    result = runner.invoke(cli, ["protocol-desc", "tests/test.eds"])
    assert result.exit_code == 0
    assert "Stage" in result.output


def test_export_data(runner: CliRunner):
    """Test export-data outputs CSV."""
    result = runner.invoke(cli, ["export-data", "tests/test.eds"])
    assert result.exit_code == 0
    lines = result.output.strip().split('\n')
    assert len(lines) > 1  # header + data
    assert "," in lines[0]  # CSV format


def test_export_temperatures(runner: CliRunner):
    """Test export-temperatures outputs CSV."""
    result = runner.invoke(cli, ["export-temperatures", "tests/test.eds"])
    assert result.exit_code == 0
    assert "," in result.output


def test_protocol_plot_pdf(tmp_path, runner: CliRunner):
    """Test protocol-plot creates PDF output file."""
    output = tmp_path / "plot.pdf"
    result = runner.invoke(
        cli,
        ["protocol-plot", "-o", str(output), "--no-open", "tests/test.eds"],
    )
    assert result.exit_code == 0
    assert output.exists()
    assert output.stat().st_size > 0


def test_protocol_plot_png(tmp_path, runner: CliRunner):
    """Test protocol-plot with PNG format."""
    output = tmp_path / "plot.png"
    result = runner.invoke(
        cli,
        ["protocol-plot", "-o", str(output), "-f", "png", "--no-open", "tests/test.eds"],
    )
    assert result.exit_code == 0
    assert output.exists()
    assert output.stat().st_size > 0


def test_protocol_plot_svg(tmp_path, runner: CliRunner):
    """Test protocol-plot with SVG format."""
    output = tmp_path / "plot.svg"
    result = runner.invoke(
        cli,
        ["protocol-plot", "-o", str(output), "-f", "svg", "--no-open", "tests/test.eds"],
    )
    assert result.exit_code == 0
    assert output.exists()
    assert output.stat().st_size > 0


# Error handling tests

def test_info_missing_file(runner: CliRunner):
    """Test info command with non-existent file."""
    result = runner.invoke(cli, ["info", "nonexistent.eds"])
    assert result.exit_code != 0


def test_protocol_desc_missing_file(runner: CliRunner):
    """Test protocol-desc command with non-existent file."""
    result = runner.invoke(cli, ["protocol-desc", "nonexistent.eds"])
    assert result.exit_code != 0


def test_export_data_missing_file(runner: CliRunner):
    """Test export-data command with non-existent file."""
    result = runner.invoke(cli, ["export-data", "nonexistent.eds"])
    assert result.exit_code != 0


# Machine-dependent tests (skipped without QSLIB_TEST_MACHINE env var)

@requires_machine
def test_machine_status(runner: CliRunner):
    """Test machine-status command."""
    result = runner.invoke(cli, ["machine-status", TEST_MACHINE])
    assert result.exit_code == 0
    assert "Machine" in result.output


@requires_machine
def test_list_stored(runner: CliRunner):
    """Test list-stored command."""
    result = runner.invoke(cli, ["list-stored", TEST_MACHINE])
    # Exit code 0 even if no experiments found
    assert result.exit_code == 0


@requires_machine
def test_list_stored_verbose(runner: CliRunner):
    """Test list-stored command with verbose flag."""
    result = runner.invoke(cli, ["list-stored", "-v", TEST_MACHINE])
    assert result.exit_code == 0


@requires_machine
def test_list_stored_with_pattern(runner: CliRunner):
    """Test list-stored command with pattern."""
    result = runner.invoke(cli, ["list-stored", f"{TEST_MACHINE}:*"])
    assert result.exit_code == 0
