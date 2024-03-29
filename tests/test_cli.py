# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import sys

import pytest
from click.testing import CliRunner

from qslib import Experiment
from qslib.cli import cli


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


# def test_real_setup(runner: CliRunner):
#     result = runner.invoke(
#         cli,
#         [
#             "setup-machine",
#             "-c",
#             "pass_controller",
#             "-a",
#             "pass_admin",
#             "-d",
#             "localhost",
#             "local_controller_password",
#         ],
#     )

#     assert result.return_value == 0

#     m = Machine("localhost")
