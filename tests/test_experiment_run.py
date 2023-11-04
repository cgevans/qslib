# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2


# def test_drawer(monkeypatch):
#     def patchrun(self, value: str) -> str:
#         if value == "CLOSE":
#             return ""
#         else:
#             raise ValueError

#     monkeypatch.setattr(Machine, "run_command", patchrun)

#     m = Machine("none")

#     m.drawer_close()


# @pytest.mark.parametrize(
#     "function",
#     [
#         ("drawer_position", ()),
#         ("machine_status", ()),
#         ("run_status", ()),
#         ("get_running_protocol", ()),
#         ("list_runs_in_storage", ()),
#     ],
# )
# def test_mach_not_connected(function: tuple[str, tuple[Any]]) -> None:
#     m = Machine("none")
#     with pytest.raises(ConnectionError):
#         getattr(m, function[0]).__call__(*function[1])


# @pytest.mark.parametrize(
#     "function",
#     [
#         ("from_running", (Machine("none"),)),
#         (
#             "from_machine_storage",
#             (
#                 Machine("none"),
#                 "filename",
#             ),
#         ),
#     ],
# )
# def test_expload_not_connected(function: str) -> None:
#     with pytest.raises(ConnectionError):
#         getattr(Experiment, function[0]).__call__(*function[1])
