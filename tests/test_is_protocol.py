import asyncio
import logging

import pytest

from qslib.qs_is_protocol import QS_IS_Protocol


@pytest.fixture
def fake_connection():
    q = QS_IS_Protocol.__new__(QS_IS_Protocol)
    q.connection_made(None)
    return q


def test_partial_quote(
    fake_connection: QS_IS_Protocol,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    def setparsed(msg):
        fake_connection._parsed = msg

    monkeypatch.setattr(asyncio, "create_task", lambda x: x)
    monkeypatch.setattr(fake_connection, "parse_message", setparsed)

    fake_connection.data_received(b"<tag>\n")
    assert fake_connection.quote_stack == [b"tag"]

    fake_connection.data_received(b"12345\n")

    with caplog.at_level(logging.DEBUG, logger="qslib.qs_is_protocol"):
        fake_connection.data_received(b"</ta")

    assert "Unclosed tag opener: b'</ta'" in caplog.messages
    assert fake_connection.quote_stack == [b"tag"]

    fake_connection.data_received(b"g>\n")
    assert fake_connection.quote_stack == []

    assert fake_connection._parsed == b"<tag>\n12345\n</tag>\n"
