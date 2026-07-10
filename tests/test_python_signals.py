# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import os
import signal
import socket
import threading
import time
from multiprocessing import get_context

import pytest

from qslib._qslib import QSConnection


def _serve(listener: socket.socket, stop) -> None:
    with listener:
        client, _ = listener.accept()
        with client:
            client.sendall(b"READy -session=1 -product=Test -version=1 -build=1 -capabilities=Index\n")
            client.recv(4096)
            stop.wait(5)


def _silent_connection():
    listener = socket.create_server(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    context = get_context("spawn")
    stop = context.Event()
    process = context.Process(target=_serve, args=(listener, stop))
    process.start()
    listener.close()
    return QSConnection("127.0.0.1", port, "TCP"), stop, process


@pytest.mark.skipif(os.name == "nt", reason="POSIX signal test")
@pytest.mark.parametrize("method", ["get_response", "get_ack", "get_response_with_timeout"])
def test_response_waits_handle_sigint(method: str) -> None:
    connection, stop, server = _silent_connection()
    response = connection.run_command("SILENCE")
    interrupt = threading.Timer(0.2, os.kill, (os.getpid(), signal.SIGINT))
    interrupt.start()
    started = time.monotonic()

    try:
        with pytest.raises(KeyboardInterrupt):
            if method == "get_response_with_timeout":
                response.get_response_with_timeout(30)
            else:
                getattr(response, method)()
        assert time.monotonic() - started < 2
    finally:
        interrupt.cancel()
        stop.set()
        server.join(timeout=2)
        if server.is_alive():
            server.terminate()
            server.join(timeout=2)
