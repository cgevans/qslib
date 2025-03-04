# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

from __future__ import annotations

import asyncio
import io
import logging
import re
import time
from asyncio.futures import Future
from dataclasses import dataclass
from typing import Any, Coroutine, Optional, Protocol, Type

from .scpi_commands import AccessLevel, SCPICommand, _arglist

NL_OR_Q = re.compile(rb"(?:\n|<(/?)([\w.]*)[ *]*>?)")
TIMESTAMP = re.compile(rb"(\d{8,}\.\d{3})")

log = logging.getLogger(__name__)


class Error(Exception):
    pass


class CommandError(Error):
    @staticmethod
    def parse(command: str, ref_index: str, response: str) -> CommandError:
        m = re.match(r"\[(\w+)\] (.*)", response)
        if (not m) or (m[1] not in COM_ERRORS):
            return UnparsedCommandError(command, ref_index, response)
        try:
            return COM_ERRORS[m[1]].parse(command, ref_index, m[2])
        except ValueError:
            return UnparsedCommandError(command, ref_index, response)


@dataclass
class UnparsedCommandError(CommandError):
    """The machine has returned an error that we are not familiar with,
    and that we haven't parsed."""

    command: Optional[str]
    ref_index: Optional[str]
    response: str


@dataclass
class QS_IOError(CommandError):
    command: str
    message: str
    data: dict[str, str]

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> QS_IOError:
        m = re.match(r"(.*) --> (.*)", message)
        if not m:
            raise ValueError

        data = _arglist.parse_string(m[1])[0].opts

        return cls(command, m[2], data)


@dataclass
class InsufficientAccess(CommandError):
    command: str
    requiredAccess: AccessLevel
    currentAccess: AccessLevel
    message: str

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> InsufficientAccess:
        m = re.match(
            r'-requiredAccess="(\w+)" -currentAccess="(\w+)" --> (.*)', message
        )
        if not m:
            raise ValueError
        return cls(command, AccessLevel(m[1]), AccessLevel(m[2]), m[3])


@dataclass
class AuthError(CommandError):
    command: str
    message: str

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> AuthError:
        m = re.match(r"--> (.*)", message)
        if not m:
            raise ValueError
        return cls(command, m[1])


@dataclass
class AccessLevelExceeded(CommandError):
    command: str
    accessLimit: AccessLevel
    message: str

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> AccessLevelExceeded:
        m = re.match(r'-accessLimit="(\w+)" --> (.*)', message)
        if not m:
            raise ValueError
        return cls(command, AccessLevel(m[1]), m[2])


@dataclass
class InvocationError(CommandError):
    command: str
    message: str

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> InvocationError:
        return cls(command, message)


@dataclass
class NoMatch(CommandError):
    command: str
    message: str

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> NoMatch:
        return cls(command, message)


COM_ERRORS: dict[str, Type[CommandError]] = {
    "InsufficientAccess": InsufficientAccess,
    "AuthError": AuthError,
    "AccessLevelExceeded": AccessLevelExceeded,
    "InvocationError": InvocationError,
    "NoMatch": NoMatch,
    "IOError": QS_IOError,
}


class ReplyError(IOError):
    pass


