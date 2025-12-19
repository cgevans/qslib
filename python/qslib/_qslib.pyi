# SPDX-FileCopyrightText: 2021 - 2024 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""Type stubs for qslib._qslib Rust extension module."""

from typing import Any, Dict, List, Optional, Tuple, Union
import polars as pl

# Exceptions
class QslibException(Exception):
    """Base exception for qslib errors."""
    ...

class CommandResponseError(QslibException):
    """Error in command response from QuantStudio machine."""
    ...

class CommandError(CommandResponseError):
    """Error returned by a command."""
    ...

class UnexpectedMessageResponse(CommandResponseError):
    """Received an unexpected message type as response."""
    ...

class DisconnectedBeforeResponse(CommandResponseError):
    """Connection was lost before receiving a response."""
    ...


# Connection classes
class QSConnection:
    """Connection to a QuantStudio machine for sending commands and receiving responses."""

    def __init__(
        self,
        host: str,
        port: int = 7443,
        connection_type: str = "Auto",
        timeout: Optional[int] = 10,
    ) -> None:
        """Create a new QSConnection.

        Args:
            host: Hostname or IP address to connect to
            port: Port number (default: 7443)
            connection_type: Connection type - "Auto", "SSL", or "TCP" (default: "Auto")
            timeout: Connection timeout in seconds (default: 10)

        Raises:
            ValueError: If connection_type is invalid or connection fails
        """
        ...

    def run_command(self, command: Union[str, bytes]) -> "MessageResponse":
        """Send a command.

        Args:
            command: Command string or bytes to send

        Returns:
            MessageResponse object to get the server's response

        Raises:
            ValueError: If command is invalid or connection error occurs
        """
        ...

    def run_command_bytes(self, bytes: bytes) -> "MessageResponse":
        """Send a raw bytes command.

        Args:
            bytes: Raw command bytes to send

        Returns:
            MessageResponse object to get the server's response

        Raises:
            ValueError: If command is invalid or connection error occurs
        """
        ...

    def subscribe_log(self, topics: List[str]) -> "LogReceiver":
        """Subscribe to log messages for specified topics.

        Args:
            topics: List of topic strings to subscribe to

        Returns:
            LogReceiver object to receive log messages

        Raises:
            ValueError: If subscription fails
        """
        ...

    def connected(self) -> bool:
        """Check if the connection is still active.

        Returns:
            True if connected, False otherwise
        """
        ...

    def authenticate(self, password: str) -> None:
        """Authenticate with a password.

        Args:
            password: Password string

        Raises:
            CommandError: If authentication fails
        """
        ...

    def set_access_level(self, level: str) -> None:
        """Set access level.

        Args:
            level: Access level string ("Guest", "Observer", "Controller",
                   "Administrator", "Full")

        Raises:
            CommandError: If setting access level fails
            ValueError: If level is invalid
        """
        ...

    def get_running_protocol(self) -> "Protocol":
        """Get the currently running protocol (parsed by Rust).

        Returns:
            Protocol object with parsed protocol information

        Raises:
            CommandError: If no protocol is running or if an error occurs
        """
        ...

    def expect_ident(self, ident: Any) -> "MessageResponse":
        """Wait for a specific message identifier.

        Args:
            ident: The message identifier to wait for

        Returns:
            MessageResponse for the expected message
        """
        ...


class MessageResponse:
    """Response handler for QuantStudio machine commands."""

    def get_response(self) -> str:
        """Get the response from the machine.

        Returns:
            Response message from server

        Raises:
            CommandError: If response is an error
            UnexpectedMessageResponse: If response type is unexpected
            DisconnectedBeforeResponse: If connection lost
        """
        ...

    def get_response_bytes(self) -> bytes:
        """Get the response from the machine as bytes.

        Returns:
            Response message from server as bytes

        Raises:
            CommandError: If response is an error
            UnexpectedMessageResponse: If response type is unexpected
            DisconnectedBeforeResponse: If connection lost
        """
        ...

    def __next__(self) -> str:
        """Get next response from server (alias for get_response)."""
        ...

    def get_ack(self) -> None:
        """Get acknowledgment from server.

        Raises:
            CommandError: If response is an error
            UnexpectedMessageResponse: If response is not an acknowledgment
            DisconnectedBeforeResponse: If connection lost
        """
        ...

    def get_response_with_timeout(self, timeout: int) -> str:
        """Get response from server with timeout.

        Args:
            timeout: Timeout in seconds

        Returns:
            Response message from server

        Raises:
            TimeoutError: If timeout occurs
            CommandError: If response is an error
        """
        ...


class LogReceiver:
    """Receiver for subscribed log messages."""

    def __next__(self) -> Any:
        """Get next log message.

        Returns:
            LogMessage object with topic and message attributes

        Raises:
            ValueError: If no message available
        """
        ...

    def next(self) -> Any:
        """Get next log message (alias for __next__)."""
        ...


class Protocol:
    """Parsed protocol from QuantStudio machine (Rust implementation)."""

    @property
    def name(self) -> str:
        """Protocol name."""
        ...

    @property
    def volume(self) -> float:
        """Sample volume in microliters."""
        ...

    @property
    def runmode(self) -> str:
        """Run mode string."""
        ...

    @property
    def stages(self) -> int:
        """Number of stages in the protocol."""
        ...

    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...


class Command:
    """SCPI command builder."""

    def __init__(self, command: str) -> None:
        """Create a new command.

        Args:
            command: The command string (e.g., "SYST:STAT?")
        """
        ...

    def to_string(self) -> str:
        """Convert command to string representation."""
        ...

    @staticmethod
    def from_string(s: str) -> "Command":
        """Parse a command from string.

        Args:
            s: Command string to parse

        Returns:
            Parsed Command object

        Raises:
            ValueError: If parsing fails
        """
        ...

    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...


class OkResponse:
    """Parsed OK response from QuantStudio machine."""

    options: Dict[str, Any]
    """Options dictionary from response."""

    args: List[Any]
    """Positional arguments from response."""


# Data classes
class FilterDataCollection:
    """Collection of filter data from an experiment."""

    name: str
    """Name of the data collection."""

    plate_point_data: List["PlatePointData"]
    """List of plate point data entries."""

    @staticmethod
    def read_file(path: str) -> "FilterDataCollection":
        """Read filter data from an XML file.

        Args:
            path: Path to the XML file

        Returns:
            Parsed FilterDataCollection

        Raises:
            ValueError: If file cannot be read or parsed
        """
        ...

    def to_polars(self) -> pl.DataFrame:
        """Convert to a Polars DataFrame.

        Returns:
            DataFrame with filter data
        """
        ...


class PlatePointData:
    """Data for a single plate point (stage/cycle/step/point combination)."""

    stage: int
    """Stage number."""

    cycle: int
    """Cycle number."""

    step: int
    """Step number."""

    point: int
    """Point number."""

    plate_data: List[Any]
    """List of plate data for this point."""

    def to_polars(self) -> pl.DataFrame:
        """Convert to a Polars DataFrame."""
        ...


# Message log classes
class RunLogInfo:
    """Parsed run log information."""

    runstarttime: Optional[float]
    """Unix timestamp when run started."""

    runendtime: Optional[float]
    """Unix timestamp when run ended."""

    prerunstart: Optional[float]
    """Unix timestamp when prerun started."""

    activestarttime: Optional[float]
    """Unix timestamp when active run started."""

    activeendtime: Optional[float]
    """Unix timestamp when active run ended."""

    runstate: str
    """Current run state (INIT, RUNNING, COMPLETE, ABORTED, STOPPED)."""

    stage_names: List[str]
    """Names of stages in the protocol."""

    stage_start_times: List[float]
    """Start timestamps for each stage."""

    stage_end_times: List[Optional[float]]
    """End timestamps for each stage (None if not ended)."""

    @staticmethod
    def parse(log: bytes) -> "RunLogInfo":
        """Parse run log information from message log bytes.

        Args:
            log: Raw message log bytes

        Returns:
            Parsed RunLogInfo
        """
        ...


class TemperatureLog:
    """Parsed temperature log from message log."""

    timestamps: List[float]
    """Unix timestamps for each reading."""

    heatsink_temps: List[float]
    """Heatsink temperatures."""

    cover_temperatures: List[float]
    """Cover temperatures."""

    block_temperatures: List[List[float]]
    """Block temperatures for each zone."""

    sample_temperatures: List[List[float]]
    """Sample temperatures for each zone."""

    num_zones: int
    """Number of temperature zones."""

    @staticmethod
    def parse(log: bytes) -> "TemperatureLog":
        """Parse temperature log from message log bytes.

        Args:
            log: Raw message log bytes

        Returns:
            Parsed TemperatureLog
        """
        ...

    @staticmethod
    def parse_to_polars(log: bytes) -> pl.DataFrame:
        """Parse temperature log directly to Polars DataFrame.

        Args:
            log: Raw message log bytes

        Returns:
            DataFrame with temperature data
        """
        ...


# Plate setup classes
class Sample:
    """Sample definition for plate setup."""

    def __init__(
        self,
        name: str,
        uuid: Optional[str] = None,
        color: Optional[Tuple[int, int, int, int]] = None,
        properties: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
    ) -> None:
        """Create a new sample.

        Args:
            name: Sample name
            uuid: Optional UUID (auto-generated if not provided)
            color: Optional RGBA color tuple
            properties: Optional custom properties
            description: Optional description
        """
        ...

    @property
    def name(self) -> str:
        """Sample name."""
        ...

    @name.setter
    def name(self, value: str) -> None: ...

    @property
    def color(self) -> str:
        """Color as hex string."""
        ...

    @color.setter
    def color(self, value: str) -> None: ...

    @property
    def color_rgba(self) -> Tuple[int, int, int, int]:
        """Color as RGBA tuple."""
        ...

    def set_color_rgba(self, r: int, g: int, b: int, a: int) -> None:
        """Set color from RGBA values."""
        ...

    @property
    def description(self) -> Optional[str]:
        """Sample description."""
        ...

    @description.setter
    def description(self, value: Optional[str]) -> None: ...

    @property
    def uuid(self) -> Optional[str]:
        """Sample UUID."""
        ...


class PlateSetup:
    """Plate setup defining sample locations on a plate."""

    def __init__(
        self,
        name: Optional[str] = None,
        plate_type: Optional[str] = None,
    ) -> None:
        """Create a new plate setup.

        Args:
            name: Optional plate name
            plate_type: Plate type ("TYPE_8X12" for 96-well, "TYPE_16X24" for 384-well)
        """
        ...

    @staticmethod
    def from_xml_string(xml: str) -> "PlateSetup":
        """Parse plate setup from XML string.

        Args:
            xml: XML string containing plate setup

        Returns:
            Parsed PlateSetup

        Raises:
            ValueError: If XML is invalid
        """
        ...

    def to_xml_string(self) -> str:
        """Convert plate setup to XML string.

        Returns:
            XML string representation
        """
        ...

    @property
    def name(self) -> Optional[str]:
        """Plate name."""
        ...

    @name.setter
    def name(self, value: Optional[str]) -> None: ...

    @property
    def barcode(self) -> Optional[str]:
        """Plate barcode."""
        ...

    @barcode.setter
    def barcode(self, value: Optional[str]) -> None: ...

    @property
    def description(self) -> Optional[str]:
        """Plate description."""
        ...

    @description.setter
    def description(self, value: Optional[str]) -> None: ...

    @property
    def rows(self) -> int:
        """Number of rows."""
        ...

    @property
    def columns(self) -> int:
        """Number of columns."""
        ...

    @property
    def plate_type(self) -> str:
        """Plate type string."""
        ...

    @plate_type.setter
    def plate_type(self, value: str) -> None: ...

    def get_well_names(self) -> List[str]:
        """Get list of all well names (e.g., ['A1', 'A2', ...])."""
        ...

    def get_samples_and_wells(self) -> Dict[str, Tuple[Sample, List[str]]]:
        """Get mapping of sample names to (Sample, well_names) tuples."""
        ...

    def get_sample(self, name: str) -> Optional[Sample]:
        """Get a specific sample by name."""
        ...

    def to_line_protocol(
        self,
        timestamp: int,
        run_name: Optional[str] = None,
        machine_name: Optional[str] = None,
    ) -> List[str]:
        """Convert to InfluxDB line protocol format."""
        ...

    def print_debug(self) -> None:
        """Print debug representation."""
        ...

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...


# Module-level functions
def get_n_zones(log: bytes) -> int:
    """Get the number of temperature zones from a message log.

    Args:
        log: Raw message log bytes

    Returns:
        Number of temperature zones

    Raises:
        ValueError: If no temperature data found in log
    """
    ...


def parse_argmap(input: str) -> Dict[str, Any]:
    """Parse a string into an ArgMap (options dictionary).

    Args:
        input: String containing options like "-key=value"

    Returns:
        Dictionary mapping keys to values

    Raises:
        ValueError: If parsing fails
    """
    ...


def parse_argmap_bytes(input: bytes) -> Dict[str, Any]:
    """Parse bytes into an ArgMap (options dictionary).

    Args:
        input: Bytes containing options like "-key=value"

    Returns:
        Dictionary mapping keys to values

    Raises:
        ValueError: If parsing fails
    """
    ...


def parse_value(input: str) -> Any:
    """Parse a string into a Value.

    Args:
        input: String to parse

    Returns:
        Parsed Value (string, int, float, or list)

    Raises:
        ValueError: If parsing fails
    """
    ...


def parse_value_bytes(input: bytes) -> Any:
    """Parse bytes into a Value.

    Args:
        input: Bytes to parse

    Returns:
        Parsed Value (string, int, float, or list)

    Raises:
        ValueError: If parsing fails
    """
    ...
