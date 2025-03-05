from typing import List, Optional, Union

class QSConnection:
    """Connection to a QuantStudio machine for sending commands and receiving responses"""
    def __init__(self, host: str, port: int = 7443, connection_type: str = "Auto") -> None:
        """Create a new QSConnection
        
        Args:
            host: Hostname or IP address to connect to
            port: Port number (default: 7443) 
            connection_type: Connection type - "Auto", "SSL", or "TCP" (default: "Auto")
            
        Raises:
            ValueError: If connection_type is invalid or connection fails
        """
        ...
    
    def run_command(self, command: Union[str, bytes]) -> MessageResponse:
        """Send a command
        
        Args:
            command: Command string or bytes to send
            
        Returns:
            MessageResponse object to get the server's response
            
        Raises:
            ValueError: If command is invalid or connection error occurs
        """
        ...
        
    def run_command_bytes(self, bytes: bytes) -> MessageResponse:
        """Send a raw bytes command
        
        Args:
            bytes: Raw command bytes to send
            
        Returns:
            MessageResponse object to get the server's response
            
        Raises:
            ValueError: If command is invalid or connection error occurs
        """
        ...
        
    def subscribe_log(self, topics: List[str]) -> LogReceiver:
        """Subscribe to log messages for specified topics
        
        Args:
            topics: List of topic strings to subscribe to
            
        Returns:
            LogReceiver object to receive log messages
            
        Raises:
            ValueError: If subscription fails
        """
        ...
        
    def connected(self) -> bool:
        """Check if the connection is still active
        
        Returns:
            True if connected, False otherwise
        """
        ...

class MessageResponse:
    """Response handler for QuantStudio machine commands"""
    def get_response(self) -> str:
        """Get the response from the machine
        
        Returns:
            Response message from server
            
        Raises:
            ValueError: If response is an error or invalid
        """
        ...
        
    def __next__(self) -> str:
        """Get next response from server (alias for get_response)"""
        ...
        
    def get_ack(self) -> str:
        """Get acknowledgment from server
        
        Returns:
            Empty string on success
            
        Raises:
            ValueError: If response is not an acknowledgment
        """
        ...
        
    def get_response_with_timeout(self, timeout: int) -> str:
        """Get response from server with timeout
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            Response message from server
            
        Raises:
            TimeoutError: If timeout occurs
            ValueError: If response is an error or invalid
        """
        ...

class LogReceiver:
    """Receiver for subscribed log messages"""
    def __next__(self) -> LogMessage:
        """Get next log message
        
        Returns:
            LogMessage containing topic and message
            
        Raises:
            ValueError: If no message available
        """
        ...
        
    def next(self) -> LogMessage:
        """Get next log message (alias for __next__)"""
        ...

class LogMessage:
    """Log message from QuantStudio machine"""
    topic: str  # Topic the message was published to
    message: str  # Content of the log message
