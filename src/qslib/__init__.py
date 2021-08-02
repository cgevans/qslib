from .machine import Machine
from .experiment import Experiment
from . import tcprotocol as tc
from .tcprotocol import Stage, Step, Protocol

__all__ = ("Machine", "Experiment", "tc")

from importlib.metadata import PackageNotFoundError, version  # pragma: no cover

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = version(dist_name)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError
