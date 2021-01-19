import sys

from importlib.metadata import PackageNotFoundError, version  # pragma: no cover

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = version(dist_name)
except PackageNotFoundError:
    __version__ = "unknown"  # pragma: no cover
finally:
    del version, PackageNotFoundError

from .data import *
from .qsconnection_async import *