from importlib.metadata import PackageNotFoundError, version  # pragma: no cover

from .data import *
from .qsconnection_async import *

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = version(dist_name)
except PackageNotFoundError:
    __version__ = "unknown"  # pragma: no cover
finally:
    del version, PackageNotFoundError
