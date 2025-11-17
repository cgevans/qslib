# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""(Non-machine/protocol) utility functions for other modules."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ElTr
from datetime import datetime
from typing import Sequence

from functools import wraps
from typing import Any, Callable, TypeVar

T = TypeVar('T')

def cached_method(f: Callable[..., T]) -> Callable[..., T]:
    """Decorator that caches method results on the instance.  Needs the following added to class:
    
    ```python
    def _clear_cache(self: Any) -> None:
        "Clear all method caches on this instance."
        if hasattr(self, '_cached_methods'):
            for cache_name in self._cached_methods:
                if hasattr(self, cache_name):
                    delattr(self, cache_name)
            self._cached_methods.clear()
    ```
    
    """
    cache_name = f'_cache_{f.__name__}'
    
    @wraps(f)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> T:
        # Add _cached_methods and _clear_cache to instance if needed
        if not hasattr(self, '_cached_methods'):
            self._cached_methods = set()
            
        if not hasattr(self, cache_name):
            setattr(self, cache_name, {})
            self._cached_methods.add(cache_name)
            
        cache = getattr(self, cache_name)
        key = (args, tuple(sorted(kwargs.items())))
        
        if key not in cache:
            cache[key] = f(self, *args, **kwargs)
        return cache[key]
    
    return wrapper





def _find_or_create(
    element: ElTr.Element | ElTr.ElementTree, path: str
) -> ElTr.Element:
    """Find the element at path, or create it."""
    if isinstance(element, ElTr.ElementTree):
        element = element.getroot()
    if (maybe_element := element.find(path)) is not None:
        return maybe_element
    for elemname in path.split("/"):
        if maybe_element := element.find(elemname):
            element = maybe_element
        else:
            element = ElTr.SubElement(element, elemname)
    return element


def _set_or_create(
    element: ElTr.Element | ElTr.ElementTree,
    path: str,
    text: str | None = None,
    **kwargs: str,
) -> ElTr.Element:
    """Find or create, then set, the element at path."""
    element_at_path = _find_or_create(element, path)
    for key, val in kwargs.items():
        element_at_path.attrib[key] = val
    if text is not None:
        element_at_path.text = text
    return element_at_path


def _unwrap_tags(val: str) -> str:
    """Remove outer <quote>content</quote> tags from string."""
    return re.sub(r"^<[^>]+?>\n?(.*)\n?</[^>]+?>$", r"\1", val)


def _nowuuid() -> str:
    """Consistent and legible UUID-like string from data."""
    return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")


def _pp_seqsliceint(sss: Sequence[int] | slice | int) -> str:
    """Reasonable natural English description slice/int/sequence."""
    if isinstance(sss, int):
        return str(sss)
    if isinstance(sss, Sequence):
        return str(list(sss))
    if isinstance(sss, slice):
        if sss.start is None:
            ret_str = f"up to {sss.stop}"
        elif sss.stop is None:
            ret_str = f"{sss.start} onwards"
        else:
            ret_str = f"{sss.start} to {sss.stop}"
        if sss.step is not None:
            ret_str += f" by step {sss.step}"
        return ret_str
    raise TypeError


__all__ = (
    "_find_or_create",
    "_set_or_create",
    "_unwrap_tags",
    "_nowuuid",
    "_pp_seqsliceint",
)
