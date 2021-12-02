from __future__ import annotations
from datetime import datetime
from typing import Sequence, Union
import xml.etree.ElementTree as ET
import re


def _find_or_create(element: ET.Element | ET.ElementTree, path: str) -> ET.Element:
    if isinstance(element, ET.ElementTree):
        element = element.getroot()
    if (m := element.find(path)) is not None:
        return m
    else:
        e = element
        for elemname in path.split("/"):
            if te := e.find(elemname):
                e = te
            else:
                e = ET.SubElement(e, elemname)
    return e


def _set_or_create(
    element: ET.Element | ET.ElementTree, path: str, text=None, **kwargs
) -> ET.Element:
    e = _find_or_create(element, path)
    for k, v in kwargs.items():
        e.attrib[k] = v
    if text is not None:
        e.text = text
    return e


def _text_or_none(element: ET.Element | ET.ElementTree, path: str) -> Union[str, None]:
    if (e := element.find(path)) is not None:
        return e.text
    else:
        return None


def _unwrap_tags(s: str) -> str:
    return re.sub(r"^<[^>]+?>\n?(.*)\n?</[^>]+?>$", r"\1", s)


def _unwrap_tags_bytes(s: bytes) -> bytes:
    return re.sub(rb"^<[^>]+?>\n?(.*)\n?</[^>]+?>$", rb"\1", s)


def _nowuuid() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")


def _pp_seqsliceint(sss: Sequence | slice | int) -> str:
    if isinstance(sss, int):
        return str(sss)
    elif isinstance(sss, Sequence):
        return str(list(sss))
    elif isinstance(sss, slice):
        if sss.start is None:
            s = f"up to {sss.stop}"
        elif sss.stop is None:
            s = f"{sss.start} onwards"
        else:
            s = f"{sss.start} to {sss.stop}"
        if sss.step is not None:
            s += f" by step {sss.step}"
        return s
    else:
        raise TypeError


__all__ = (
    "_find_or_create",
    "_set_or_create",
    "_text_or_none",
    "_unwrap_tags",
    "_unwrap_tags_bytes",
    "_nowuuid",
    "_pp_seqsliceint",
)
