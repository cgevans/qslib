from __future__ import annotations
from typing import Union
import xml.etree.ElementTree as ET

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

__all__ = ("_find_or_create", "_set_or_create", "_text_or_none")