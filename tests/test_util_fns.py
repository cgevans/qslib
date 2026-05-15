# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import xml.etree.ElementTree as ET

import hypothesis.strategies as st
import pytest  # noqa
from hypothesis import given

from qslib._util import (
    _find_or_create,
    _pp_seqsliceint,
    _set_or_create,
    _unwrap_tags,
)
from qslib.data import FilterSet
from qslib.machine import FilterDataFilename


@given(
    stage=st.integers(0, 10000),
    cycle=st.integers(0, 10000),
    step=st.integers(0, 10000),
    point=st.integers(0, 10000),
    em=st.integers(1, 6),
    ex=st.integers(1, 6),
)
def test_fd_fn(stage, cycle, step, point, em, ex):
    fds = f"S{stage:02}_C{cycle:03}_T{step:02}_P{point:04}_M{em}_X{ex}"
    fdfn = fds + "_filterdata.xml"

    x = FilterDataFilename.fromstring(fdfn)

    assert x.filterset == FilterSet(ex, em)
    assert x.stage == stage
    assert x.cycle == cycle
    assert x.step == step
    assert x.point == point

    assert fdfn == x.tostring()


def test_pp_seqsliceint():
    assert _pp_seqsliceint(5) == "5"
    assert _pp_seqsliceint(slice(1, 5)) == "1 to 5"
    assert _pp_seqsliceint(slice(5, None)) == "5 onwards"
    assert _pp_seqsliceint(slice(None, 5)) == "up to 5"
    assert _pp_seqsliceint([1, 2, 5]) == "[1, 2, 5]"
    assert _pp_seqsliceint(slice(1, 5, 2)) == "1 to 5 by step 2"
    with pytest.raises(TypeError):
        _pp_seqsliceint(5.5)  # type: ignore


# --- Phase 5: New utility function edge case tests ---


def test_unwrap_tags_basic():
    """_unwrap_tags should strip outer XML-like tags from a string."""
    assert _unwrap_tags("<quote>hello world</quote>") == "hello world"
    assert _unwrap_tags("<tag>content</tag>") == "content"


def test_unwrap_tags_with_newlines():
    """_unwrap_tags should handle content with leading/trailing newlines inside tags."""
    result = _unwrap_tags("<quote>\nmultiline\ncontent\n</quote>")
    assert "multiline\ncontent" in result


def test_unwrap_tags_no_tags():
    """_unwrap_tags should return the string unchanged if there are no wrapping tags."""
    assert _unwrap_tags("plain text") == "plain text"
    assert _unwrap_tags("no <partial> tags") == "no <partial> tags"


def test_find_or_create_existing():
    """_find_or_create should return existing element when path exists."""
    root = ET.Element("root")
    child = ET.SubElement(root, "child")
    child.text = "existing"

    result = _find_or_create(root, "child")
    assert result is child
    assert result.text == "existing"


def test_find_or_create_missing():
    """_find_or_create should create the element when path does not exist."""
    root = ET.Element("root")

    result = _find_or_create(root, "newchild")
    assert result.tag == "newchild"
    # The newly created element should be a child of root
    assert root.find("newchild") is result


def test_find_or_create_nested_path():
    """_find_or_create should create intermediate elements for nested paths."""
    root = ET.Element("root")

    result = _find_or_create(root, "parent/child")
    assert result.tag == "child"
    # The parent element should also have been created
    parent = root.find("parent")
    assert parent is not None
    assert parent.find("child") is result


def test_find_or_create_from_element_tree():
    """_find_or_create should work with an ElementTree (not just Element)."""
    root = ET.Element("root")
    tree = ET.ElementTree(root)
    child = ET.SubElement(root, "child")
    child.text = "via tree"

    result = _find_or_create(tree, "child")
    assert result is child
    assert result.text == "via tree"


def test_set_or_create_new_element():
    """_set_or_create should create an element and set text/attributes."""
    root = ET.Element("root")

    result = _set_or_create(root, "item", text="hello", id="42")
    assert result.tag == "item"
    assert result.text == "hello"
    assert result.attrib["id"] == "42"


def test_set_or_create_existing_element():
    """_set_or_create should update text/attributes on an existing element."""
    root = ET.Element("root")
    item = ET.SubElement(root, "item")
    item.text = "old"
    item.attrib["id"] = "1"

    result = _set_or_create(root, "item", text="new", id="2")
    assert result is item
    assert result.text == "new"
    assert result.attrib["id"] == "2"
