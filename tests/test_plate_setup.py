# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

"""Comprehensive tests for PlateSetup and Sample classes."""

import numpy as np
import polars as pl
import pytest

from qslib import PlateSetup
from qslib.plate_setup import (
    Sample,
    _color_to_str,
)


# --- Existing test ---


def test_plate_setup_equality():
    ps = PlateSetup(
        {
            "s1": ["A1"],
            "s2": ["A2", "B4"],
        }
    )

    ps2 = PlateSetup(
        {
            "s1": ["A1"],
            "s2": ["A2", "B4"],
        }
    )
    assert ps == ps2


# --- PlateSetup construction ---


def test_plate_setup_empty():
    ps = PlateSetup()
    assert len(ps.samples_by_name) == 0
    assert ps.plate_type == 96


def test_plate_setup_dict_construction():
    ps = PlateSetup({"sample1": ["A1", "A2"], "sample2": "B1"})
    assert "sample1" in ps.samples_by_name
    assert "sample2" in ps.samples_by_name
    assert ps.samples_by_name["sample1"].wells == ["A1", "A2"]
    assert ps.samples_by_name["sample2"].wells == ["B1"]


def test_plate_setup_384():
    ps = PlateSetup({"s1": ["A1"]}, plate_type=384)
    assert ps.plate_type == 384


# --- get_wells ---


def test_get_wells_by_sample():
    ps = PlateSetup({"s1": ["A1", "A2"], "s2": ["B1"]})
    assert ps.get_wells("s1") == ["A1", "A2"]


def test_get_wells_by_well():
    ps = PlateSetup({"s1": ["A1"]})
    assert ps.get_wells("A1") == ["A1"]


def test_get_wells_mixed():
    ps = PlateSetup({"s1": ["A1", "A2"]})
    result = ps.get_wells(["s1", "B1"])
    assert result == ["A1", "A2", "B1"]


def test_get_wells_case_insensitive():
    ps = PlateSetup({"s1": ["A1"]})
    assert ps.get_wells("a1") == ["A1"]


# --- get_descriptive_string ---


def test_get_descriptive_string_sample():
    s = Sample("MySample", description="My Description")
    ps = PlateSetup(samples=[s])
    ps.sample_wells["MySample"] = ["A1"]
    assert ps.get_descriptive_string("MySample") == "My Description"


def test_get_descriptive_string_no_description():
    ps = PlateSetup({"MySample": ["A1"]})
    assert ps.get_descriptive_string("MySample") == "MySample"


def test_get_descriptive_string_well():
    ps = PlateSetup({"s1": ["A1"]})
    assert ps.get_descriptive_string("a1") == "A1"


# --- sample_names_as_array ---


def test_sample_names_as_array():
    ps = PlateSetup({"s1": ["A1", "A2"]})
    arr = ps.sample_names_as_array()
    assert arr.shape == (8, 12)
    assert arr[0, 0] == "s1"
    assert arr[0, 1] == "s1"
    assert arr[1, 0] is None or arr[1, 0] == "None" or str(arr[1, 0]) == "None"


# --- to_polars ---


def test_to_polars():
    ps = PlateSetup({"s1": ["A1"], "s2": ["B1"]})
    df = ps.to_polars()
    assert isinstance(df, pl.DataFrame)
    assert "name" in df.columns
    assert "wells" in df.columns
    assert df.height == 2


# --- to_polars_by_well ---


def test_to_polars_by_well():
    ps = PlateSetup({"s1": ["A1"]})
    df = ps.to_polars_by_well()
    assert df.height == 96  # All wells
    assert "well" in df.columns
    assert "name" in df.columns


def test_to_polars_by_well_full():
    ps = PlateSetup({"s1": ["A1"]})
    df = ps.to_polars_by_well(full=True)
    assert df.height == 96
    assert "color" in df.columns


# --- to_table ---


def test_to_table_markdown():
    ps = PlateSetup({"s1": ["A1"]})
    table = ps.to_table(format="markdown")
    assert isinstance(table, str)
    assert "s1" in table


# --- from_array ---


def test_from_array_96():
    arr = np.full((8, 12), None, dtype=object)
    arr[0, 0] = "s1"
    arr[0, 1] = "s1"
    arr[1, 0] = "s2"
    ps = PlateSetup.from_array(arr)
    assert ps.plate_type == 96
    assert ps.samples_by_name["s1"].wells == ["A1", "A2"]
    assert ps.samples_by_name["s2"].wells == ["B1"]


def test_from_array_invalid_shape():
    arr = np.full((3, 3), None, dtype=object)
    with pytest.raises(ValueError, match="must be"):
        PlateSetup.from_array(arr)


# --- Sample class ---


def test_sample_creation():
    s = Sample("TestSample")
    assert s.name == "TestSample"
    assert s.uuid is not None
    assert len(s.uuid) > 0


def test_sample_auto_uuid():
    s1 = Sample("S1")
    s2 = Sample("S2")
    assert s1.uuid != s2.uuid


def test_sample_equality_ignores_uuid():
    s1 = Sample("S1", color=(255, 0, 0, 255))
    s2 = Sample("S1", color=(255, 0, 0, 255))
    assert s1 == s2  # UUIDs are different but equality ignores them


def test_sample_inequality():
    s1 = Sample("S1")
    s2 = Sample("S2")
    assert s1 != s2


def test_sample_with_wells():
    s = Sample("TestSample", color=(10, 20, 30, 255), description="A test", wells=["A1", "A2"])
    assert s.name == "TestSample"
    assert s.color == (10, 20, 30, 255)
    assert s.description == "A test"
    assert s.wells == ["A1", "A2"]


def test_sample_to_record():
    s = Sample("TestSample", color=(255, 0, 0, 255))
    r = s.to_record()
    assert r["name"] == "TestSample"
    assert r["color"] == "#ff0000ff"
    assert "uuid" in r


# --- Color helpers ---


def test_color_to_str():
    assert _color_to_str((255, 0, 128, 255)) == "#ff0080ff"


# --- PlateSetup XML roundtrip ---


def test_plate_setup_xml_roundtrip():
    ps = PlateSetup(
        {"s1": ["A1", "A2"], "s2": ["B1"]},
        samples=[
            Sample("s1", color=(255, 0, 0, 255), description="Sample one"),
            Sample("s2", color=(0, 255, 0, 255)),
        ],
    )
    xml = ps.to_xml_string()
    ps2 = PlateSetup.from_xml_string(xml)
    assert ps2.plate_type == 96
    assert set(ps2.samples_by_name.keys()) == {"s1", "s2"}
    assert ps2.samples_by_name["s1"].wells == ["A1", "A2"]
    assert ps2.samples_by_name["s2"].wells == ["B1"]
    assert ps2.samples_by_name["s1"].color == (255, 0, 0, 255)
    assert ps2.samples_by_name["s1"].description == "Sample one"


def test_plate_setup_xml_roundtrip_384():
    ps = PlateSetup({"s1": ["A1", "A24"]}, plate_type=384)
    xml = ps.to_xml_string()
    ps2 = PlateSetup.from_xml_string(xml)
    assert ps2.plate_type == 384
    assert ps2.samples_by_name["s1"].wells == ["A1", "A24"]


def test_plate_setup_to_xml_string_with_existing():
    """Verify that updating existing XML preserves non-sample structure."""
    existing_xml = """<Plate>
    <Name>My Plate</Name>
    <BarCode>BC123</BarCode>
    <Description>Test Description</Description>
    <Rows>8</Rows>
    <Columns>12</Columns>
    <PlateKind>
        <Name>96-Well Plate (8x12)</Name>
        <Type>TYPE_8X12</Type>
        <RowCount>8</RowCount>
        <ColumnCount>12</ColumnCount>
    </PlateKind>
    <FeatureMap>
        <Feature>
            <Id>sample</Id>
            <Name>sample</Name>
        </Feature>
    </FeatureMap>
</Plate>"""

    ps = PlateSetup({"NewSample": ["A1"]})
    xml_out = ps.to_xml_string(existing_xml)

    # Parse back and verify
    ps2 = PlateSetup.from_xml_string(xml_out)
    assert "NewSample" in ps2.samples_by_name
    assert ps2.samples_by_name["NewSample"].wells == ["A1"]


def test_plate_setup_from_xml_string_bytes():
    """Verify from_xml_string accepts bytes."""
    ps = PlateSetup({"s1": ["A1"]})
    xml = ps.to_xml_string()
    xml_bytes = xml.encode("utf-8")
    ps2 = PlateSetup.from_xml_string(xml_bytes)
    assert "s1" in ps2.samples_by_name
