
from qslib import PlateSetup

def test_plate_setup_equality():
    ps = PlateSetup({
        "s1": ["A1"],
        "s2": ["A2", "B4"],
    })

    ps2 = PlateSetup({
        "s1": ["A1"],
        "s2": ["A2", "B4"],
    })
    assert ps == ps2
    