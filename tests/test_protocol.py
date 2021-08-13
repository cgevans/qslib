import pytest
import qslib.tcprotocol as tc
import numpy as np

def test_temperature_lists() -> None:
    a = 52.3
    a_l = [52.3]*6
    a_a = np.array(a_l)

    d_a = np.linspace(40, 46, 6)
    d_l = list(d_a)

    
    assert tc._temperature_str(a) == tc._temperature_str(a_l) == tc._temperature_str(a_a) == "52.30°C"
    assert tc._temperature_str(d_a) == tc._temperature_str(d_l)