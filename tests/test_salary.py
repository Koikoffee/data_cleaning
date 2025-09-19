# tests/test_salary.py
import math
import pytest
from ETL.transform import parse_salary

@pytest.mark.parametrize("text,expect", [
    ("10 - 20 triệu",        (10_000_000.0, 20_000_000.0, "VND", "range")),
    ("từ 15 triệu",          (15_000_000.0, None,        "VND", "floor")),
    ("đến 30 triệu",         (None,        30_000_000.0, "VND", "ceiling")),
    ("2000 USD",             (2000.0,      2000.0,       "USD", "point")),
    ("15k USD",              (15000.0,     15000.0,      "USD", "point")),
    ("Thoả thuận",           (None,        None,         "VND", "negotiable")),
    ("",                     (None,        None,         "VND", "empty")),
])
def test_parse_salary_basic(text, expect):
    #Ensure salary strings are parsed into (min,max,unit,note).
    got = parse_salary(text)
    # Compare numeric with tolerance, others exact
    for g, e in zip(got, expect):
        if isinstance(e, (int, float)) and e is not None:
            assert g == pytest.approx(e)
        else:
            assert g == e
