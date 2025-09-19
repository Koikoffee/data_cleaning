# tests/test_address.py
from ETL.transform import split_address_joined, format_pairs

def test_split_address_joined_single_pair():
    # Should split into (city, district) and keep canonical names.
    city, dist = split_address_joined("Hồ Chí Minh: Quận 10")
    assert city in ("TP. Hồ Chí Minh", "Hồ Chí Minh")
    assert "Quận 10" in (dist or "")

def test_format_pairs_multiple():
    # Multiple pairs joined should keep both pairs in readable string.
    s = "Hà Nội, Cầu Giấy; Hồ Chí Minh: Bình Thạnh"
    pairs = format_pairs(s)
    assert "Hà Nội" in pairs and "Cầu Giấy" in pairs
    assert ("Hồ Chí Minh" in pairs) or ("TP. Hồ Chí Minh" in pairs)
    assert "Bình Thạnh" in pairs