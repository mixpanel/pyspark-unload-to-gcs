from datetime import datetime, timezone

from export import datetime_to_ms, ms_to_datetime


def test_ms_to_datetime():
    result = ms_to_datetime(1704067200000)
    expected = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert result == expected


def test_datetime_to_ms():
    dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    result = datetime_to_ms(dt)
    assert result == 1704067200000
