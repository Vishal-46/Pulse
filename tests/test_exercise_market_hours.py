"""Tests for Exercise 1. They SKIP until you implement pulse.quality.rules.is_nse_session."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from pulse.quality.rules import is_nse_session

IST = timezone(timedelta(hours=5, minutes=30))


def check(ts):
    try:
        return is_nse_session(ts)
    except NotImplementedError:
        pytest.skip("Exercise 1 not implemented yet")


@pytest.mark.parametrize("ts, expected", [
    (datetime(2026, 11, 18, 9, 15, tzinfo=IST), True),     # first bar of the day
    (datetime(2026, 11, 18, 15, 29, tzinfo=IST), True),    # last bar of the day
    (datetime(2026, 11, 18, 15, 30, tzinfo=IST), False),   # closing bell: no bar starts here
    (datetime(2026, 11, 18, 9, 14, tzinfo=IST), False),    # pre-open
    (datetime(2026, 11, 18, 3, 45, tzinfo=UTC), True),     # = 09:15 IST, given in UTC
    (datetime(2026, 11, 18, 10, 0, tzinfo=UTC), False),    # = 15:30 IST, given in UTC
    (datetime(2026, 11, 21, 11, 0, tzinfo=IST), False),    # Saturday
])
def test_is_nse_session(ts, expected):
    assert check(ts) is expected
