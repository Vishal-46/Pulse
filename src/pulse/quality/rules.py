"""Business rules about NSE market data, in two forms:

* is_nse_session()   plain Python, used by the producer side and unit tests
* in_session_col()   the SAME rule as a Spark column expression, used by the Silver job

Rule: a bar is "in session" if it STARTS Monday-Friday, 09:15 <= time < 15:30 India time.
(Exchange holidays are ignored: that would need a holiday calendar table.)
"""

from datetime import datetime, time
from zoneinfo import ZoneInfo

from pyspark.sql import Column
from pyspark.sql import functions as F

NSE_TZ_NAME = "Asia/Kolkata"
NSE_TZ = ZoneInfo(NSE_TZ_NAME)
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)
SESSION_MINUTES = 375  # 09:15 -> 15:30 = 6h15m = number of 1-minute bars in a full session


def is_nse_session(ts: datetime) -> bool:
    # Convert FIRST: our timestamps are UTC, and 09:15 IST is 03:45 UTC.
    local = ts.astimezone(NSE_TZ)
    # weekday(): Monday=0 ... Sunday=6, so < 5 means Mon-Fri.
    # "< SESSION_CLOSE" (not <=): no bar STARTS at 15:30, the market closes then.
    return local.weekday() < 5 and SESSION_OPEN <= local.time() < SESSION_CLOSE


def to_nse_local(ts_col: str | Column) -> Column:
    """UTC timestamp column -> India wall-clock time (session timezone is UTC)."""
    return F.from_utc_timestamp(F.col(ts_col) if isinstance(ts_col, str) else ts_col, NSE_TZ_NAME)


def in_session_col(ts_col: str = "ts") -> Column:
    local = to_nse_local(ts_col)
    minute_of_day = F.hour(local) * 60 + F.minute(local)
    open_m = SESSION_OPEN.hour * 60 + SESSION_OPEN.minute  # 555
    close_m = SESSION_CLOSE.hour * 60 + SESSION_CLOSE.minute  # 930
    # Spark dayofweek(): Sunday=1 ... Saturday=7, so 2..6 means Mon-Fri (differs from Python!)
    return F.dayofweek(local).between(2, 6) & (minute_of_day >= open_m) & (minute_of_day < close_m)
