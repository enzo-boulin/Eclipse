from datetime import timezone

import pandas as pd
import pytest
from pandas.testing import assert_series_equal

from utils import format_ts


@pytest.fixture
def base_series():
    """Simple timezone-aware series for testing."""
    idx = pd.date_range("2025-11-04 10:00", periods=3, freq="h", tz="Europe/Paris")
    return pd.Series([1, 2, 3], index=idx)


def test_format_ts_basic(base_series):
    """It should correctly reindex, convert to UTC, and floor/ceil bounds."""
    start = pd.Timestamp("2025-11-04 10:30", tz="Europe/Paris")
    end = pd.Timestamp("2025-11-04 12:10", tz="Europe/Paris")

    result = format_ts(base_series, start, end)

    expected_index = pd.date_range(
        "2025-11-04 09:00",  # floored to hour before start, UTC equivalent
        "2025-11-04 11:00",  # ceiled after end, UTC equivalent
        freq="1h",
        tz="UTC",
    )
    expected = pd.Series([1.0, 2.0, 3.0], index=expected_index)

    assert_series_equal(result, expected, check_dtype=False)


def test_format_ts_with_naive_index():
    """It should localize a tz-naive series if ts_tz is provided."""
    idx = pd.date_range("2025-11-04 10:00", periods=3, freq="h")  # no tz
    ts = pd.Series([1, 2, 3], index=idx)

    start = pd.Timestamp("2025-11-04 10:00", tz="Europe/Paris")
    end = pd.Timestamp("2025-11-04 12:00", tz="Europe/Paris")

    result = format_ts(ts, start, end, ts_tz="Europe/Paris")

    assert result.index.tz == timezone.utc
    assert result.isna().sum() == 0


def test_format_ts_raises_if_naive_start_or_end(base_series):
    """It should raise if start or end are tz-naive."""
    start = pd.Timestamp("2025-11-04 10:00")  # no tz
    end = pd.Timestamp("2025-11-04 12:00")  # no tz

    with pytest.raises(ValueError, match="timezone-aware"):
        format_ts(base_series, start, end)


def test_format_ts_raises_if_naive_index_and_no_tz():
    """It should raise if index is tz-naive and no tz_tz provided."""
    idx = pd.date_range("2025-11-04 10:00", periods=3, freq="h")
    ts = pd.Series([1, 2, 3], index=idx)

    start = pd.Timestamp("2025-11-04 10:00", tz="Europe/Paris")
    end = pd.Timestamp("2025-11-04 12:00", tz="Europe/Paris")

    with pytest.raises(ValueError, match="tz-naive"):
        format_ts(ts, start, end)


def test_format_ts_inserts_nan_if_missing(base_series):
    """It should insert NaN for missing hours between start and end."""
    ts = base_series.drop(base_series.index[1])

    start = pd.Timestamp("2025-11-04 10:30:00+01:00")
    end = pd.Timestamp("2025-11-04 12:12:00+01:00")

    result = format_ts(ts, start, end)

    assert result.isna().sum() == 1
    assert result.index.tz == timezone.utc


def test_format_ts_invalid_freq(base_series):
    """It should raise if freq is invalid."""
    start = pd.Timestamp("2025-11-04 10:00", tz="Europe/Paris")
    end = pd.Timestamp("2025-11-04 12:00", tz="Europe/Paris")

    with pytest.raises(ValueError, match="Failed to create target index"):
        format_ts(base_series, start, end, freq="invalid_freq")
