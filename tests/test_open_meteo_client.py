import pandas as pd
import pytest
import vcr
from inline_snapshot import snapshot

from enstoe_client import THRESHOLD, EntsoeHourlyClient

VCR_DIR = "tests/cassettes/"
ENTSOE_TOKEN = "FAKE"


@pytest.fixture
def client():
    return EntsoeHourlyClient(api_key=ENTSOE_TOKEN)


@vcr.use_cassette(f"{VCR_DIR}test_after_threshold.yaml")
def test_after_threshold(client):
    start = THRESHOLD
    end = start + pd.DateOffset(days=1)
    ts = client.get_hourly_load(start=start, end=end)

    assert str(ts) == snapshot(
        """\
                               load
2024-12-31 00:00:00+00:00  61705.00
2024-12-31 01:00:00+00:00  60431.00
2024-12-31 02:00:00+00:00  58513.00
2024-12-31 03:00:00+00:00  57611.00
2024-12-31 04:00:00+00:00  59018.00
2024-12-31 05:00:00+00:00  61929.00
2024-12-31 06:00:00+00:00  64919.00
2024-12-31 07:00:00+00:00  67517.00
2024-12-31 08:00:00+00:00  69284.00
2024-12-31 09:00:00+00:00  70363.00
2024-12-31 10:00:00+00:00  70375.00
2024-12-31 11:00:00+00:00  71483.00
2024-12-31 12:00:00+00:00  69942.00
2024-12-31 13:00:00+00:00  67517.00
2024-12-31 14:00:00+00:00  65788.00
2024-12-31 15:00:00+00:00  65118.00
2024-12-31 16:00:00+00:00  67561.00
2024-12-31 17:00:00+00:00  69339.00
2024-12-31 18:00:00+00:00  68220.00
2024-12-31 19:00:00+00:00  65265.00
2024-12-31 20:00:00+00:00  62886.00
2024-12-31 21:00:00+00:00  63984.00
2024-12-31 22:00:00+00:00  64603.00
2024-12-31 23:00:00+00:00  63057.75\
"""
    )


@vcr.use_cassette(f"{VCR_DIR}test_before_threshold.yaml")
def test_before_threshold(client):
    end = THRESHOLD
    start = end - pd.DateOffset(hours=3)
    ts = client.get_hourly_load(start=start, end=end)

    assert str(ts) == snapshot(
        """\
                              load
2024-12-30 21:00:00+00:00  66599.0
2024-12-30 22:00:00+00:00  66176.0
2024-12-30 23:00:00+00:00  63815.0\
"""
    )


@vcr.use_cassette(f"{VCR_DIR}test_over_threshold.yaml")
def test_over_threshold(client):
    start = THRESHOLD - pd.DateOffset(hours=2)
    end = THRESHOLD + pd.DateOffset(hours=2)
    ts = client.get_hourly_load(start=start, end=end)

    assert str(ts) == snapshot(
        """\
                              load
2024-12-30 22:00:00+00:00  66176.0
2024-12-30 23:00:00+00:00  63815.0
2024-12-31 00:00:00+00:00  61705.0
2024-12-31 01:00:00+00:00  60431.0\
"""
    )
