import pandas as pd
import pytest
import vcr
from inline_snapshot import snapshot

from open_meteo_client import OpenMeteoClient

VCR_DIR = "tests/cassettes/"
ENTSOE_TOKEN = "FAKE"


@pytest.fixture
def client():
    return OpenMeteoClient()


@vcr.use_cassette(f"{VCR_DIR}test_get_city.yaml")
def test_get_city(client):
    start = pd.Timestamp("2025-08-04", tz="CET")
    end = start + pd.DateOffset(hours=24)
    ts = client.get_city("paris", 48.8566, 2.3522, start, end)

    assert str(ts) == snapshot(
        """\
2025-08-03 22:00:00+00:00    19.1
2025-08-03 23:00:00+00:00    18.7
2025-08-04 00:00:00+00:00    18.9
2025-08-04 01:00:00+00:00    19.3
2025-08-04 02:00:00+00:00    18.9
2025-08-04 03:00:00+00:00    18.4
2025-08-04 04:00:00+00:00    18.3
2025-08-04 05:00:00+00:00    18.2
2025-08-04 06:00:00+00:00    18.5
2025-08-04 07:00:00+00:00    19.0
2025-08-04 08:00:00+00:00    19.9
2025-08-04 09:00:00+00:00    20.5
2025-08-04 10:00:00+00:00    21.6
2025-08-04 11:00:00+00:00    22.8
2025-08-04 12:00:00+00:00    23.4
2025-08-04 13:00:00+00:00    26.5
2025-08-04 14:00:00+00:00    27.3
2025-08-04 15:00:00+00:00    28.4
2025-08-04 16:00:00+00:00    28.3
2025-08-04 17:00:00+00:00    28.0
2025-08-04 18:00:00+00:00    27.4
2025-08-04 19:00:00+00:00    26.0
2025-08-04 20:00:00+00:00    24.5
2025-08-04 21:00:00+00:00    23.5
Freq: h, Name: paris, dtype: float64\
"""
    )


@vcr.use_cassette(f"{VCR_DIR}test_get_averaged.yaml")
def test_get_averaged(client):
    start = pd.Timestamp("2025-08-04", tz="CET")
    end = start + pd.DateOffset(hours=3)
    ts = client.get_averaged(start=start, end=end)

    assert str(ts) == snapshot(
        """\
2025-08-03 22:00:00+00:00    20.39
2025-08-03 23:00:00+00:00    19.74
2025-08-04 00:00:00+00:00    19.47
Freq: h, Name: temp, dtype: float64\
"""
    )
