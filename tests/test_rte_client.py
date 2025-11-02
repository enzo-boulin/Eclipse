import json

import pandas as pd
import requests
from inline_snapshot import snapshot

from config import PrevisionType
from rte_client import RTEClient


class DummyResponse:
    def __init__(self, status_code=200, body=None, text=""):
        self.status_code = status_code
        self._body = body or {}
        self.text = text

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def fake_post(url, headers, data, timeout):
    return DummyResponse(
        200,
        body={
            "access_token": "tok-abc",
            "token_type": "Bearer",
            "expires_in": 3600,
        },
    )


def fake_request(json_file: str):
    api_body = json.load(open(json_file, "r", encoding="utf-8"))

    def fn(method, url, headers, params, data, timeout):
        return DummyResponse(200, body=api_body)

    return fn


# def test_basic_auth_header():
#     client = RTEClient("myid", "mysecret", token_cache_file=None)
#     header = client._basic_auth_header()
#     assert header.startswith("Basic ")

#     payload = header.split(" ", 1)[1]
#     assert base64.b64decode(payload.encode()).decode() == "myid:mysecret"


# def test_get_access_token_success(monkeypatch, tmp_path):
#     called = {}

#     def fake_post(url, headers, data, timeout):
#         called["url"] = url
#         return DummyResponse(
#             200,
#             body={
#                 "access_token": "tok-123",
#                 "token_type": "Bearer",
#                 "expires_in": 3600,
#             },
#         )

#     monkeypatch.setattr("requests.post", fake_post)

#     cache_file = tmp_path / "token_cache.json"
#     client = RTEClient("id", "secret", token_cache_file=str(cache_file))
#     token = client.get_access_token()
#     assert token == "tok-123"
#     # token saved to cache file
#     data = json.loads(cache_file.read_text())
#     assert data["access_token"] == "tok-123"


def test_get_france_power_exchanges(monkeypatch):
    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr("requests.request", fake_request("tests/power_exchanges.json"))

    client = RTEClient("id", "secret")
    df = client.get_france_power_exchanges()
    assert str(df) == snapshot("""\
                             value  price
2025-11-03 00:00:00+01:00  15764.8  69.32
2025-11-03 00:15:00+01:00  15622.9  75.80
2025-11-03 00:30:00+01:00  15712.0  29.89
2025-11-03 00:45:00+01:00  15698.2  16.37
2025-11-03 01:00:00+01:00      NaN    NaN
2025-11-03 01:15:00+01:00  16171.1  33.99\
""")


def test_get_short_term_consumptions(monkeypatch):
    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr("requests.request", fake_request("tests/consumptions.json"))

    client = RTEClient("id", "secret")
    consumptions = client.get_short_term_consumptions()
    assert str(consumptions[PrevisionType.REALISED]) == snapshot("""\
                           value
2025-10-01 00:00:00+02:00  44345
2025-10-01 00:15:00+02:00  44174
2025-10-01 00:30:00+02:00  43040
2025-10-01 00:45:00+02:00  41806
2025-10-01 01:00:00+02:00  40872
2025-10-01 01:15:00+02:00  41060\
""")
    assert str(consumptions[PrevisionType.ID]) == snapshot("""\
                             value
2025-10-01 00:00:00+02:00  44000.0
2025-10-01 00:15:00+02:00  43800.0
2025-10-01 00:30:00+02:00  42200.0
2025-10-01 00:45:00+02:00      NaN
2025-10-01 01:00:00+02:00  40300.0\
""")


def test_get_short_term_ID_consumption(monkeypatch):
    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr("requests.request", fake_request("tests/ID.json"))

    client = RTEClient("id", "secret")
    consumptions = client.get_short_term_consumptions(
        types=PrevisionType.ID, end=pd.Timestamp("2025-10-01T01:37:00+02:00")
    )
    assert str(consumptions[PrevisionType.ID]) == snapshot("""\
                             value
2025-10-01 00:00:00+02:00  44000.0
2025-10-01 00:15:00+02:00  43800.0
2025-10-01 00:30:00+02:00  42200.0
2025-10-01 00:45:00+02:00      NaN
2025-10-01 01:00:00+02:00  40300.0
2025-10-01 01:15:00+02:00      NaN
2025-10-01 01:30:00+02:00      NaN
2025-10-01 01:45:00+02:00      NaN\
""")


def test_get_realised_consumption(monkeypatch):
    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr("requests.request", fake_request("tests/consumption.json"))

    client = RTEClient("id", "secret")
    ts = client.get_realised_consumption(
        start=pd.Timestamp("2025-10-01T00:00:00+02:00"),
        end=pd.Timestamp("2025-10-01T01:37:00+02:00"),
    )
    assert isinstance(ts, pd.Series)
    assert str(ts) == snapshot("""\
2025-10-01 00:00:00+02:00    44345.0
2025-10-01 00:15:00+02:00    44174.0
2025-10-01 00:30:00+02:00    43040.0
2025-10-01 00:45:00+02:00    41806.0
2025-10-01 01:00:00+02:00        NaN
2025-10-01 01:15:00+02:00    41060.0
2025-10-01 01:30:00+02:00        NaN
2025-10-01 01:45:00+02:00        NaN
Freq: 15min, Name: value, dtype: float64\
""")
