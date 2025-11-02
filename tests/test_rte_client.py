import base64
import json

import pandas as pd
import requests

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


def test_basic_auth_header():
    c = RTEClient("myid", "mysecret", token_cache_file=None)
    hdr = c._basic_auth_header()
    assert hdr.startswith("Basic ")
    # verify base64 payload
    payload = hdr.split(" ", 1)[1]
    assert base64.b64decode(payload.encode()).decode() == "myid:mysecret"


def test_get_access_token_success(monkeypatch, tmp_path):
    called = {}

    def fake_post(url, headers, data, timeout):
        called["url"] = url
        return DummyResponse(
            200,
            body={
                "access_token": "tok-123",
                "token_type": "Bearer",
                "expires_in": 3600,
            },
        )

    monkeypatch.setattr("requests.post", fake_post)

    cache_file = tmp_path / "token_cache.json"
    c = RTEClient("id", "secret", token_cache_file=str(cache_file))
    token = c.get_access_token()
    assert token == "tok-123"
    # token saved to cache file
    data = json.loads(cache_file.read_text())
    assert data["access_token"] == "tok-123"


def test_get_france_power_exchanges_parsing(monkeypatch):
    # make token endpoint return a valid token
    def fake_post(url, headers, data, timeout):
        return DummyResponse(
            200,
            body={
                "access_token": "tok-abc",
                "token_type": "Bearer",
                "expires_in": 3600,
            },
        )

    # API response with one entry and one value row
    api_body = {
        "france_power_exchanges": [
            {
                "start_date": "2025-10-01T00:00:00+02:00",
                "end_date": "2025-10-01T00:15:00+02:00",
                "values": [
                    {
                        "start_date": "2025-10-01T00:00:00+02:00",
                        "end_date": "2025-10-01T00:15:00+02:00",
                        "value": "123",
                        "price": "45",
                    }
                ],
            }
        ]
    }

    def fake_request(method, url, headers, params, data, timeout):
        return DummyResponse(200, body=api_body)

    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr("requests.request", fake_request)

    c = RTEClient("id", "secret")
    df = c.get_france_power_exchanges()
    # DataFrame should have one row with numeric columns converted
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 1
    assert "value" in df.columns and "price" in df.columns
    assert pd.api.types.is_numeric_dtype(df["value"]) or df["value"].dtype == float


def test_get_short_term_consumption_empty(monkeypatch):
    # token
    monkeypatch.setattr(
        "requests.post",
        lambda url, headers, data, timeout: DummyResponse(
            200, body={"access_token": "t", "token_type": "Bearer", "expires_in": 3600}
        ),
    )

    # API returns empty short_term list
    monkeypatch.setattr(
        "requests.request",
        lambda method, url, headers, params, data, timeout: DummyResponse(
            200, body={"short_term": []}
        ),
    )

    c = RTEClient("id", "secret")
    df = c.get_short_term_consumption()
    assert isinstance(df, pd.DataFrame)
    assert df.empty
