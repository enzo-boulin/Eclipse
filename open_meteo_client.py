from typing import Dict

import pandas as pd
import requests

from config import CITIES_CFG, OPEN_METEO_BASE_URL, TZ
from utils import format_ts


class OpenMeteoClient:
    """
    Client for fetching and aggregating hourly weather data
    from the Open-Meteo Archive API (no API key required).
    """

    def __init__(
        self,
        cities_cfg: Dict[str, Dict[str, float]] = CITIES_CFG,
        base_url: str = OPEN_METEO_BASE_URL,
        timezone: str = TZ,
    ) -> None:
        """
        Parameters
        ----------
        cities_cfg : dict
            Dictionary of cities configuration, e.g.
            {
                "paris": {"lat": 48.8566, "lon": 2.3522, "weight": 0.18},
                "lyon": {"lat": 45.764, "lon": 4.8357, "weight": 0.10},
                ...
            }
        """
        self.cities_cfg = cities_cfg
        self.timezone = timezone
        self.base_url = base_url

    def get_city(
        self,
        city_name: str,
        lat: float,
        lon: float,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.Series:
        """Fetch hourly temperature for a single city as a time-indexed Series in CET."""
        # raise if start or end are not localized
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("start and end timestamps must be timezone-aware")
        start_str = start.tz_convert("UTC").strftime("%Y-%m-%d")
        end_str = end.tz_convert("UTC").strftime("%Y-%m-%d")

        url = (
            f"{self.base_url}?latitude={lat}&longitude={lon}"
            f"&start_date={start_str}&end_date={end_str}"
            "&hourly=temperature_2m"
            f"&timezone=UTC"
        )

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(
            {
                "datetime": pd.to_datetime(data["hourly"]["time"]),
                city_name: data["hourly"]["temperature_2m"],
            }
        )
        df = df.set_index("datetime")
        # Returns in the desired timezone (from the url)
        df.index = df.index.tz_localize("UTC")
        ts = format_ts(df.squeeze(), start=start, end=end, include_start=False)
        if ts.index[-1] == end.tz_convert("UTC").floor("h"):
            ts = ts[:-1]
        return ts

    def get_averaged(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
        """
        Fetch hourly temperature for all configured cities
        and compute the population-weighted national average.

        Returns
        -------
        pd.DataFrame
            Columns: datetime, temp_<city>, temp_FR
        """
        tss = []
        for name, info in self.cities_cfg.items():
            ts = info["weight"] * self.get_city(
                name, info["lat"], info["lon"], start, end
            )
            tss.append(ts)
        mean_ts = pd.concat(tss, axis=1).sum(axis=1, skipna=True)

        return mean_ts.rename("temp").round(2)


if __name__ == "__main__":
    client = OpenMeteoClient()
    start = pd.Timestamp("2025-08-04", tz="CET")
    end = start + pd.DateOffset(hours=2)
    print("Fetching data from", start, "to", end)
    ts = client.get_averaged(start, end)
    print(ts.tz_convert("CET"))
