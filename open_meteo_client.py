from typing import Dict

import pandas as pd
import requests

from config import CITIES_CFG, OPEN_METEO_BASE_URL, TZ


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
    ) -> pd.DataFrame:
        """Fetch hourly temperature for a single city."""
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")

        url = (
            f"{self.base_url}?latitude={lat}&longitude={lon}"
            f"&start_date={start_str}&end_date={end_str}"
            "&hourly=temperature_2m"
            f"&timezone={self.timezone}"
        )
        breakpoint()
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(
            {
                "datetime": pd.to_datetime(data["hourly"]["time"]),
                city_name: data["hourly"]["temperature_2m"],
            }
        )
        return df

    def get_averaged(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        Fetch hourly temperature for all configured cities
        and compute the population-weighted national average.

        Returns
        -------
        pd.DataFrame
            Columns: datetime, temp_<city>, temp_FR
        """
        temp_dfs: list[pd.DataFrame] = []
        for name, info in self.cities_cfg.items():
            df_city: pd.DataFrame = self.get_city(
                name, info["lat"], info["lon"], start, end
            )
            df_city.rename(columns={name: f"temp_{name}"}, inplace=True)
            temp_dfs.append(df_city)

        # Merge all cities on datetime
        df_all: pd.DataFrame = temp_dfs[0]
        for df_city in temp_dfs[1:]:
            df_all = pd.merge(df_all, df_city, on="datetime", how="inner")

        # Compute weighted mean
        total_weight: float = sum(city["weight"] for city in self.cities.values())
        df_all["temp_FR"] = (
            sum(
                df_all[f"temp_{city}"] * info["weight"]
                for city, info in self.cities.items()
            )
            / total_weight
        )

        return df_all


# -------------------------------------------------------
# Example usage
# -------------------------------------------------------
if __name__ == "__main__":
    client = OpenMeteoClient()
    end = pd.Timestamp.now(tz="Europe/Paris") - pd.DateOffset(days=1)
    start = end - pd.DateOffset(days=60)

    df_temp = client.get_averaged(start, end)
    print(df_temp.head())
    print(df_temp.columns)
