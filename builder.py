import numpy as np
import pandas as pd

from config import ENTSOE_TOKEN
from enstoe_client import EntsoeHourlyClient
from open_meteo_client import OpenMeteoClient


def index_to_time_features(index: pd.DatetimeIndex) -> pd.DataFrame:
    day = index.day
    month = index.month
    day_of_week = index.dayofweek
    hour = index.hour
    features = pd.DataFrame(
        {
            "day_sin": np.sin(2 * np.pi * day / 31),
            "day_cos": np.cos(2 * np.pi * day / 31),
            "month_sin": np.sin(2 * np.pi * month / 12),
            "month_cos": np.cos(2 * np.pi * month / 12),
            "dow_sin": np.sin(2 * np.pi * day_of_week / 7),
            "dow_cos": np.cos(2 * np.pi * day_of_week / 7),
            "is_weekend": (day_of_week >= 5).astype(int),
            "hour_sin": np.sin(2 * np.pi * hour / 24),
            "hour_cos": np.cos(2 * np.pi * hour / 24),
        },
        index=index,
    )
    return features


def build_dataset(
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """
    Build a dataset combining electricity prices from ENTSO-E and weather data from Open-Meteo.

    Parameters
    ----------
    entsoe_client : EntsoeHourlyClient
        Client to fetch electricity prices.
    open_meteo_client : OpenMeteoClient
        Client to fetch weather data.
    start : pd.Timestamp
        Start time (tz-aware).
    end : pd.Timestamp
        End time (tz-aware).
    Returns
    -------
    pd.DataFrame
        Combined dataset with electricity prices and weather data.
        Columns include time features, 'load' (MW), and 'temp' (°C).
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("Both `start` and `end` must be timezone-aware Timestamps.")

    entsoe = EntsoeHourlyClient(api_key=ENTSOE_TOKEN)

    load = entsoe.get_hourly_load(start, end)

    meteo = OpenMeteoClient()

    temp = meteo.get_averaged(start, end)

    idx = load.index.intersection(temp.index)
    df = index_to_time_features(idx)
    df["load"] = load.reindex(idx)
    df["temp"] = temp.reindex(idx)

    df.index = df.index.tz_convert("CET")
    return df


if __name__ == "__main__":
    start = pd.Timestamp("2020-01-01", tz="CET")
    end = pd.Timestamp("2025-11-05", tz="CET")
    print("Building dataset from", start, "to", end)
    df = build_dataset(start, end)
    print(df)
