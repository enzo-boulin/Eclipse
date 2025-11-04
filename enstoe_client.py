import pandas as pd
from entsoe import EntsoePandasClient

from utils import format_ts

# from this date EntsoePandasClient sends 15 min load data
THRESHOLD = pd.Timestamp("2024-12-31 01:00:00+01:00")
COUNTRY_CODE = "FR"
ONE_HOUR = pd.Timedelta("1h")
FIFTEEN_MINUTES = pd.Timedelta("15min")


class EntsoeHourlyClient(EntsoePandasClient):
    """
    Subclass of EntsoePandasClient that ensures the returned time series
    is hourly by resampling 15-min data to hourly averages when needed.
    """

    threshold = THRESHOLD
    code = COUNTRY_CODE

    def get_hourly_load(self, start: pd.Timestamp, end: pd.Timestamp):
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("start and end timestamps must be timezone-aware")

        if end <= self.threshold:
            ts = super().query_load(self.code, start=start, end=end)
            ts = format_ts(ts, start=start, end=end, include_start=False)

        elif start >= self.threshold:
            ts = super().query_load(self.code, start=start, end=end)
            ts = format_ts(ts, start=start, end=end, include_start=False, freq="15min")
            ts = ts.resample("1h").mean()
        else:
            ts_before = super().query_load(self.code, start=start, end=self.threshold)
            ts_before = format_ts(
                ts_before, start=start, end=self.threshold, include_start=False
            )

            ts_after = super().query_load(self.code, start=self.threshold, end=end)
            ts_after = format_ts(
                ts_after,
                start=self.threshold,
                end=end,
                include_start=False,
                freq="15min",
            )
            ts_after = ts_after.resample("1h").mean()

            ts = pd.concat([ts_before, ts_after])
            ts = ts[~ts.index.duplicated(keep="last")]
            ts = format_ts(ts, start=start, end=end, include_start=False)

        return ts.rename(columns={"Actual Load": "load"})


if __name__ == "__main__":
    from config import ENTSOE_TOKEN

    client = EntsoeHourlyClient(api_key=ENTSOE_TOKEN)

    start = THRESHOLD - pd.DateOffset(hours=3)
    end = THRESHOLD + pd.DateOffset(hours=3)

    print("Fetching data from", start.tz_convert("UTC"), "to", end.tz_convert("UTC"))

    ts = (
        client.get_hourly_load(start=start, end=end)
        if start < end
        else client.get_hourly_load(start=end, end=start)
    )
    print(ts)
