import orjson
import numpy as np
import pandas as pd
import os
import pathlib
import pyarrow
import requests
from retry import retry
import time
import yaml

DIRECTORY = os.path.dirname(os.path.realpath(__file__))
COLUMNS = {
    "open time": np.uint64,
    "open": np.float32,
    "high": np.float32,
    "low": np.float32,
    "close": np.float32,
    "volume": np.float32,
    "close time": np.uint64,
    "quote asset volume": np.float32,
    "number of trades": np.uint32,
    "taker buy base asset volume": np.float32,
    "taker buy quote asset volume": np.float32,
}

PERIOD_LOOKUP = {"s": 1, "m": 1 * 60, "h": 60 * 60, "d": 1440 * 60, "w": 10080 * 60}


@retry(
    (
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
    ),
    tries=3,
    delay=0.1,
    backoff=2,
)
def request_get(url: str, throttler: list[float], config: dict):
    while len(throttler) >= config.get("limit_requests", 10):
        print("sleep", len(throttler))
        time.sleep(time.time() - throttler[0])
        throttler[:] = [
            i
            for i in throttler
            if i >= (time.time() - ti(config.get("limit_period", "1m")))
        ]
    data = orjson.loads(requests.get(url).text)
    throttler.append(time.time())
    return data


def pairs_get(quote: str, throttler: list[float] = [], config: dict = {}):
    return [
        i["symbol"]
        for i in request_get(
            url="https://api.binance.com/api/v3/exchangeInfo",
            throttler=throttler,
            config=config,
        )["symbols"]
        if True
        and i["quoteAsset"] == quote
        and (bool(i["isSpotTradingAllowed"]) if config.get("spot_only") else True)
        and ((i["status"] == "TRADING") if config.get("tradable_only") else True)
    ]


@retry(
    (
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
    ),
    tries=3,
    delay=0.1,
    backoff=2,
)
def klines_get(
    pair: str,
    throttler: list[float] = [],
    config: dict = {},
    end_time: int = None,
    limit: int = 1000,
):
    url = (
        f"https://api.binance.com/api/v3/klines?symbol={pair}&interval={config.get('interval')}&limit={limit}"
        + (f"&endTime={end_time}" if end_time else "")
    )
    output = pd.DataFrame(
        request_get(url=url, throttler=throttler, config=config)
    ).iloc[:, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]
    output.columns = list(COLUMNS.keys())
    output = output.astype(COLUMNS)
    output["open time"] = (output["open time"].values / 1000).astype(np.uint32)
    output["close time"] = (output["close time"].values / 1000).astype(np.uint32)
    output["pair"] = pair
    output["pair"] = output["pair"].astype("category")
    return output


def klines_history_chunk_get(**kwargs):
    if "ts" in kwargs:
        output = klines_get(
            pair=kwargs["pair"],
            throttler=kwargs.get("throttler", []),
            config=kwargs.get("config", {}),
            end_time=int(kwargs["ts"] * 1000),
            limit=1000,
        )
    else:
        output = klines_get(
            pair=kwargs["pair"],
            throttler=kwargs.get("throttler", []),
            config=kwargs.get("config", {}),
            limit=1000,
        )
    print(
        "{} {} chunk, nrows {}".format(
            kwargs["pair"],
            kwargs.get("config", {}).get("interval"),
            output.shape[0],
        )
    )
    return output, output["open time"].iloc[0]


def klines_history_get(**kwargs):
    temp_chunk, kwargs["ts"] = klines_history_chunk_get(**kwargs)
    output = temp_chunk.copy()
    while (temp_chunk.shape[0] > 1) and (
        kwargs["ts_from"] not in temp_chunk["open time"].tolist()
    ):
        temp_chunk, kwargs["ts"] = klines_history_chunk_get(**kwargs)
        output = pd.concat([temp_chunk, output])
    return (
        output.drop_duplicates(subset="open time", keep="first")
        .sort_values(["open time"])
        .astype(COLUMNS)
    )


def pair_history_get(pair: str, throttler: list[float] = [], config: dict = {}):
    dirpath = f"{DIRECTORY}/../data/klines/{config.get('interval')}/"
    pathlib.Path(dirpath).mkdir(parents=True, exist_ok=True)
    path_output = f"{dirpath}/{pair}.feather"
    print(f"output filepath - {path_output}")
    try:
        output = pd.read_feather(path_output)
        temp_chunk = klines_history_get(
            pair=pair,
            ts_from=output["open time"].max(),
            throttler=throttler,
            config=config,
        )
        output = pd.concat([output, temp_chunk])
        output = output.drop_duplicates(subset=["open time"], keep="last")
        output = output.sort_values(["open time"])
        del temp_chunk
    except (FileNotFoundError, KeyError, pyarrow.lib.ArrowIOError):
        print(f"No previous data for {pair} found")
        output = klines_history_get(
            pair=pair, ts_from="", throttler=throttler, config=config
        )
    output.reset_index(drop=True).to_feather(path_output)
    print(f"{pair} {config.get('interval')} - final shape {output.shape}")


def ti(interval):
    return int(interval[:-1]) * PERIOD_LOOKUP[interval[-1]]


def yaml_read(filepath):
    return yaml.safe_load(pathlib.Path(filepath).read_bytes())


if __name__ == "__main__":
    print(f"klines downloader started")

    config = yaml_read(f"{DIRECTORY}/../config.yaml")
    print(f"config is: {config}")
    throttler = []

    time_start = time.time()
    for quote in config.get("quotes", []):
        pairs = pairs_get(quote=quote, throttler=throttler, config=config)
        print(f"number of pairs for quote {quote}: {len(pairs)}")
        for index, pair in enumerate(pairs):
            print(f"{index + 1} / {len(pairs)} - {pair}")
            pair_history_get(pair=pair, throttler=throttler, config=config)
    print(
        f"klines downloaded for the interval {config.get('interval')} in"
        f" {int(time.time() - time_start)} sec\n"
    )

    print("klines downloader finished")
