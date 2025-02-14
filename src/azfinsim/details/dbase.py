r"""
encapsulates the cache / io operations
"""
import pandas as pd
import redis
import threading
import logging
import time
import io
from . import metrics

log = logging.getLogger(__name__)

_metrics_config = {
    "io_read_time": {
        "description": "Time for IO reads",
        "unit": "s",
        "type": "float",
        "aggregation": "sum",
    },
    "io_write_time": {
        "description": "Time for IO writes",
        "unit": "s",
        "type": "float",
        "aggregation": "sum",
    },
}

_metrics = None


class TradesCache:
    def __init__(self, mode: str):
        global _metrics
        if _metrics is None:
            metrics.define_measurements_and_views(_metrics_config)
            _metrics = True

        if mode not in ["r", "w", "rw"]:
            raise RuntimeError(f"Invalid mode: {mode}")
        self._mode = mode

    def get_trade(self, tradenum: int, column: str = "tradenum") -> pd.DataFrame:
        """returns a dataframe with the trade"""
        assert self._mode == "r"
        raise RuntimeError("Not implemented")

    def set_trades(self, trades: pd.DataFrame, column: str = "tradenum") -> None:
        assert self._mode == "w"
        assert isinstance(trades, pd.DataFrame)
        assert column in trades.columns
        raise RuntimeError("Not implemented")

    def get_trade_count(self) -> int:
        raise RuntimeError("Not implemented")


class TradesCacheRedis(TradesCache):
    """Redis implementation of TradesCache"""

    def __init__(self, redis_client: redis.Redis, mode: str, read_key='trade:{}', write_key='trade:{}', **kwargs):
        super().__init__(mode)
        self._redis_client = redis_client
        self._read_key = read_key
        self._write_key = write_key

        # validate connection to redis server
        self._redis_client.ping()

    def get_trade(self, tradenum: int, column: str = "tradenum") -> pd.DataFrame:
        """returns a dataframe with the trade"""
        assert self._mode == "r" or self._mode == "rw"
        assert isinstance(tradenum, int)
        assert isinstance(column, str)
        start = time.perf_counter()
        data = self._redis_client.get(self._read_key.format(tradenum))
        end = time.perf_counter()
        delta_ts = end - start
        metrics.put("io_read_time", delta_ts)
        if data is None:
            raise RuntimeError(f"No trade found for {tradenum}")
        # log.info('{}, data: {}'.format(tradenum, data))
        buffer = io.BytesIO(data)
        return pd.read_pickle(buffer).to_frame().T

    def get_trade_count(self) -> int:
        log.warning("get_trade_count not implemented for redis")
        return 0

    def set_trades(self, trades: pd.DataFrame, column: str = "tradenum") -> None:
        assert self._mode == "w" or self._mode == "rw"
        assert isinstance(trades, pd.DataFrame)
        assert column in trades.columns

        # convert individual trades to json and store in redis
        start = time.perf_counter()
        pipeline = self._redis_client.pipeline()
        for _, row in trades.iterrows():
            tradenum = row[column]
            # log.info('{}, row: {}'.format(tradenum, row.to_json()))
            buffer = io.BytesIO()
            row.to_pickle(buffer)
            pipeline.set(self._write_key.format(tradenum), buffer.getvalue())
        pipeline.execute(raise_on_error=True)
        end = time.perf_counter()
        delta_ts = end - start
        metrics.put("io_write_time", delta_ts)


class TradesCacheFile(TradesCache):
    """Filesystem implementation of TradesCache"""

    def __init__(self, fname: str, mode: str, **kwargs):
        super().__init__(mode)
        assert mode in ["r", "w"]  # we don't support "rw" for files yet
        self._fname = fname
        self._lock = threading.Lock()
        self._add_header = True
        self._trades = None

    def _read(self):
        assert self._mode == "r"
        with self._lock:
            if self._trades is None:
                start = time.perf_counter()
                self._trades = pd.read_csv(self._fname, index_col=False)
                end = time.perf_counter()
                delta_ts = end - start
                metrics.put("io_read_time", delta_ts)

    def get_trade(self, tradenum: int, column: str = "tradenum") -> pd.DataFrame:
        """returns a dataframe with the trade"""
        assert self._mode == "r"
        assert isinstance(tradenum, int)
        assert isinstance(column, str)
        self._read()
        return self._trades[self._trades[column] == tradenum]

    def get_trades(self) -> pd.DataFrame:
        assert self._mode == "r"
        self._read()
        return self._trades

    def get_trade_count(self) -> int:
        assert self._mode == "r"
        self._read()
        return len(self._trades)

    def get_first_trade(self) -> pd.DataFrame:
        assert self._mode == "r"
        self._read()
        return self._trades.iloc[0]

    def set_trades(self, trades: pd.DataFrame, column: str = "tradenum") -> None:
        assert self._mode == "w"
        assert isinstance(trades, pd.DataFrame)
        assert column in trades.columns

        with self._lock:
            # append trades to file
            start = time.perf_counter()
            trades.to_csv(
                self._fname,
                mode="w" if self._add_header else "a",
                header=self._add_header,
                index=False,
            )
            end = time.perf_counter()
            delta_ts = end - start
            metrics.put("io_write_time", delta_ts)
            self._add_header = False


def connect(args, mode: str, **kwargs) -> TradesCache:
    """connect to the cache"""
    if args.cache_type == "redis":
        if args.cache_ssl == "yes":
            return TradesCacheRedis(
                redis.Redis(
                    host=args.cache_name,
                    port=args.cache_port,
                    password=args.cache_key,
                    ssl_cert_reqs="none",  # -- or specify location of certs
                    ssl=True,
                ),
                mode,
                **kwargs
            )
        else:
            return TradesCacheRedis(
                redis.Redis(
                    host=args.cache_name, port=args.cache_port, password=args.cache_key
                ),
                mode,
                **kwargs
            )
    elif args.cache_type == "filesystem":
        return TradesCacheFile(args.cache_path, mode)
    else:
        raise RuntimeError(f"Invalid cache type: {args.cache_type}")
