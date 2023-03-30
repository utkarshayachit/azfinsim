#! /usr/bin/env python3
#
# generator.py: Load the AzFinsim Cache with randomly generated trade data of specified length
#
import concurrent.futures
import logging
import time

from .utils import GenerateTrade
from .dbase import connect
from . import metrics

log = logging.getLogger(__name__)

# config for metrics
_metrics_config = {
    "execution_time": {
        "description": "process execution time",
        "unit": "s",
        "type": "float",
        "aggregation": "last_value",
    },
}


# -- pipeline / batching method
def create_trade_range(start_trade, batch_size, end_trade, dbase):
    stop_trade = min(end_trade, start_trade + batch_size)
    log.info("{:10}: trades {}-{}".format("GENERATE", start_trade, stop_trade - 1))
    df = GenerateTrade(start_trade, stop_trade - start_trade)
    log.info("{:10}: trades {}-{}".format("SAVE", start_trade, stop_trade - 1))
    dbase.set_trades(df)
    return True


def check_args(args):
    if (
        args.cache_type is None
        and args.cache_path is not None
        and args.cache_name is None
    ):
        # adjust default cache type
        log.info("{:10}: --cache-type=filesystem".format("AUTO_ARG"))
        args.cache_type = "filesystem"
    if (
        args.cache_type is None
        and args.cache_path is None
        and args.cache_name is not None
    ):
        # adjust default cache type
        log.info("{:10}: --cache-type=redis".format("AUTO_ARG"))
        args.cache_type = "redis"

    if args.cache_type == "redis":
        if args.cache_name is None:
            raise ValueError("cache_name must be specified for redis cache")
        if args.cache_key is None:
            raise ValueError("cache_key must be specified for redis cache")
    if args.cache_type == "filesystem":
        if args.cache_path is None:
            raise ValueError("cache_path must be specified for filesystem cache")

    if args.start_trade is None:
        args.start_trade = 0
        log.info("{:10}: --start-trade=0".format("AUTO_ARG"))
    if args.trade_window is None:
        args.trade_window = 100000  # 100,000 trades by default
        log.info("{:10}: --trade-window=100,000".format("AUTO_ARG"))


def execute(args):
    # validate and sanitize args
    check_args(args)

    log.info("{0:10}: generator starting".format("BEGIN"))

    # setup metrics
    metrics.define_measurements_and_views(_metrics_config)

    # -- open connection to dbase (redis or filesystem)
    log.info(
        "{:10}: connecting to {}".format(
            "CACHE",
            args.cache_path if args.cache_type == "filesystem" else args.cache_name,
        )
    )
    dbase = connect(args, mode="w")
    log.info("{:10}: connected".format("CACHE"))

    start_trade = args.start_trade
    batch_size = min(10000, args.trade_window)
    stop_trade = start_trade + args.trade_window

    threads = 1  # disable concurrency for now
    log.info(
        "{:10}: batch_size={}, start_trade={}, stop_trade={}".format(
            "CONFIG", batch_size, start_trade, stop_trade
        )
    )

    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(
            lambda x: create_trade_range(x, batch_size, stop_trade, dbase),
            range(start_trade, stop_trade, batch_size),
        )
    end = time.perf_counter()
    timedelta = end - start

    log.info("{:10}: {:0.5}s".format("EXEC_TIME", timedelta))

    metrics.put("execution_time", timedelta)
    log.info("{:10}: azfinsim.generator complete".format("END"))

    # flush metrics
    metrics.record()
