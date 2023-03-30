import logging
import os.path
import time

from . import metrics
from .dbase import connect

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


def check_args(args):
    args.cache_type = "filesystem"
    if args.cache_path is None:
        raise ValueError("cache_path must be specified")
    if args.output_path is None:
        args.output_path = os.path.dirname(args.cache_path)
        log.info("{:10}: --output-path={}".format("AUTO_ARG", args.output_path))
    if args.trade_window is None or args.trade_window < 1:
        raise ValueError("trade_window must be specified")


def execute(args):
    # validate and sanitize args
    check_args(args)

    log.info("{:10}: split start".format("BEGIN"))

    # setup metrics
    metrics.define_measurements_and_views(_metrics_config)

    # -- open connection to dbase
    log.info("{:10}: connecting to {}".format("IN_CACHE", args.cache_path))
    dbase = connect(args, mode="r")

    start_trade = int(dbase.get_first_trade()["tradenum"])
    end_trade = start_trade + dbase.get_trade_count()
    log.info("{:10}: all trades {}-{}".format("TRADES", start_trade, end_trade - 1))

    dir, file = os.path.split(args.cache_path)
    name, ext = os.path.splitext(file)

    trades = dbase.get_trades()

    start_ts = time.perf_counter()
    # -- split trades into batches
    for index, offset in enumerate(range(0, len(trades), args.trade_window)):
        args.cache_path = os.path.join(dir, "{}.{}{}".format(name, index, ext))
        log.info("{:10}: creating {}".format("OUT_CACHE", args.cache_path))
        output = connect(args, mode="w")

        df = trades.iloc[offset:offset + args.trade_window]
        log.info(
            "{:10}: trades {}-{} (count={})".format(
                "SAVE", df.iloc[0]["tradenum"], df.iloc[-1]["tradenum"], len(df)
            )
        )
        output.set_trades(df)
    end_ts = time.perf_counter()
    delta_ts = end_ts - start_ts
    metrics.put("execution_time", delta_ts)

    # record metrics
    metrics.record()
    log.info("{:10}: split complete".format("END"))
