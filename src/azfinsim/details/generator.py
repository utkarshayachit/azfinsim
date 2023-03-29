#! /usr/bin/env python3
#
# generator.py: Load the AzFinsim Cache with randomly generated trade data of specified length
#
import concurrent.futures
import logging
import math
import psutil
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
        "aggregation": "last_value"
    },
}

#-- pipeline / batching method 
def create_trade_range(start_trade, batch_size, end_trade, dbase):
    stop_trade = min(end_trade, start_trade + batch_size)
    log.info('{:10}: generating {}-{}'.format('BATCH', start_trade, stop_trade-1))
    df = GenerateTrade(start_trade, stop_trade - start_trade)
    log.info('{:10}: storing {}-{}'.format('BATCH', start_trade, stop_trade-1))
    dbase.set_trades(df)
    return True

def execute(args):
    # setup metrics
    metrics.define_measurements_and_views(_metrics_config)

    log.info('{0:10}: azfinsim.generator starting'.format('BEGIN'))
    #-- set threads to vcore count unless specified
    vcores = psutil.cpu_count(logical=True)
    pcores = psutil.cpu_count(logical=False)
    log.info('{0:10}: physical={1}, logical={2}'.format('CORES', pcores, vcores))

    #-- open connection to dbase (redis or filesystem)
    log.info("{:10}: connecting to {}".format('CACHE', args.cache_path if args.cache_type == "filesystem" else args.cache_name))
    dbase = connect(args, mode='w')
    log.info("{:10}: connected".format('CACHE'))

    # use concurrency, if specified
    threads = args.threads

    if args.cache_type=='filesystem' and args.start_trade != 0:
        log.critical("Cannot start at a trade other than 0 when writing to file")

    start_trade = args.start_trade
    batch_size = min(10000, max(1, math.ceil(args.trade_window/threads)))
    stop_trade = start_trade + args.trade_window

    log.info('{0:10}: threads={1}, batch_size={2}, start_trade={3}, stop_trade={4}'.format('CONFIG', threads, batch_size, start_trade, stop_trade))

    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(lambda x: create_trade_range(x, batch_size, stop_trade, dbase), range(start_trade, stop_trade, batch_size))
    end = time.perf_counter()
    timedelta=end-start

    log.info('{:10}: {:0.5}s'.format('TIME', timedelta))

    metrics.put("execution_time", timedelta)
    log.info('{:10}: azfinsim.generator complete'.format('END'))

    # flush metrics
    metrics.record()
