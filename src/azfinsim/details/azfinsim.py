#! /usr/bin/env python3

# This is the main execution engine that runs on the pool nodes

import time
import sys
import logging
import pandas as pd
import os.path
from numpy.random import random_sample

from . import utils, montecarlo
from . import metrics 
from .dbase import connect

# config for metrics
_metrics_config = {
    "execution_time": {
        "description": "process execution time",
        "unit": "s",
        "type": "float",
        "aggregation": "last_value"
    },
    "compute_time": {
        "description": "Time to calculate PV, Delta, and Vega or Synthetic computation",
        "unit": "s",
        "type": "float",
        "aggregation": "sum"
    },
    "failed": {
        "description": "Calculation failed",
        "unit": "count",
        "type": "int",
        "aggregation": "last_value"
    }
}

log = logging.getLogger(__name__)

def execute(args):
    # start time
    start_ts = time.perf_counter()

    # setup metrics
    metrics.define_measurements_and_views(_metrics_config)
    metrics.put("failed", 0)

    start_trade=args.start_trade

    #-- open connection to dbase
    log.info("CACHE %10s: CONNECT %s", '', args.cache_path if args.cache_type == "filesystem" else args.cache_name)
    if args.cache_type == "filesystem":
        dbase = connect(args, mode='r')

        # if cache_type is filesystem then we need a separate connection for output
        dirname, basename = os.path.split(args.cache_path)
        name, ext = os.path.splitext(basename)
        args.cache_path = os.path.join(dirname, f'{name}.results{ext}')
        log.info('CACHE %10s: RESULTS %s', '', args.cache_path)
        results_dbase = connect(args, mode='w')
    else:
        dbase = connect(args, mode='rw')
        results_dbase = dbase
    log.info('CACHE %10s: CONNECTED', '')

    # use dbase trade count if trade_window is 0 (only valid for filesystem cache)
    trade_window = args.trade_window if args.trade_window != 0 else dbase.get_trade_count()
    stop_trade = start_trade + trade_window

    if (stop_trade - start_trade) <= 0:
        log.critical("No trades to process")
        sys.exit(1)

    results = pd.DataFrame()
    out_batch_size = 10000 # number of trade results to write in a single batch

    log.info("TRADE %10s: START=%d, COUNT=%d", '', start_trade, trade_window)

    for tradenum in range(start_trade, stop_trade):
        log.info("TRADE %10d: BEGIN" % args.start_trade)

	    #-- read trade from cache
        log.debug("Retrieving Trade: %d", tradenum)
        df = dbase.get_trade(tradenum)

        log.debug("READ: %s", df.to_string())
        log.info("TRADE %10d: READ", tradenum)

        #-- Inject Random Failure 
        if utils.InjectRandomFail(args.failure):
            metrics.put("failed", 1)
            metrics.record()
            sys.exit(1)

        start_compute_ts = time.perf_counter()
        row_s = { 'tradenum': tradenum }
        if args.algorithm == "synthetic":
	        #-- fake pricing computation - tunable duration - mainly for benchmarking schedulers
            if args.task_duration > 0:
                utils.DoFakeCompute(args.delay_start, args.task_duration, args.mem_usage)
            # generate fake results
            rows_s['random'] = [random_sample()]
        elif args.algorithm == "pvonly": 
            log.debug("TRADE %10d: Start PV" % tradenum)
            pv, pv_time = montecarlo.price_option(df.iloc[0].to_dict()) #- single row in dataframe TODO: save all & tab print
            row_s["pv"] = [pv]
            row_s["pv_time"] = [pv_time]
        elif args.algorithm == "deltavega":
            #--- Perform timedelta vega risk calculation
            log.debug("TRADE %10d: Start Delta Vega" % tradenum)
            row_s["delta"] = [montecarlo.risk('fx1', df.iloc[0].to_dict())]
            row_s["vega"] = [montecarlo.risk('sigma1', df.iloc[0].to_dict())]
        else:
            raise RuntimeError("Unknown algorithm: %s" % args.algorithm)
        end_compute_ts = time.perf_counter()
        compute_ts = end_compute_ts - start_compute_ts
        log.info("TRADE %10d: RESULT: %s", tradenum, str(row_s))
        log.info("TRADE %10d: COMPUTE : %.12f", tradenum, compute_ts)
        metrics.put("compute_time", compute_ts)

        #-- append row to results
        log.debug("Appending Trade: %d", tradenum)
        results = pd.concat([results, pd.DataFrame.from_dict(row_s)], ignore_index=True)

        #-- write result back to cache
        if (tradenum-start_trade) % out_batch_size == 0 or tradenum == stop_trade-1:
            results_dbase.set_trades(results)
            log.info("TRADE %10d: WRITE", tradenum)
            results = results.iloc[0:0] # clear results
    log.info("TRADE %10d: DONE", args.start_trade)

    #-- log finish time
    end_ts = time.perf_counter()
    log.info("ENDTIME %8s: %f", '', end_ts)
    
    timedelta = end_ts - start_ts
    log.info("TASKTIME %7s: %.12f", '', timedelta)
    metrics.put("execution_time", timedelta)

    # flush metrics
    metrics.record()
