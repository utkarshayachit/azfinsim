import logging
import time
import glob
from natsort import natsorted

from . import metrics
from .dbase import connect

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

def check_args(args):
    args.cache_type = 'filesystem'
    if args.cache_path is None:
        raise ValueError('cache_path must be specified')
    if args.output_path is None:
        raise ValueError('output_path must be specified')

def execute(args):
    # validate and sanitize args
    check_args(args)

    log.info('{:10}: concat start'.format('BEGIN'))
    
    # setup metrics
    metrics.define_measurements_and_views(_metrics_config)

    inputs = natsorted(glob.glob(args.cache_path))
    # todo: sort inputs

    #-- open connection to dbase 
    args.cache_path = args.output_path
    log.info("{:10}: creating {}".format('OUT_CACHE', args.output_path))
    
    output = connect(args, mode='w')

    start_ts = time.perf_counter()
    for file in inputs:
        log.info('{:10}: reading {}'.format('IN_CACHE', file))
        args.cache_path = file
        dbase = connect(args, mode='r')
        output.set_trades(dbase.get_trades())
    end_ts = time.perf_counter()
    delta_ts = end_ts - start_ts
    metrics.put('execution_time', delta_ts)

    # record metrics
    metrics.record()
    log.info('{:10}: concat complete'.format('END'))
