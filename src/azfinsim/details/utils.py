import datetime as dt
import logging
import numpy as np
import pandas as pd
import random

log = logging.getLogger(__name__)


def InjectRandomFail(failure):
    if random.uniform(0.0, 1.0) < failure:
        log.error("RANDOM ERROR INJECTION: TASK EXIT WITH ERROR")
        return True
    return False


def DoFakeCompute(delay_time, task_duration, mem_usage):
    import time

    # allocate the memory
    array_size = (mem_usage, 131072)
    data = np.ones(array_size, dtype=np.float64)
    # do startup delay
    time.sleep(delay_time)

    # now do fake computation
    task_duration_s = task_duration / 1000.0  # - convert from ms to s
    end_time = time.time() + task_duration_s
    while time.time() < end_time:
        data *= 12345.67890
        data[:] = 1.0


def GenerateTrade(tradenum: int, N: int) -> pd.DataFrame:
    # just use the time now
    newFile = {}
    newFile["tradenum"] = range(tradenum, tradenum + N)
    newFile["fx1"] = np.random.rand(N) * 0.12 + 0.8285

    newFile["start_date"] = [dt.date(2017, 12, 29)] * N
    newFile["end_date"] = [dt.date(2018, 8, 28)] * N

    newFile["drift"] = np.random.rand(N) * 0.2 - 0.1
    newFile["maturity"] = [0.20] * N

    t_steps = np.busday_count(
        dt.date(2017, 12, 29), dt.date(2018, 8, 28)
    )  # number of working days between 29/12/2017 and 08/03/2018
    newFile["t_steps"] = [t_steps] * N
    # -- 10k vs 100k Monte Carlo Paths
    # newFile['trials'] = np.random.randint(10000,10000,N)
    # newFile['trials'] = np.random.randint(100000,100000,N)
    newFile["trials"] = np.repeat(10000, N)
    # newFile['trials'] = np.repeat(100000,N)

    newFile["ro"] = [
        0.000038413221829
    ] * N  # calibration value: 0.000038413221829   Vega01 value: 0.0000387714624899
    newFile["v"] = [0.00154807378604] * N
    newFile["sigma1"] = np.random.rand(N) * 0.03 - 0.015 + 0.0808844481978

    newFile["warrantsNo"] = np.random.randint(30000, 60000, N)
    newFile["notionalPerWarr"] = np.random.rand(N) * 100 + 950
    # newFile['strike'] = np.random.rand(N)*0.2 + 0.9
    newFile["strike"] = np.random.rand(N) * 0.12 + 0.7
    return pd.DataFrame.from_dict(newFile)
