#!/usr/bin/env bash

set -e
# set -x

redis-cli -h $REDIS_HOST -p $REDIS_PORT flushdb

echo "populate with 23,000 trades"
start_trade=9899
num_trades=23000
python3 -m azfinsim.generator \
    --cache-name $REDIS_HOST --cache-port $REDIS_PORT --cache-ssl no \
    -s $start_trade \
    -w $num_trades

echo "verify trades have been added"
keys=$(redis-cli --raw -h $REDIS_HOST -p $REDIS_PORT keys "[0-9]*.bin" | wc -l)
if [ $keys -ne $num_trades ]; then
    echo "Expected $num_trades keys, found $keys"
    exit 1
fi

echo "process trades"
python3 -m azfinsim.azfinsim \
    --cache-name $REDIS_HOST --cache-port $REDIS_PORT --cache-ssl no \
    -s $((start_trade+99)) \
    -w 10

echo "verify results were added"
keys=$(redis-cli --raw -h $REDIS_HOST -p $REDIS_PORT keys "[0-9]*.results.bin" | wc -l)
if [ $keys -ne 10 ]; then
    echo "Expected 10 results keys, found $keys"
    exit 1
fi

echo "process trades using pvonly"
python3 -m azfinsim.azfinsim \
    --cache-name $REDIS_HOST --cache-port $REDIS_PORT --cache-ssl no \
    -s $((start_trade+99+5)) \
    -w 10 \
    --algorithm pvonly

echo "verify results were added"
keys=$(redis-cli --raw -h $REDIS_HOST -p $REDIS_PORT keys "[0-9]*.results.bin" | wc -l)
if [ $keys -ne 15 ]; then
    echo "Expected 15 results keys, found $keys"
    exit 1
fi

echo "done"
