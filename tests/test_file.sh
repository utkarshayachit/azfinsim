#!/usr/bin/env bash
set -e
# set -x

start_trade=9999
num_trades=20
num_files=4

mkdir -p $RESULTS_DIR

echo "populate with $num_trades trades"
python3 -m azfinsim.generator \
    --cache-path $RESULTS_DIR/trades.csv \
    -s $start_trade \
    -w $num_trades

echo "verify trades have been added"
keys=$(cat $RESULTS_DIR/trades.csv | wc -l)
keys=$((keys-1)) # remove header
if [ $keys -ne $num_trades ]; then
    echo "Expected $num_trades keys, found $keys"
    exit 1
fi

echo "split trades into $num_files files"
python3 -m azfinsim.split \
    --cache-path $RESULTS_DIR/trades.csv \
    -w $((num_trades/$num_files))

echo "process trades"
for i in $(seq 0 $((num_files-1))); do
    echo "process $RESULTS_DIR/trades.$i.csv"
    python3 -m azfinsim.azfinsim \
        --cache-path $RESULTS_DIR/trades.$i.csv \
        --algorithm pvonly

    echo "verify results were added"
    keys=$(cat $RESULTS_DIR/trades.$i.results.csv | wc -l)
    keys=$((keys-1)) # remove header
    if [ $keys -ne $((num_trades/num_files)) ]; then
        echo "Expected $((num_trades/num_files)) results keys, found $keys"
        exit 1
    fi
done

echo "merge results"
python3 -m azfinsim.concat \
    --cache-path "$RESULTS_DIR/trades.[0-9]*.results.csv" \
    --output-path $RESULTS_DIR/results.csv

echo "verify results were merged"
keys=$(cat $RESULTS_DIR/results.csv | wc -l)
keys=$((keys-1)) # remove header
if [ $keys -ne $num_trades ]; then
    echo "Expected $num_trades results keys, found $keys"
    exit 1
fi
