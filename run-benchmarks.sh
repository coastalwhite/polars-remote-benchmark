#!/bin/sh

set -e

LOCAL_DIR="data/tables/scale-$SCALE_FACTOR"
DATA_DIR="$HOME/py-polars-cache/pds-h-data"
START=1
NUM_RUNS=5

if [ ! -d "$DATA_DIR" ]; then
    echo "No cached PDS-H data found. Generating data first..."

    make run-polars-no-env
    mkdir -p "$DATA_DIR"
    mv "$LOCAL_DIR" "$DATA_DIR"
    START=2
fi

rm -rf "$LOCAL_DIR" 2> /dev/null || true
mkdir -p "data/tables"
ln -s "$DATA_DIR/scale-$SCALE_FACTOR" "$LOCAL_DIR"

echo "Running benchmarks $NUM_RUNS times"
for i in $(seq $START $NUM_RUNS)
do
    echo "Run $i of $NUM_RUNS"
    echo
    python3 -m queries.polars
    echo
done

echo "Starting with streaming..."
for i in $(seq $START $NUM_RUNS)
do
    echo "Run $i of $NUM_RUNS"
    echo
    for j in $(seq 1 22)
    do
	# Query 8 and 9 currently OOM
        if [ "$j" -eq "8" ]; then
	    echo "Code block 'Run polars query $j' took: 0.0 s"
	else
	    if [ "$j" -eq "9" ]; then
	        echo "Code block 'Run polars query $j' took: 0.0 s"
	    else
	        RUN_POLARS_NEW_STREAMING=1 python3 -m "queries.polars.q$j"
	    fi
	fi
    done
    echo
done

