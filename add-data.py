#!/usr/bin/env python3
#
# This file appends parses the output of the PDS-H benchmark and outputs that
# data to a CSV if wanted. It also appends the file size of the wheel, and
# generates graphs for all of this.

import re
import sys
import os
import polars as pl
import subprocess

HASH_SIZE      =   7
NUM_QUERIES    =  22
PLOT_MAX_WIDTH =  50

BASE_DIR = '/home/polars/py-polars-cache'
STORE_FILE = f'{BASE_DIR}/data.csv'

if len(sys.argv) < 2:
    sys.stderr.write(f"Usage: {sys.argv[0]} <path/to/py-polars/polars>\n")
    exit(2)

POLARS_DIR = sys.argv[1].strip()

line_regex = re.compile(r"^Code block 'Run polars query (\d+)' took: (\d+\.\d+) s$")

# Create a dictionary object with all the query times from the benchmarks
on_streaming = False
query_times = {} 
streaming_query_times = {} 
for i in range(1, NUM_QUERIES+1):
    query_times[f'q{i}'] = []
    streaming_query_times[f'q{i}'] = []
for line in sys.stdin.readlines():
    line = line.strip()

    if 'Starting with streaming...' in line:
        on_streaming = True
        continue

    m = line_regex.match(line)

    if m is None:
        continue

    query_number = int(m.groups()[0])
    query_time = float(m.groups()[1])

    query_time = query_time

    if on_streaming:
        streaming_query_times[f'q{query_number}'].append(query_time)
    else:
        query_times[f'q{query_number}'].append(query_time)

# Get the median times for all the benchmark runs
query_times = pl.DataFrame(query_times).select(pl.all().median()).row(0)
streaming_query_times = pl.DataFrame(streaming_query_times).select(pl.all().median()).row(0)

# Append data of the current benchmark to the benchmarks file
os.system(f"strip {POLARS_DIR}/polars.abi3.so --strip-debug -o {POLARS_DIR}/polars.abi3.so.stripped")
file_size = os.path.getsize(f"{POLARS_DIR}/polars.abi3.so.stripped")
# Get the commit hash
commit_hash = subprocess.check_output("git rev-parse HEAD", shell=True).decode("utf-8").strip()
# Get the commit title
commit_message = subprocess.check_output("git show -s --format=%s", shell=True).decode("utf-8").strip().replace('"', "'")
# Get the Unix Timestamp of the commit
creation_time = subprocess.check_output("git show -s --format=%ct", shell=True).decode("utf-8").strip()
creation_time = int(creation_time)

line = f"{creation_time},{commit_hash},\"{commit_message}\",{file_size}"
for t in query_times:
    line += f",{t}"
for t in streaming_query_times:
    line += f",{t}"
with open(STORE_FILE, 'a') as f:
    f.write(line + '\n')