#!/usr/bin/env python3
#

import re
import sys
import os
import math
import altair as alt
from time import time
import polars as pl
import subprocess

HASH_SIZE      =   7
NUM_QUERIES    =  22
PLOT_MAX_WIDTH =  50

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

STORE_FILE = f'{BASE_DIR}/data.csv'
OUT_DIR    = f'{BASE_DIR}/output'

GRAPH_OUTPUT_FORMAT = 'html'

# Create the schema
schema = {}
schema['creation_time'] = pl.UInt64
schema['commit_hash'] = pl.String
schema['commit_message'] = pl.String
schema['file_size'] = pl.UInt64
for q in range(1, NUM_QUERIES+1):
    schema[f'q{q}'] = pl.Float64
for q in range(1, NUM_QUERIES+1):
    schema[f'sq{q}'] = pl.Float64
schema = pl.Schema(schema)

ENGINES = ['In-Memory', 'Streaming']
PREFIX_DICT = {
    'In-Memory': '',
    'Streaming': 's',
}

# Load the data from the benchmarks file
df = (
    pl.scan_csv(STORE_FILE, schema=schema)
        .sort(pl.col.creation_time)

        # Shorten the commit hashes
        .with_columns(commit_hash = pl.col.commit_hash.str.head(HASH_SIZE))

        .with_columns([
            pl.when(
                pl.col(f'{PREFIX_DICT[engine]}q{q}') > 0.00001
            ).then(pl.col(f'{PREFIX_DICT[engine]}q{q}'))
            for q in range(1, NUM_QUERIES+1)
            for engine in ENGINES
        ])

        .tail(PLOT_MAX_WIDTH)
        .collect()
)


# Make the commit hashes a monospace font
def monospace_axisx():
    return {
        "config" : {
             "axisX": {
                  "labelFont": 'monospace',
             },
        }
    }
alt.themes.register('monospace-axisx', monospace_axisx)
alt.themes.enable('monospace-axisx')

# Create one chart for all the queries
selector = pl.selectors.starts_with(f'mean_q')
all_chart = alt.Chart(
    pl.concat(
        df
            .select([
                pl.col('commit_hash'),
                pl.col('commit_message'),
            ] + [
                pl.when(
                    pl.col(f'{PREFIX_DICT[engine]}q{q}') > 0.0001
                ).then(
                    (pl.col(f'{PREFIX_DICT[engine]}q{q}') / pl.col(f'{PREFIX_DICT[engine]}q{q}').mean())
                ).alias(f'mean_q{q}')
                for q in range(1, NUM_QUERIES+1)
            ] + [
                pl.lit(engine).alias('engine'),
            ])
        for engine in ENGINES
    ).with_columns(
        norm_time = pl.sum_horizontal(selector) / pl.sum_horizontal(
            pl.when(selector.is_not_null()).then(pl.lit(1))
        ),
    ).with_columns(
        norm_time = pl.when(pl.col.norm_time > 0.0001).then(pl.col.norm_time),
    )
).encode(
     x=alt.X('commit_hash:N', sort=None).title('Commit hash'),
     y=alt.Y(f'norm_time:Q').title('Normalized query runtime'),
     color=alt.Color('engine:N').title('Engine'),
     tooltip='commit_message',
)
all_chart = all_chart.mark_point() + all_chart.mark_line()
all_chart.properties(
    title="Normalized runtimes for PDS-H queries over time",
).save(
    f'{OUT_DIR}/queries.{GRAPH_OUTPUT_FORMAT}',
    format=GRAPH_OUTPUT_FORMAT,
)


# Create a chart for each individual query
for q in range(1, NUM_QUERIES+1):
    c = (
        alt
            .Chart(pl.concat(
                df.select((
                    pl.col('commit_hash'),
                    pl.col('commit_message'),
                    pl.col(f'{PREFIX_DICT[engine]}q{q}').alias(f'q{q}'),
                    pl.lit(engine).alias('engine')
                )) for engine in ENGINES
            ))
            .encode(
                x=alt.X('commit_hash:N', sort=None).title('Commit hash'),
                y=alt.Y(f'q{q}:Q').title('Query runtime (s)'),
                color=alt.Color('engine:N').title('Engine'),
                tooltip='commit_message',
            )
    )
    c = c.mark_line() + c.mark_point()
        
    c.properties(
        title=f"Runtime for PDS-H Query {q} over time",
    ).save(
        f'{OUT_DIR}/queries/{q}.{GRAPH_OUTPUT_FORMAT}',
        format=GRAPH_OUTPUT_FORMAT,
    )


# Create a chart for the file size over time
BYTES_IN_MB = 2**20
filesize_chart = (
    alt
        .Chart(df.select('commit_hash', 'commit_message', 'file_size'))
        .encode(
            x=alt.X('commit_hash:N', sort=None).title('Commit hash'),
            y=alt.Y(
                f'file_size:Q', axis=alt.Axis(
                    # Change the labels to be MBs
                    labelExpr=f"round(datum.value / {BYTES_IN_MB}) + 'MB'",
                ),
            ).title('Binary size'),
            tooltip='commit_message',
        )
)
filesize_chart = filesize_chart.mark_line() + filesize_chart.mark_point()
filesize_chart.properties(
    title="File size of the wheel with minimal debuginfo over time",
).save(
    f'{OUT_DIR}/file_size.{GRAPH_OUTPUT_FORMAT}',
    format=GRAPH_OUTPUT_FORMAT,
)