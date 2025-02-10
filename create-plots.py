#!/usr/bin/env python3

import sys
import os, io
import seaborn as sns
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
from pathlib import Path
import polars as pl

HASH_SIZE      =   7
NUM_QUERIES    =  22
PLOT_MAX_WIDTH =  50

MARGIN         = 1.2
ALPHA          =  .8
MARKER_SIZE    =   6
MARKER_EDGE    = 'white'

BASE_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

STORE_FILE = BASE_DIR / 'data.csv'
OUT_DIR    = BASE_DIR / 'output'

MARKED_GID_PREFIX = 'marked-lineplot'

# Apply the default theme
sns.set_theme()

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


def save_with_tooltips(fig, path: Path, has_datapoint: list[list[bool]]):
    f = io.BytesIO()
    fig.savefig(f, format='svg')
    tree, xmlid = ET.XMLID(f.getvalue())

    # Find the SVG group with all the markers and add the commit messages in
    # there. It is quite hacky, but it works.
    # 
    # Inspired by: https://matplotlib.org/stable/gallery/user_interfaces/svg_tooltip_sgskip.html
    for i, ps in enumerate(has_datapoint):
        assert len(ps) == PLOT_MAX_WIDTH

        group = xmlid[f'{MARKED_GID_PREFIX}-{i}']

        marker_group = None
        for elem in group:
            if not elem.tag.endswith('}g'):
                continue
            if len(elem) != sum(has_datapoint[i]):
                continue
            if not all(e.tag.endswith('}use') for e in elem):
                continue
            marker_group = elem
            break
        if marker_group is None:
            print('Did not find marker group!', file=sys.stderr)
            exit(1)

        j = 0
        for e in marker_group:
            while not ps[j]:
                j += 1

            assert e.tag.endswith('use')
            t = ET.Element('title')
            t.text = df['commit_message'][j]
            e.append(t)

            j += 1

    ET.ElementTree(tree).write(path)
    plt.close()
    print(f'{path}... done')

# Make the commit hashes a monospace font
ET.register_namespace("", "http://www.w3.org/2000/svg")

def set_commit_hash_xaxis(ax):
    ax.set_xticks(df['commit_hash'])
    ax.tick_params(axis='x', rotation=90, labelsize=8, labelfontfamily='monospace')
    ax.set_xlabel("Commit Hash")
    ax.grid(axis="x") # Only horizontal stripes


selector = pl.selectors.starts_with(f'mean_q')
prod = pl.lit(1.0)
for q in range(1, NUM_QUERIES+1):
    prod = prod * pl.col(f'q{q}')
per_engine_data = [
    df
        .select([
            pl.col('commit_hash'),
            pl.col('commit_message'),
        ] + [
            pl.col(f'{PREFIX_DICT[engine]}q{q}').alias(f'q{q}')
            for q in range(1, NUM_QUERIES+1)
        ])
        .with_columns(
            pl.when(pl.col(f'q{q}') > 0.0001).then(f'q{q}').alias(f'q{q}')
            for q in range(1, NUM_QUERIES+1)
        ).with_columns(
            (pl.col(f'q{q}') / pl.col(f'q{q}').mean()).alias(f'mean_q{q}')
            for q in range(1, NUM_QUERIES+1)
        )
        .with_columns(
            geomean = prod.pow(1.0 / NUM_QUERIES).alias('geomean'),
            norm_time = pl.sum_horizontal(selector) / pl.sum_horizontal(
                pl.when(selector.is_not_null()).then(pl.lit(1))
            ),
        ).with_columns(
            norm_time = pl.when(pl.col.norm_time > 0.0001).then(pl.col.norm_time),
        )
    for engine in ENGINES
]

# Create one chart for all the queries
fig, ax = plt.subplots(figsize=(8, 4))
y_limit = max((per_engine_data[i].get_column("geomean").max() or 0.0) for i in range(len(ENGINES))) * MARGIN
for i, engine in enumerate(ENGINES):
    ax.plot(
        per_engine_data[i]['commit_hash'], per_engine_data[i]['geomean'],
        marker='o', linestyle='-',
        label=engine, gid=f'{MARKED_GID_PREFIX}-{i}',
        markersize=MARKER_SIZE, markeredgecolor=MARKER_EDGE,
        alpha=ALPHA,
    )

set_commit_hash_xaxis(ax)

ax.set_ylim(bottom = 0, top = y_limit)
ax.set_ylabel("Geometric Mean of Queries (s)")

legend = ax.legend()
legend.set_title('Engine')

ax.set_title('Geometric mean for PDS-H queries over time')
fig.tight_layout()

has_datapoint = [
    d['geomean'].is_not_null().to_list()
    for d in per_engine_data
]
save_with_tooltips(fig, OUT_DIR / 'queries-geomean.svg', has_datapoint)


# Create one chart for all the queries
selector = pl.selectors.starts_with(f'mean_q')
per_engine_data = [
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
            pl.col(f'{PREFIX_DICT[engine]}q{q}').alias(f'q{q}')
            for q in range(1, NUM_QUERIES+1)
        ])
        .with_columns(
            norm_time = pl.sum_horizontal(selector) / pl.sum_horizontal(
                pl.when(selector.is_not_null()).then(pl.lit(1))
            ),
        ).with_columns(
            norm_time = pl.when(pl.col.norm_time > 0.0001).then(pl.col.norm_time),
        )
    for engine in ENGINES
]
fig, ax = plt.subplots(figsize=(8, 4))
y_limit = max((per_engine_data[i].get_column("norm_time").max() or 0.0) for i in range(len(ENGINES))) * MARGIN
for i, engine in enumerate(ENGINES):
    ax.plot(
        per_engine_data[i]['commit_hash'], per_engine_data[i]['norm_time'],
        marker='o', linestyle='-',
        label=engine, gid=f'{MARKED_GID_PREFIX}-{i}',
        markersize=MARKER_SIZE, markeredgecolor=MARKER_EDGE,
        alpha=ALPHA,
    )

set_commit_hash_xaxis(ax)

ax.set_ylim(bottom = 0, top = y_limit)
ax.set_ylabel("Normalized Query Runtime")

legend = ax.legend()
legend.set_title('Engine')

ax.set_title('Normalized runtimes for PDS-H queries over time')
fig.tight_layout()

has_datapoint = [
    d['norm_time'].is_not_null().to_list()
    for d in per_engine_data
]
save_with_tooltips(fig, OUT_DIR / 'queries.svg', has_datapoint)

# Create a chart for each individual query
for q in range(1, NUM_QUERIES+1):
    fig, ax = plt.subplots(figsize=(8, 4))
    y_limit = max((per_engine_data[i][f'q{q}'].max() or 0.0) for i in range(len(ENGINES))) * MARGIN
    for i, engine in enumerate(ENGINES):
        ax.plot(
            per_engine_data[i]['commit_hash'], per_engine_data[i][f'q{q}'],
            marker='o', linestyle='-',
            label=engine, gid=f'{MARKED_GID_PREFIX}-{i}',
            markersize=MARKER_SIZE, markeredgecolor=MARKER_EDGE,
            alpha=ALPHA,
        )

    set_commit_hash_xaxis(ax)

    ax.set_ylim(bottom = 0, top = y_limit)
    ax.set_ylabel('Query runtime (s)')

    legend = ax.legend()
    legend.set_title('Engine')

    ax.set_title(f"Runtime for PDS-H Query {q} over time")
    fig.tight_layout()

    has_datapoint = [
        d[f'q{q}'].is_not_null().to_list()
        for d in per_engine_data
    ]
    save_with_tooltips(fig, OUT_DIR / 'queries' / f'{q}.svg', has_datapoint)

# Create a chart for the file size over time
BYTES_IN_MB = 2**20
fig, ax = plt.subplots(figsize=(8, 4))
y_limit = ((df['file_size'].max() / BYTES_IN_MB) or 0.0) * MARGIN
ax.plot(
    df['commit_hash'], df['file_size'] / BYTES_IN_MB,
    marker='o', linestyle='-',
    label='File Size', gid=f'{MARKED_GID_PREFIX}-0',
    markersize=MARKER_SIZE, markeredgecolor=MARKER_EDGE,
    alpha=ALPHA,
)

set_commit_hash_xaxis(ax)

ax.set_ylim(bottom = 0, top = y_limit)
ax.set_ylabel('Binary size (MB)')

ax.set_title(f"File size of the wheel with minimal debuginfo over time")
fig.tight_layout()

has_datapoint = [ df['file_size'].is_not_null().to_list() ]
save_with_tooltips(fig, OUT_DIR / 'file_size.svg', has_datapoint)