# Polars Remote Benchmark

Pull in `data.csv`:

```bash
scp -r ...:/path/to/data.csv data.csv
```

Run the benchmarks in the `polars-benchmark` folder:

```bash
/path/to/run-benchmarks.sh | tee benchmark-result
```

Add data to the `data.csv`:

```bash
python3 data.csv /path/to/polars/py-polars/polars < benchmark-result
```

Create the plots:

```bash
python3 create-plots.py
```

Upload the plots:

```bash
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_URI=... ./upload-graphs.sh
```
