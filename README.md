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

Install the systemd service files:

```bash
# Add AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_URI
vi /etc/systemd/system/upload-benchmarks.env

cp services/upload-benchmarks.service /etc/systemd/system
cp services/upload-benchmarks.path /etc/systemd/system

# Adjust paths in services
vi /etc/systemd/system/upload-benchmarks.service
vi /etc/systemd/system/upload-benchmarks.path

chmod 400 /etc/systemd/system/upload-benchmarks.*

# Start the services
systemctl enable upload-benchmarks.service
systemctl enable upload-benchmarks.path
systemctl start upload-benchmarks.service
systemctl start upload-benchmarks.path
```

Trigger daemon to upload the plots:

```bash
touch /path/to/polars-remote-benchmark/upload-probe
```
