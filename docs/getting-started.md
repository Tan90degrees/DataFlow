# Getting started

English | [简体中文](zh-CN/getting-started.md)

## Install

Use Python 3.11 or 3.12 and the same Ray version on driver and workers. From the repository:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[runtime]'
```

The base package installs only Pydantic. The `runtime` extra pins `ray[data]==2.58.0`.
No database, HTTP API, object-storage service or controller is required.
Other Python versions depend on availability of matching Ray wheels and are not covered by CI.

## Connect to one cluster

For a one-machine development demonstration, start the shared cluster once in this environment:

```bash
ray start --head --port=6379 --num-cpus=2 --include-dashboard=false
```

DataFlow itself never starts Ray. Production uses the cluster already provisioned by your platform.
Additional worker nodes join that same head; there is no second cluster for downstream stages.

Create a small input on the same machine:

```bash
mkdir -p /tmp/dataflow-demo
python - <<'CODE'
import pyarrow as pa
import pyarrow.parquet as pq
pq.write_table(pa.table({"value": [1, 2, 3, 4]}), "/tmp/dataflow-demo/input.parquet")
CODE
python examples/single_cluster.py /tmp/dataflow-demo/input.parquet /tmp/dataflow-demo/output --address auto
```

The command prints a dictionary from sink node ID to output path. Verify the result:

```bash
python -c 'import pyarrow.parquet as pq; print(pq.read_table("/tmp/dataflow-demo/output"))'
```

Choose a new output directory on subsequent runs: Ray's Parquet writer appends by default.
For multiple physical nodes, input and final output paths must be visible at the same mount path
on workers, for example through NFS. Intermediate data moves through Ray, without shared files.

## JSON entrypoint

Edit the business input and final output paths in `examples/single_cluster.json`, then run:

```bash
dataflow-run examples/single_cluster.json --address auto
# Equivalent module entrypoint:
python -m dataflow.cli examples/single_cluster.json --address auto
```

`--address` accepts an existing Ray connection address. Without it, DataFlow honors `RAY_ADDRESS`
and otherwise uses `auto`. It rejects `local` and never falls back to starting a cluster.
See [connection ownership](operations.md) when embedding in an application.

When the development cluster is no longer needed, its owner can run `ray stop` in that environment.
Disconnecting a DataFlow run does not stop the cluster.
