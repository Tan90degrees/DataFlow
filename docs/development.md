# Development and verification

English | [简体中文](zh-CN/development.md)

## Repository

| Path | Purpose |
| --- | --- |
| `src/dataflow/contracts.py` | Operator kinds and per-worker resources |
| `src/dataflow/graph.py` | Pipeline/edge contracts, reference checks and topology |
| `src/dataflow/sdk/` | Python authoring DSL |
| `src/dataflow/runtime.py` | Ray Data operator/resource adapter |
| `src/dataflow/single_cluster.py` | Validation, DAG execution and connection ownership |
| `src/dataflow/cli.py` | The single `dataflow-run` command |
| `examples/` | SDK, JSON and branching examples |
| `tests/` | Unit, documentation, packaging and opt-in real Ray checks |
| `docs/` | Current English and Simplified Chinese documentation |

## Local checks

```bash
pip install -e '.[dev]'
ruff check .
ruff format --check .
pytest
python -m build
```

Unit tests need no running Ray cluster and use a lazy backend to check branch evaluation counts,
resource forwarding, validation, errors and session ownership. SDK imports/spec hashing also run
with runtime/service dependencies deliberately unavailable. The real Ray test is skipped unless
explicitly enabled; this is not evidence that real execution passed.

```bash
pip install -e '.[dev,runtime]'
DATAFLOW_TEST_RAY=1 pytest tests/test_single_cluster_ray.py -v -s -o faulthandler_timeout=90
```

The integration test creates one head and one worker as test infrastructure, executes repeated
pipelines in that cluster, verifies final records, and checks application-owned session reuse.
It requires process inspection, local sockets and sufficient process/shared-memory resources.
Restricted execution environments may not satisfy those requirements. CI limits runtime and
prints/preserves Ray logs on failure instead of leaving silent hangs.

CI checks Python 3.11/3.12 unit tests, lint/format, source and wheel builds, an installed-wheel
smoke test without Ray, and a separate Python 3.11 head/worker run. It uploads distributions as
workflow artifacts. There is no cluster-provisioning or vendor-image release matrix.

## Packaging and runtime image

```bash
python -m build
# Ray is already present in the base image:
docker build -t dataflow-runtime:0.2.0 .
```

The base package requires only Pydantic. `runtime` adds Ray Data; `dev` adds build/test tools.
The Dockerfile adds no control service and starts no cluster automatically. The platform chooses
the head/worker commands and can extend the image with business modules.

This branch introduces a breaking 0.2 API cleanup; creating build artifacts does not publish a
release. Review and tag/release are explicit repository operations. Do not describe load,
failover, memory or fairness guarantees as proven without the corresponding workload results.

Every Markdown document under `docs/` has a matching `docs/zh-CN/` page and language-switch link.
Documentation tests validate pairing and local links. Keep runnable commands and schema names
consistent across both languages.


## Concurrent DAG verification

`src/dataflow/runner.py` owns bounded in-process multi-DAG admission and the shared action pool.
`tests/test_dag_concurrency.py` proves overlap, dependency ordering, backpressure/cancellation,
failure draining, merge ports and immutable submission snapshots with deterministic events.
The real Ray module shares one head/worker fixture for session reuse, union/join semantics and
bounded-runner execution. It also emits the benchmark described in [performance](performance.md).
Production correctness/throughput/memory claims require representative workload evidence.
