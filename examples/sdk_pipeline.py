"""Minimal DataFlow SDK example.

In production, keep these top-level callables in an importable package that is baked into
the immutable runtime image referenced by the pipeline.
"""

from dataflow.sdk import DataFlowClient, Resources, pipeline


def preprocess(batch):
    return batch


class Predictor:
    def __call__(self, batch):
        return batch


def keep_result(row):
    return True


@pipeline(
    name="image-inference",
    runtime_image="registry.example.com/dataflow-runtime:sha-abc123",
    cluster_profile="gpu-medium",
    artifact_base_uri="s3://dataflow-example/artifacts",
)
def image_pipeline(flow, input_path: str, output_path: str) -> None:
    dataset = flow.read_parquet(input_path, node_id="read")
    dataset = dataset.map_batches(
        preprocess,
        node_id="preprocess",
        resources=Resources(cpu=2, memory_bytes=4 * 1024**3),
        batch_size=128,
    )
    dataset = dataset.checkpoint().map_batches(
        Predictor,
        node_id="predict",
        resources=Resources(cpu=2, gpu=1),
    )
    dataset.filter(keep_result, node_id="filter").write_parquet(
        output_path,
        node_id="write",
    )


def main() -> None:
    spec = image_pipeline.spec(
        "s3://dataflow-example/input",
        "s3://dataflow-example/output",
    )
    with DataFlowClient("http://localhost:8080") as client:
        submission = client.submit(
            spec,
            parameters={"request_id": "sdk-example"},
            created_by="sdk-example",
        )
        print(submission.run["id"])


if __name__ == "__main__":
    main()
