from __future__ import annotations

import pytest

from dataflow.admission import AdmissionPolicy


def test_admission_policy_parses_limits_from_environment() -> None:
    policy = AdmissionPolicy.from_env(
        {
            "DATAFLOW_ADMISSION_GLOBAL_LIMIT": "8",
            "DATAFLOW_ADMISSION_PROFILE_LIMITS": '{"cpu":4,"gpu":2}',
            "DATAFLOW_ADMISSION_MAX_NEW_PER_PASS": "3",
        }
    )

    assert policy.global_limit == 8
    assert policy.profile_limit("cpu") == 4
    assert policy.profile_limit("gpu") == 2
    assert policy.profile_limit("other") is None
    assert policy.max_new_per_pass == 3


def test_admission_policy_defaults_to_unlimited_capacity_but_bounded_new_work() -> None:
    policy = AdmissionPolicy.from_env({})

    assert policy.global_limit is None
    assert policy.profile_limits == {}
    assert policy.max_new_per_pass == 32


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({"DATAFLOW_ADMISSION_GLOBAL_LIMIT": "-1"}, "non-negative"),
        ({"DATAFLOW_ADMISSION_GLOBAL_LIMIT": "many"}, "integer"),
        ({"DATAFLOW_ADMISSION_PROFILE_LIMITS": "[]"}, "JSON object"),
        ({"DATAFLOW_ADMISSION_PROFILE_LIMITS": '{"cpu":-1}'}, "non-negative"),
        ({"DATAFLOW_ADMISSION_MAX_NEW_PER_PASS": "0"}, "positive"),
    ],
)
def test_admission_policy_rejects_invalid_environment(
    environment: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        AdmissionPolicy.from_env(environment)
