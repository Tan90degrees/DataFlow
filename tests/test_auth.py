from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dataflow.auth import (
    ROLE_CLUSTER_PROFILE_ADMIN,
    ROLE_PIPELINE_READ,
    ROLE_PIPELINE_SUBMIT,
    ROLE_RUN_CANCEL,
    ApiAuthSettings,
    BearerAuthenticator,
    ClaimsIdentityAdapter,
    DataFlowAuthMiddleware,
    StaticBearerClaimsProvider,
    required_role_for_request,
)
from dataflow.sdk import DataFlowClient


def _authenticator() -> BearerAuthenticator:
    return BearerAuthenticator(
        StaticBearerClaimsProvider(
            {
                "reader-token": {"sub": "reader", "roles": [ROLE_PIPELINE_READ]},
                "writer-token": {
                    "sub": "writer",
                    "roles": [ROLE_PIPELINE_READ, ROLE_PIPELINE_SUBMIT],
                },
            }
        )
    )


def test_claims_adapter_accepts_oidc_shaped_subject_and_roles() -> None:
    identity = ClaimsIdentityAdapter().identity_from_claims(
        {"sub": "alice", "roles": "pipeline.read run.cancel", "iss": "https://issuer"}
    )
    assert identity.subject == "alice"
    assert identity.roles == frozenset({ROLE_PIPELINE_READ, ROLE_RUN_CANCEL})
    assert identity.claims["iss"] == "https://issuer"


def test_static_bearer_authenticator_rejects_missing_invalid_and_malformed_credentials() -> None:
    authenticator = _authenticator()
    assert authenticator.authenticate(None) is None
    assert authenticator.authenticate("Basic abc") is None
    assert authenticator.authenticate("Bearer unknown") is None
    assert authenticator.authenticate("Bearer reader-token") is not None


def test_auth_settings_parse_static_claims_and_custom_claim_names() -> None:
    settings = ApiAuthSettings.from_env(
        {
            "DATAFLOW_API_AUTH_MODE": "static",
            "DATAFLOW_API_AUTH_STATIC_TOKENS": (
                '{"t":{"subject":"svc","permissions":["pipeline.read"]}}'
            ),
            "DATAFLOW_API_AUTH_SUBJECT_CLAIM": "subject",
            "DATAFLOW_API_AUTH_ROLES_CLAIM": "permissions",
        }
    )
    identity = settings.authenticator().authenticate("Bearer t")  # type: ignore[union-attr]
    assert identity is not None
    assert identity.subject == "svc"
    assert identity.roles == frozenset({ROLE_PIPELINE_READ})


def test_auth_settings_fail_closed_when_static_mode_has_no_tokens() -> None:
    with pytest.raises(RuntimeError, match="STATIC_TOKENS"):
        ApiAuthSettings.from_env({"DATAFLOW_API_AUTH_MODE": "static"})


def test_role_policy_separates_read_submit_cancel_and_cluster_profile_admin() -> None:
    assert required_role_for_request("GET", "/healthz") is None
    assert required_role_for_request("GET", "/v1/pipelines") == ROLE_PIPELINE_READ
    assert required_role_for_request("POST", "/v1/pipelines") == ROLE_PIPELINE_SUBMIT
    assert (
        required_role_for_request("POST", "/v1/pipelines/abc/versions")
        == ROLE_PIPELINE_SUBMIT
    )
    assert required_role_for_request("POST", "/v1/pipeline-runs") == ROLE_PIPELINE_SUBMIT
    assert (
        required_role_for_request("POST", "/v1/pipeline-runs/abc/cancel")
        == ROLE_RUN_CANCEL
    )
    assert (
        required_role_for_request("POST", "/v1/cluster-profiles")
        == ROLE_CLUSTER_PROFILE_ADMIN
    )


def test_auth_middleware_returns_consistent_401_403_and_request_correlation() -> None:
    app = FastAPI()

    @app.get("/v1/pipelines")
    def read() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/v1/pipelines")
    def write() -> dict[str, bool]:
        return {"ok": True}

    app.add_middleware(DataFlowAuthMiddleware, authenticator=_authenticator())
    client = TestClient(app)

    unauthenticated = client.get("/v1/pipelines", headers={"x-request-id": "req-1"})
    assert unauthenticated.status_code == 401
    assert unauthenticated.json() == {
        "error": {
            "code": "UNAUTHENTICATED",
            "message": "a valid bearer token is required",
            "details": {"request_id": "req-1"},
        }
    }
    assert unauthenticated.headers["www-authenticate"] == "Bearer"
    assert unauthenticated.headers["x-request-id"] == "req-1"

    forbidden = client.post(
        "/v1/pipelines",
        headers={"authorization": "Bearer reader-token", "x-request-id": "req-2"},
    )
    assert forbidden.status_code == 403
    assert forbidden.json()["error"]["code"] == "FORBIDDEN"
    assert forbidden.json()["error"]["details"]["request_id"] == "req-2"

    allowed = client.post(
        "/v1/pipelines",
        headers={"authorization": "Bearer writer-token", "x-request-id": "req-3"},
    )
    assert allowed.status_code == 200
    assert allowed.headers["x-request-id"] == "req-3"


def test_sdk_token_is_sent_as_bearer_authorization_header() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer sdk-secret"
        return httpx.Response(200, json={"status": "ok"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        client = DataFlowClient(
            "http://dataflow.test",
            token="sdk-secret",
            http_client=http_client,
        )
        assert client.health() == {"status": "ok"}


def test_sdk_rejects_ambiguous_or_empty_credentials() -> None:
    with pytest.raises(ValueError, match="token must not be empty"):
        DataFlowClient("http://dataflow.test", token=" ")
    with pytest.raises(ValueError, match="either token or an Authorization header"):
        DataFlowClient(
            "http://dataflow.test",
            token="secret",
            headers={"Authorization": "Bearer other"},
        )
