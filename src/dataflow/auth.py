"""Pluggable bearer authentication and role-based API authorization."""

from __future__ import annotations

import hmac
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from dataflow.observability import DEFAULT_OBSERVABILITY, Observability

ROLE_PIPELINE_READ = "pipeline.read"
ROLE_PIPELINE_SUBMIT = "pipeline.submit"
ROLE_RUN_CANCEL = "run.cancel"
ROLE_CLUSTER_PROFILE_ADMIN = "cluster_profile.admin"
ALL_ROLES = frozenset(
    {
        ROLE_PIPELINE_READ,
        ROLE_PIPELINE_SUBMIT,
        ROLE_RUN_CANCEL,
        ROLE_CLUSTER_PROFILE_ADMIN,
    }
)


@dataclass(frozen=True, slots=True)
class AuthenticatedIdentity:
    """Request identity derived from provider claims."""

    subject: str
    roles: frozenset[str]
    claims: Mapping[str, Any]

    def has_role(self, role: str) -> bool:
        return role in self.roles


class BearerClaimsProvider(Protocol):
    """Resolve an opaque bearer token into OIDC-shaped claims."""

    def claims_for_token(self, token: str) -> Mapping[str, Any] | None: ...


class StaticBearerClaimsProvider:
    """In-memory bearer provider for local deployments and deterministic tests.

    The provider deliberately returns claims rather than directly returning a DataFlow
    identity. A future JWT/OIDC verifier can implement ``BearerClaimsProvider`` while
    reusing the same claims-to-identity adapter and authorization policy.
    """

    def __init__(self, tokens: Mapping[str, Mapping[str, Any]]) -> None:
        if not tokens:
            raise ValueError("at least one static bearer token is required")
        normalized: list[tuple[str, dict[str, Any]]] = []
        for token, claims in tokens.items():
            if not isinstance(token, str) or not token:
                raise ValueError("static bearer tokens must be non-empty strings")
            if not isinstance(claims, Mapping):
                raise ValueError("static bearer token claims must be JSON objects")
            normalized.append((token, dict(claims)))
        self._tokens = tuple(normalized)

    def claims_for_token(self, token: str) -> Mapping[str, Any] | None:
        for expected, claims in self._tokens:
            if hmac.compare_digest(expected, token):
                return claims
        return None


@dataclass(frozen=True, slots=True)
class ClaimsIdentityAdapter:
    """Translate OIDC-compatible claims into the small DataFlow identity contract."""

    subject_claim: str = "sub"
    roles_claim: str = "roles"

    def identity_from_claims(self, claims: Mapping[str, Any]) -> AuthenticatedIdentity:
        subject = claims.get(self.subject_claim)
        if not isinstance(subject, str) or not subject.strip():
            raise ValueError(f"claim {self.subject_claim!r} must be a non-empty string")

        raw_roles = claims.get(self.roles_claim, ())
        if isinstance(raw_roles, str):
            roles = frozenset(value for value in raw_roles.split() if value)
        elif isinstance(raw_roles, (list, tuple, set, frozenset)):
            if not all(isinstance(value, str) and value for value in raw_roles):
                raise ValueError(f"claim {self.roles_claim!r} must contain only strings")
            roles = frozenset(raw_roles)
        else:
            raise ValueError(
                f"claim {self.roles_claim!r} must be a string or an array of strings"
            )

        return AuthenticatedIdentity(
            subject=subject,
            roles=roles,
            claims=dict(claims),
        )


class BearerAuthenticator:
    """Parse an Authorization header and adapt verified/provider claims."""

    def __init__(
        self,
        provider: BearerClaimsProvider,
        *,
        adapter: ClaimsIdentityAdapter | None = None,
    ) -> None:
        self._provider = provider
        self._adapter = adapter or ClaimsIdentityAdapter()

    def authenticate(self, authorization: str | None) -> AuthenticatedIdentity | None:
        if authorization is None:
            return None
        scheme, separator, token = authorization.partition(" ")
        if not separator or scheme.lower() != "bearer" or not token.strip():
            return None
        claims = self._provider.claims_for_token(token.strip())
        if claims is None:
            return None
        try:
            return self._adapter.identity_from_claims(claims)
        except ValueError:
            # Malformed provider claims are treated as an invalid credential at the HTTP
            # boundary; callers never receive internal identity-mapping details.
            return None


@dataclass(frozen=True, slots=True)
class ApiAuthSettings:
    """Environment-backed authentication settings for the production API entrypoint."""

    mode: str = "disabled"
    static_tokens: Mapping[str, Mapping[str, Any]] | None = None
    subject_claim: str = "sub"
    roles_claim: str = "roles"

    @classmethod
    def from_env(cls, environ: Mapping[str, str]) -> ApiAuthSettings:
        mode = environ.get("DATAFLOW_API_AUTH_MODE", "disabled").strip().lower()
        if mode not in {"disabled", "static"}:
            raise RuntimeError("DATAFLOW_API_AUTH_MODE must be 'disabled' or 'static'")
        if mode == "disabled":
            return cls(mode=mode)

        raw_tokens = environ.get("DATAFLOW_API_AUTH_STATIC_TOKENS")
        if not raw_tokens:
            raise RuntimeError(
                "DATAFLOW_API_AUTH_STATIC_TOKENS is required when DATAFLOW_API_AUTH_MODE=static"
            )
        try:
            decoded = json.loads(raw_tokens)
        except json.JSONDecodeError as error:
            raise RuntimeError("DATAFLOW_API_AUTH_STATIC_TOKENS must be valid JSON") from error
        if not isinstance(decoded, dict) or not decoded:
            raise RuntimeError("DATAFLOW_API_AUTH_STATIC_TOKENS must be a non-empty JSON object")
        tokens_are_valid = all(
            isinstance(token, str) and isinstance(claims, dict)
            for token, claims in decoded.items()
        )
        if not tokens_are_valid:
            raise RuntimeError(
                "DATAFLOW_API_AUTH_STATIC_TOKENS must map bearer token strings to claim objects"
            )
        subject_claim = environ.get("DATAFLOW_API_AUTH_SUBJECT_CLAIM", "sub").strip()
        roles_claim = environ.get("DATAFLOW_API_AUTH_ROLES_CLAIM", "roles").strip()
        if not subject_claim or not roles_claim:
            raise RuntimeError("authentication claim names must not be empty")
        return cls(
            mode=mode,
            static_tokens=decoded,
            subject_claim=subject_claim,
            roles_claim=roles_claim,
        )

    def authenticator(self) -> BearerAuthenticator | None:
        if self.mode == "disabled":
            return None
        assert self.static_tokens is not None
        return BearerAuthenticator(
            StaticBearerClaimsProvider(self.static_tokens),
            adapter=ClaimsIdentityAdapter(
                subject_claim=self.subject_claim,
                roles_claim=self.roles_claim,
            ),
        )


class DataFlowAuthMiddleware(BaseHTTPMiddleware):
    """Authenticate protected v1 routes and enforce coarse control-plane roles."""

    def __init__(
        self,
        app: Any,
        *,
        authenticator: BearerAuthenticator,
        observability: Observability | None = None,
    ) -> None:
        super().__init__(app)
        self._authenticator = authenticator
        self._observability = observability or DEFAULT_OBSERVABILITY

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("x-request-id") or str(uuid4())
        required_role = required_role_for_request(request.method, request.url.path)
        if required_role is None:
            with self._observability.bind(request_id=request_id):
                response = await call_next(request)
            response.headers["x-request-id"] = request_id
            return response

        identity = self._authenticator.authenticate(request.headers.get("authorization"))
        if identity is None:
            self._observability.warning(
                "api_authorization",
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                required_role=required_role,
                decision="unauthenticated",
            )
            return _auth_error_response(
                401,
                "UNAUTHENTICATED",
                "a valid bearer token is required",
                request_id=request_id,
                authenticate=True,
            )

        request.state.identity = identity
        request.state.request_id = request_id
        if not identity.has_role(required_role):
            self._observability.warning(
                "api_authorization",
                request_id=request_id,
                subject=identity.subject,
                roles=sorted(identity.roles),
                method=request.method,
                path=request.url.path,
                required_role=required_role,
                decision="forbidden",
            )
            return _auth_error_response(
                403,
                "FORBIDDEN",
                f"role {required_role!r} is required",
                request_id=request_id,
            )

        self._observability.info(
            "api_authorization",
            request_id=request_id,
            subject=identity.subject,
            roles=sorted(identity.roles),
            method=request.method,
            path=request.url.path,
            required_role=required_role,
            decision="allowed",
        )
        with self._observability.bind(
            request_id=request_id,
            subject=identity.subject,
        ):
            response = await call_next(request)
        response.headers["x-request-id"] = request_id
        return response


def required_role_for_request(method: str, path: str) -> str | None:
    """Return the role required by a public API operation.

    Health/readiness/metrics and non-v1 paths remain public. Unknown v1 operations fail
    closed behind the read role rather than accidentally becoming anonymous endpoints.
    """

    normalized_method = method.upper()
    if normalized_method == "OPTIONS" or not path.startswith("/v1/"):
        return None

    if path.startswith("/v1/cluster-profiles"):
        if normalized_method in {"POST", "PUT", "PATCH", "DELETE"}:
            return ROLE_CLUSTER_PROFILE_ADMIN
        return ROLE_PIPELINE_READ

    if path.startswith("/v1/pipeline-runs/") and path.endswith("/cancel"):
        return ROLE_RUN_CANCEL

    if normalized_method == "GET":
        return ROLE_PIPELINE_READ

    if normalized_method == "POST" and (
        path == "/v1/pipelines"
        or path == "/v1/pipeline-runs"
        or (path.startswith("/v1/pipelines/") and path.endswith("/versions"))
    ):
        return ROLE_PIPELINE_SUBMIT

    return ROLE_PIPELINE_READ


def install_api_auth(
    app: FastAPI,
    settings: ApiAuthSettings,
    *,
    observability: Observability | None = None,
) -> FastAPI:
    """Install configured authentication before the application starts serving."""

    obs = observability or DEFAULT_OBSERVABILITY
    authenticator = settings.authenticator()
    if authenticator is None:
        obs.warning("api_authentication_disabled")
        return app
    app.add_middleware(
        DataFlowAuthMiddleware,
        authenticator=authenticator,
        observability=obs,
    )
    return app


def _auth_error_response(
    status_code: int,
    code: str,
    message: str,
    *,
    request_id: str,
    authenticate: bool = False,
) -> JSONResponse:
    response = JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "details": {"request_id": request_id},
            }
        },
    )
    response.headers["x-request-id"] = request_id
    if authenticate:
        response.headers["www-authenticate"] = "Bearer"
    return response


__all__ = [
    "ALL_ROLES",
    "ApiAuthSettings",
    "AuthenticatedIdentity",
    "BearerAuthenticator",
    "BearerClaimsProvider",
    "ClaimsIdentityAdapter",
    "DataFlowAuthMiddleware",
    "ROLE_CLUSTER_PROFILE_ADMIN",
    "ROLE_PIPELINE_READ",
    "ROLE_PIPELINE_SUBMIT",
    "ROLE_RUN_CANCEL",
    "StaticBearerClaimsProvider",
    "install_api_auth",
    "required_role_for_request",
]
