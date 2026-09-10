# API authentication and RBAC

DataFlow keeps API identity separate from Ray and Kubernetes identities. The production
`dataflow-api` entrypoint builds the normal control-plane application and then installs a
pluggable bearer-authentication boundary.

## Modes

`DATAFLOW_API_AUTH_MODE=disabled` keeps the existing development behavior. This is the
default for backward compatibility and should only be used when another trusted network
boundary already protects the API.

`DATAFLOW_API_AUTH_MODE=static` enables bearer authentication using a JSON mapping in
`DATAFLOW_API_AUTH_STATIC_TOKENS`. Static mode is intended for deterministic tests,
development, and small controlled deployments; tokens are secrets and should be supplied
through the deployment secret mechanism rather than committed to source control.

Example configuration:

```bash
export DATAFLOW_API_AUTH_MODE=static
export DATAFLOW_API_AUTH_STATIC_TOKENS='{
  "submit-secret": {
    "sub": "pipeline-service",
    "roles": ["pipeline.read", "pipeline.submit", "run.cancel"]
  },
  "admin-secret": {
    "sub": "platform-admin",
    "roles": ["pipeline.read", "cluster_profile.admin"]
  }
}'
```

The claims adapter defaults to the OIDC-style `sub` subject claim and a `roles` claim.
`DATAFLOW_API_AUTH_SUBJECT_CLAIM` and `DATAFLOW_API_AUTH_ROLES_CLAIM` can select different
claim names. The roles claim may be an array of strings or a space-separated string.

The static provider is deliberately behind a claims-provider protocol. A JWT/OIDC verifier
can therefore replace token lookup while preserving the identity and authorization layers.

## Roles

| Role | Operations |
| --- | --- |
| `pipeline.read` | Read pipelines, runs, diagnostics, events, artifacts, and ClusterProfiles |
| `pipeline.submit` | Create pipelines, immutable versions, and PipelineRuns |
| `run.cancel` | Cancel a PipelineRun |
| `cluster_profile.admin` | Create or update ClusterProfiles |

Health, readiness, and metrics endpoints remain unauthenticated. Unknown `/v1/*` operations
are protected by at least `pipeline.read` so a newly added API does not accidentally become
anonymous.

Missing or invalid credentials return `401 UNAUTHENTICATED` with `WWW-Authenticate: Bearer`.
Authenticated callers without the required role receive `403 FORBIDDEN`. Both use the
standard DataFlow error envelope.

## Audit correlation

The authentication boundary accepts an incoming `X-Request-ID` or creates one. The same ID
is returned on the response and included in structured authorization logs together with the
subject, role decision, HTTP method, path, and required role. Bearer tokens and raw claims
are never logged.

## Python SDK

Pass the bearer token directly to the SDK:

```python
from dataflow.sdk import DataFlowClient

with DataFlowClient("https://dataflow.example", token="...") as client:
    pipelines = client.list_pipelines()
```

For advanced callers, an explicit `Authorization` header remains supported through the
existing `headers=` argument. `token=` and an explicit Authorization header are mutually
exclusive to avoid ambiguous credentials.
