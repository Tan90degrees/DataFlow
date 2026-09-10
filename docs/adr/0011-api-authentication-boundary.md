# ADR 0011: Keep API authentication outside orchestration identity

- Status: Accepted
- Date: 2026-09-10

## Context

DataFlow is moving from a trusted single-team control plane toward shared deployments.
Pipeline submission, cancellation, and ClusterProfile administration therefore need an
explicit authorization boundary. Reusing Kubernetes service accounts or Ray identities
would couple public API policy to execution infrastructure and make local/integration tests
dependent on a cluster or identity provider.

The first implementation also needs to work without committing DataFlow to one OIDC vendor.

## Decision

The production HTTP entrypoint wraps the existing FastAPI control-plane application with a
pluggable bearer authentication and role-authorization middleware.

Authentication is split into three contracts:

1. a bearer claims provider converts an opaque token into provider claims;
2. a claims adapter maps OIDC-shaped claims (`sub` plus a configurable roles claim) into a
   small DataFlow identity;
3. an HTTP authorization policy maps DataFlow API operations to stable DataFlow roles.

The initial provider is a static token-to-claims mapping intended for tests, development,
and small controlled deployments. Future verified JWT/OIDC providers can implement the same
claims-provider protocol without changing API handlers or scheduler/controller code.

Authorization uses four coarse roles: `pipeline.read`, `pipeline.submit`, `run.cancel`, and
`cluster_profile.admin`. Kubernetes and Ray credentials remain execution concerns and do not
participate in API authorization.

Authentication is disabled by default for compatibility with existing development and E2E
deployments; shared production deployments must explicitly configure an authentication mode.
The later production Helm packaging work will surface this configuration as deployment
values/secrets.

## Consequences

- API authorization can be tested entirely against FastAPI/PostgreSQL without an external IdP.
- ClusterProfile administration can be separated from workflow submission and cancellation.
- A future OIDC verifier is an adapter replacement, not an API rewrite.
- Static bearer tokens are suitable only when secret distribution and rotation are handled by
  the deployment environment.
- Request correlation and allow/deny decisions are emitted in structured logs without logging
  bearer tokens or raw claims.
- Authentication-disabled mode remains intentionally visible in logs so operators can detect
  a deployment relying only on its network boundary.
