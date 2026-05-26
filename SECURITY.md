# Security Policy

## Reporting a Vulnerability

This is a Proof of Concept project intended for local development and evaluation. It is **not
designed for production use** and has not undergone a security audit.

If you discover a security vulnerability, please report it privately by opening a GitHub issue
marked **[security]** or by contacting the repository maintainer directly.

Do not open a public issue for active security vulnerabilities -- wait for acknowledgement before
public disclosure.

## Supported Versions

Only the current `main` branch is supported. No backported security patches are provided.

## Known Limitations

- Default credentials are used throughout for local convenience. Never expose this stack on a
  public network.
- The `rag-service` security model (permit-all behind a network policy or OAuth2/OIDC via hxpr
  IDP) is intentionally simplified for PoC purposes.
- ACL enforcement relies on hxpr's permission filtering at query time. Ingestion-time ACL accuracy
  depends on the source system's permission model being correctly mapped.
