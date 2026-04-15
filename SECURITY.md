# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Ariadne, **please do not open a public issue**. Instead, report it privately:

- **GitHub**: Report via [GitHub Security Advisories](https://github.com/cjfravel-dev/ariadne/security/advisories/new)

Please include:

- A description of the vulnerability
- Steps to reproduce (if applicable)
- The potential impact
- Any suggested fixes

## Response Timeline

- **Acknowledgment**: Within 48 hours of your report
- **Initial assessment**: Within 1 week
- **Fix or mitigation**: Depending on severity, typically within 2–4 weeks

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x-beta | ✅ |
| 0.0.x-alpha | ❌ |

## Scope

Ariadne is a client-side Spark library that reads and writes index data to a Hadoop-accessible filesystem. Security concerns most likely to be relevant include:

- Path traversal or injection via index names or file paths
- Deserialization vulnerabilities in metadata parsing (Gson)
- Lock file manipulation leading to data corruption

## Disclosure Policy

We follow coordinated disclosure. Once a fix is available, we will:

1. Release a patched version
2. Publish a security advisory on GitHub
3. Credit the reporter (unless they prefer to remain anonymous)
