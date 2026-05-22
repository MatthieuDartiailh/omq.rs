# Security

## Reporting a vulnerability

Email **paddor@protonmail.ch**. Do not open a public GitHub issue for
security vulnerabilities.

## BLAKE3ZMQ

The BLAKE3ZMQ mechanism (`blake3zmq` feature) has not been
independently audited. Do not use it for security-critical
applications until it has had third-party review. Use the CURVE
mechanism (`curve` feature, RFC 26) as the audited alternative.
