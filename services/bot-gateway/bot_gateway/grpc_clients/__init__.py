"""gRPC clients for Go backend services.

Each client wraps a generated gRPC stub with:
- Async channel management (connect/close)
- Structured error logging
- Timeout configuration

All clients share the same BaseGrpcClient pattern.
"""
