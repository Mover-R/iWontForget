"""aiogram middleware stack.

Middleware is applied in registration order (outer → inner):
  1. LoggingMiddleware   — log every update with request_id
  2. MetricsMiddleware   — Prometheus counters/histograms
  3. UserContextMiddleware — load/create user from User Service, inject into handler kwargs
  4. ErrorHandlerMiddleware — catch unhandled exceptions, send user-friendly error message
"""
