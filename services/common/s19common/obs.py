"""Shared observability: JSON logs for Cloud Logging + Prometheus HTTP metrics.

Every service calls ``setup_logging(SERVICE)`` and (for FastAPI apps)
``instrument(app, SERVICE)`` which adds:
  * GET /metrics  - Prometheus exposition format (scraped by kube-prometheus-stack)
  * GET /healthz  - liveness/readiness probe target (also used by the GKE Ingress health check)
  * a middleware recording http_requests_total / http_request_duration_seconds
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

HTTP_REQUESTS = Counter(
    "http_requests_total", "HTTP requests handled", ["service", "method", "route", "status"]
)
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["service", "method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
HTTP_INFLIGHT = Gauge("http_requests_in_flight", "In-flight HTTP requests", ["service"])

_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", None, None)).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """One JSON object per line. Cloud Logging maps `severity` and `message` automatically."""

    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "severity": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k not in _RESERVED and not k.startswith("_"):
                try:
                    json.dumps(v)
                    payload[k] = v
                except TypeError:
                    payload[k] = str(v)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def setup_logging(service: str) -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    for noisy in ("uvicorn.access", "httpx", "httpcore", "google", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(service)


def instrument(app, service: str) -> None:
    """Attach metrics middleware + /metrics + /healthz to a FastAPI app."""
    from fastapi import Request, Response  # local import: replica server does not use FastAPI

    @app.middleware("http")
    async def _metrics_mw(request: Request, call_next):
        if request.url.path in ("/metrics", "/healthz"):
            return await call_next(request)
        HTTP_INFLIGHT.labels(service).inc()
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            route_path = getattr(route, "path", "unmatched")
            HTTP_INFLIGHT.labels(service).dec()
            HTTP_LATENCY.labels(service, request.method, route_path).observe(time.perf_counter() - start)
            HTTP_REQUESTS.labels(service, request.method, route_path, str(status)).inc()

    @app.get("/metrics", include_in_schema=False)
    async def _metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/healthz", include_in_schema=False)
    async def _healthz():
        return {"status": "ok", "service": service}
