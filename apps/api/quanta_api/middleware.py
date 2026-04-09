from __future__ import annotations

import json
import logging
import time
import uuid

from fastapi import Request

logger = logging.getLogger("quanta.request")


async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    request.state.request_id = request_id
    started = time.perf_counter()
    response = await call_next(request)
    duration_ms = round((time.perf_counter() - started) * 1000, 2)
    response.headers["x-request-id"] = request_id
    container = getattr(request.app.state, "container", None)
    if container is not None:
        container.metrics.increment("http.request.count")
        container.metrics.record_timing("http.request.duration_ms", duration_ms)
        container.trace_sink.emit(
            "http.request",
            {
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
            },
        )
    logger.info(
        json.dumps(
            {
                "event": "http_request",
                "requestId": request_id,
                "method": request.method,
                "path": request.url.path,
                "statusCode": response.status_code,
                "durationMs": duration_ms,
            }
        )
    )
    return response
