from __future__ import annotations

from fastapi import Request

from quanta_api.app_state import ServiceContainer
from quanta_api.bootstrap import build_service_container

container = build_service_container()


def get_container(request: Request) -> ServiceContainer:
    return request.app.state.container
