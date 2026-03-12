from fastapi import FastAPI

from quanta_api.api.routes import router
from quanta_api.app_state import ServiceContainer
from quanta_api.bootstrap import build_service_container
from quanta_api.core.config import settings


def create_app(container: ServiceContainer | None = None) -> FastAPI:
    app = FastAPI(title=settings.app_name, version=settings.app_version)
    app.state.container = container or build_service_container(settings)
    app.include_router(router)
    return app


app = create_app()
