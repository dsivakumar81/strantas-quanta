import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from quanta_api.api.routes import router
from quanta_api.app_state import ServiceContainer
from quanta_api.bootstrap import build_service_container
from quanta_api.core.config import settings
from quanta_api.domain.contracts import ErrorResponse
from quanta_api.errors import ConflictError, InputValidationError
from quanta_api.middleware import request_context_middleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.container.worker_service.start()
    try:
        yield
    finally:
        app.state.container.worker_service.stop()


def create_app(container: ServiceContainer | None = None) -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )
    app.state.container = container or build_service_container(settings)
    app.middleware("http")(request_context_middleware)
    app.include_router(router)

    @app.exception_handler(InputValidationError)
    async def handle_input_error(request: Request, exc: InputValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content=ErrorResponse(
                error="input_validation_error",
                message=exc.message,
                request_id=getattr(request.state, "request_id", None),
                details=exc.details,
            ).model_dump(exclude_none=True),
        )

    @app.exception_handler(ConflictError)
    async def handle_conflict_error(request: Request, exc: ConflictError) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content=ErrorResponse(
                error="conflict",
                message=exc.message,
                request_id=getattr(request.state, "request_id", None),
                details=exc.details,
            ).model_dump(exclude_none=True),
        )

    @app.exception_handler(RequestValidationError)
    async def handle_request_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content=ErrorResponse(
                error="request_validation_error",
                message="Request payload validation failed",
                request_id=getattr(request.state, "request_id", None),
                details=exc.errors(),
            ).model_dump(exclude_none=True),
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=ErrorResponse(
                error="http_error",
                message=str(exc.detail),
                request_id=getattr(request.state, "request_id", None),
            ).model_dump(exclude_none=True),
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        logging.getLogger("quanta.error").exception("Unhandled error", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error="internal_error",
                message="Internal server error",
                request_id=getattr(request.state, "request_id", None),
            ).model_dump(exclude_none=True),
        )
    return app


app = create_app()
