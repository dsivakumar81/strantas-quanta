from __future__ import annotations

import logging
import signal
import threading
import time

from quanta_api.bootstrap import build_service_container
from quanta_api.core.config import settings


logger = logging.getLogger("quanta.worker_main")


def run_worker_forever() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    container = build_service_container(settings)
    stop_event = threading.Event()

    def _handle_signal(signum, _frame) -> None:  # type: ignore[no-untyped-def]
        logger.info("Received signal %s, stopping worker", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    container.worker_service.start()
    logger.info("QUANTA worker process started")
    try:
        while not stop_event.is_set():
            time.sleep(1)
    finally:
        container.worker_service.stop()
        logger.info("QUANTA worker process stopped")


if __name__ == "__main__":
    run_worker_forever()
