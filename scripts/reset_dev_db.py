import os
import time

import psycopg


def wait_for_database(dsn: str, timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with psycopg.connect(dsn, connect_timeout=2) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                return
        except psycopg.OperationalError as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"Timed out waiting for database readiness: {last_error}")


def main() -> None:
    dsn = os.getenv("QUANTA_DATABASE_DSN", "postgresql://quanta:quanta@127.0.0.1:5432/quanta")
    wait_for_database(dsn)
    with psycopg.connect(dsn) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS public CASCADE")
            cursor.execute("CREATE SCHEMA public")


if __name__ == "__main__":
    main()
