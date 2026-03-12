import os

import psycopg


def main() -> None:
    dsn = os.getenv("QUANTA_DATABASE_DSN", "postgresql://quanta:quanta@127.0.0.1:5432/quanta")
    with psycopg.connect(dsn) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS public CASCADE")
            cursor.execute("CREATE SCHEMA public")


if __name__ == "__main__":
    main()
