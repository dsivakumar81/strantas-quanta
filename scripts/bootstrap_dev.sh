#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f ".env" ]]; then
  cp .env.example .env
fi

set -a
source .env
set +a

docker compose up -d

python3 - <<'PY'
import socket
import time

targets = [("127.0.0.1", 5432), ("127.0.0.1", 9100)]
deadline = time.time() + 60

for host, port in targets:
    while time.time() < deadline:
        sock = socket.socket()
        sock.settimeout(1)
        try:
            sock.connect((host, port))
            sock.close()
            break
        except OSError:
            time.sleep(1)
        finally:
            sock.close()
    else:
        raise SystemExit(f"Timed out waiting for {host}:{port}")
PY

source .venv/bin/activate
python scripts/reset_dev_db.py
alembic upgrade head
python scripts/generate_sample_pdf.py
python scripts/generate_sample_xlsx.py
python scripts/generate_scanned_pdf.py

echo "QUANTA dev stack is ready."
