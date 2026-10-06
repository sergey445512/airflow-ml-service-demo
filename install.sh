#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
if [ ! -x "$DEMO_ROOT/.venv/bin/python" ]; then
    python3 -m venv "$DEMO_ROOT/.venv"
fi
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
python -m pip install 'apache-airflow==3.3.2' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-${PYTHON_VERSION}.txt"
