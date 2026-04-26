#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV="${SKILL_DIR}/.venv"

if [[ ! -x "${VENV}/bin/python" ]]; then
  python3 -m venv "${VENV}"
  "${VENV}/bin/python" -m pip install --upgrade pip
  "${VENV}/bin/python" -m pip install -r "${SKILL_DIR}/requirements.txt"
fi

exec "${VENV}/bin/python" "${SCRIPT_DIR}/generate_trace_scenario_breakdowns.py" "$@"
