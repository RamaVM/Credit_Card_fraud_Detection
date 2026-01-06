#!/usr/bin/env bash
# Run a python service script inside the host environment (or container if you prefer)
# Usage: ./run_python_service.sh /workspace/2_stream_processing/predict_step1.py

SCRIPT_PATH="$1"
if [ -z "$SCRIPT_PATH" ]; then
  echo "Usage: $0 <script.py>"
  exit 1
fi

python3 -u "$SCRIPT_PATH"
