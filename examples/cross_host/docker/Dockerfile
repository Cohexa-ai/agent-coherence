# Cross-host coherence demo image (demo-grade). Lightweight: stdlib + pyyaml only.
FROM python:3.11-slim

RUN pip install --no-cache-dir "pyyaml>=6.0"

COPY src /app/src
COPY examples /app/examples

ENV PYTHONPATH=/app/src
# Cross-host mode is opt-in; the demo containers set it here.
ENV CCS_REMOTE_COORDINATOR=1
WORKDIR /app
