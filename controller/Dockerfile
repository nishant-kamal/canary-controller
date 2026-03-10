# ─── Stage 1: Builder ─────────────────────────────────────────────────────
# Alag stage mein dependencies install karo
# Final image mein sirf compiled packages jayenge, pip nahi
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps (kubernetes client ko kuch C headers chahiye)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ─── Stage 2: Final Image ─────────────────────────────────────────────────
# Sirf runtime — no gcc, no pip, no build tools
FROM python:3.11-slim

WORKDIR /app

# Builder se sirf installed packages copy karo
COPY --from=builder /install /usr/local

# Controller source code
COPY main.py canary_controller.py metrics_client.py ./

# ── Security: non-root user ────────────────────────────────────────────────
RUN useradd --no-create-home --shell /bin/false controller
USER controller

# ── Metadata ──────────────────────────────────────────────────────────────
LABEL maintainer="Nishant Kamal <2024mt03065@wilp.bits-pilani.ac.in>"
LABEL description="Canary Deployment Controller — BITS Dissertation"
LABEL version="1.0"

# ── Health check ──────────────────────────────────────────────────────────
# Kubernetes liveness probe use kar sakta hai isko
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import kubernetes; import requests; print('ok')" || exit 1

CMD ["python", "main.py"]
