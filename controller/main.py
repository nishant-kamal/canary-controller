"""
Controller Entry Point
======================
Environment variables se configure hota hai — hardcode kuch nahi.

Usage (local testing):
    IN_CLUSTER=false APP_NAME=myapp python main.py

Usage (Kubernetes Job mein):
    ENV vars ConfigMap se inject hote hain.

Fix applied:
  - Writes /tmp/controller.pid on startup AND touches it before every
    evaluation step (via touch_pid callback passed to CanaryController).
    Previously the file was written once and never updated, so a hanging
    process (e.g., stuck in time.sleep) would keep passing the liveness
    probe indefinitely. Now the probe detects stalls within ~90s.
"""

import os
import sys
import logging
from canary_controller import CanaryController, PROMOTED, ROLLEDBACK, NOT_FOUND
from metrics_client import MetricsClient

logger = logging.getLogger(__name__)

PID_FILE = "/tmp/controller.pid"


def touch_pid():
    """
    Liveness probe ke liye PID file touch karo (mtime update).
    Startup pe aur har evaluation step se pehle call hota hai.
    Agar process hang ho jaye to file stale ho jaayegi aur probe fail kar dega.
    """
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))


def main():
    # ── Config from environment ────────────────────────────────────────
    app_name   = os.getenv("APP_NAME", "myapp")
    namespace  = os.getenv("NAMESPACE", "default")
    in_cluster = os.getenv("IN_CLUSTER", "true").lower() == "true"

    logger.info("=" * 55)
    logger.info("  Canary Controller — Dissertation (2024MT03065)  ")
    logger.info("=" * 55)
    logger.info(f"  APP_NAME   : {app_name}")
    logger.info(f"  NAMESPACE  : {namespace}")
    logger.info(f"  IN_CLUSTER : {in_cluster}")
    logger.info("=" * 55)

    # FIX: touch_pid() called on startup AND passed as callback so
    # CanaryController touches it before each sleep — liveness probe
    # will detect a hung process within (periodSeconds * failureThreshold) = 90s.
    touch_pid()
    logger.info(f"📝  PID file written: {PID_FILE}")

    # ── Prometheus health check ────────────────────────────────────────
    mc = MetricsClient()
    if not mc.health_check():
        logger.error("❌ Prometheus unreachable! PROMETHEUS_URL check karo.")
        logger.error("   Hint: kubectl get svc -A | grep prometheus")
        sys.exit(1)
    logger.info("✅  Prometheus reachable")

    # ── Run controller ─────────────────────────────────────────────────
    controller = CanaryController(in_cluster=in_cluster)
    result = controller.run(app_name=app_name, namespace=namespace, heartbeat=touch_pid)

    # ── Exit codes (CI/CD pipeline ke liye useful) ─────────────────────
    if result == PROMOTED:
        logger.info("🟢  Exit 0 — Canary promoted successfully")
        sys.exit(0)
    elif result == ROLLEDBACK:
        logger.error("🔴  Exit 1 — Rollback triggered (metrics threshold breach)")
        sys.exit(1)
    elif result == NOT_FOUND:
        logger.warning("🟡  Exit 2 — No canary deployment found")
        sys.exit(2)


if __name__ == "__main__":
    main()
