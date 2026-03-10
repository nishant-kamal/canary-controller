"""
Canary Deployment Controller
============================
Dissertation: Metrics-Driven Traffic Management for Self-Healing Microservices
Author: Nishant Kamal (2024MT03065)

Flow:
  1. Canary deployment detect karo
  2. Traffic gradually badhaao (10 → 30 → 50 → 80 → 100)
  3. Har step pe Prometheus metrics check karo
  4. Unhealthy? → Rollback. Healthy? → Promote.
"""

import time
import logging
from dataclasses import dataclass
from typing import Optional
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from metrics_client import MetricsClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─── Thresholds (tune karo experiments ke liye) ────────────────────────────
ERROR_RATE_THRESHOLD  = 0.05   # 5%  — isse zyada 5xx → rollback
LATENCY_P99_THRESHOLD = 500    # ms  — isse zyada latency → rollback
EVALUATION_WINDOW_SEC = 30     # har step ke baad kitne seconds wait karo
TRAFFIC_STEPS         = [10, 30, 50, 80, 100]  # canary traffic %

# ─── Result codes ──────────────────────────────────────────────────────────
PROMOTED  = "PROMOTED"
ROLLEDBACK = "ROLLEDBACK"
NOT_FOUND  = "NOT_FOUND"


@dataclass
class HealthSnapshot:
    error_rate: float
    latency_p99: float
    is_healthy: bool
    reason: str


class CanaryController:
    """
    Core controller.
    Ek baar run() call karo — controller poora canary lifecycle handle karega.
    """

    def __init__(self, in_cluster: bool = True):
        # in_cluster=True  → pod ke andar chal raha hai (production)
        # in_cluster=False → local kubectl config use karega (testing)
        if in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config()

        self.apps_v1    = client.AppsV1Api()
        self.custom_api = client.CustomObjectsApi()
        self.metrics    = MetricsClient()

    # ──────────────────────────────────────────────────────────────────────
    # PUBLIC ENTRY POINT
    # ──────────────────────────────────────────────────────────────────────

    def run(self, app_name: str, namespace: str = "default") -> str:
        """
        Canary controller ka main loop.

        Returns:
            "PROMOTED"    → canary successfully 100% pe aa gaya
            "ROLLEDBACK"  → metrics kharab the, rollback ho gaya
            "NOT_FOUND"   → canary deployment nahi mila
        """
        logger.info(f"▶  Controller started | app={app_name} ns={namespace}")

        if not self._canary_exists(app_name, namespace):
            logger.warning("⚠  No canary deployment found. Deploy karo pehle.")
            return NOT_FOUND

        # Step 0: canary shuru karo 0% pe (stable chal raha hai abhi bhi)
        self._set_traffic_split(app_name, namespace, canary_pct=0)
        logger.info("⏳  Canary pods ready hone ka wait...")
        self._wait_for_canary_ready(app_name, namespace)

        # Step loop: 10 → 30 → 50 → 80 → 100
        for step, canary_pct in enumerate(TRAFFIC_STEPS):
            logger.info(f"\n{'─'*50}")
            logger.info(f"📊  Step {step+1}/{len(TRAFFIC_STEPS)} → Canary traffic: {canary_pct}%")

            self._set_traffic_split(app_name, namespace, canary_pct)
            logger.info(f"🔀  VirtualService updated: stable={100-canary_pct}% canary={canary_pct}%")

            # Wait karo metrics stable hone tak
            logger.info(f"⏱  {EVALUATION_WINDOW_SEC}s wait kar raha hoon metrics ke liye...")
            time.sleep(EVALUATION_WINDOW_SEC)

            # Health check
            health = self._evaluate_health(app_name)
            self._log_health(health, canary_pct)

            if not health.is_healthy:
                logger.error(f"🚨  UNHEALTHY! Reason: {health.reason}")
                self._rollback(app_name, namespace)
                return ROLLEDBACK

            if canary_pct == 100:
                logger.info("🎉  Canary fully promoted!")
                self._finalize_promotion(app_name, namespace)
                return PROMOTED

        return PROMOTED  # should not reach here

    # ──────────────────────────────────────────────────────────────────────
    # HEALTH EVALUATION
    # ──────────────────────────────────────────────────────────────────────

    def _evaluate_health(self, app_name: str) -> HealthSnapshot:
        error_rate  = self.metrics.get_error_rate(app_name, "canary")
        latency_p99 = self.metrics.get_p99_latency(app_name, "canary")

        issues = []
        if error_rate > ERROR_RATE_THRESHOLD:
            issues.append(f"error_rate={error_rate:.2%} > threshold={ERROR_RATE_THRESHOLD:.2%}")
        if latency_p99 > LATENCY_P99_THRESHOLD:
            issues.append(f"p99={latency_p99:.0f}ms > threshold={LATENCY_P99_THRESHOLD}ms")

        is_healthy = len(issues) == 0
        reason = " | ".join(issues) if issues else "OK"

        return HealthSnapshot(error_rate, latency_p99, is_healthy, reason)

    def _log_health(self, h: HealthSnapshot, canary_pct: int):
        status = "✅ HEALTHY" if h.is_healthy else "❌ UNHEALTHY"
        logger.info(
            f"{status} | canary={canary_pct}% | "
            f"error_rate={h.error_rate:.2%} | p99_latency={h.latency_p99:.0f}ms"
        )

    # ──────────────────────────────────────────────────────────────────────
    # KUBERNETES OPERATIONS
    # ──────────────────────────────────────────────────────────────────────

    def _set_traffic_split(self, app_name: str, namespace: str, canary_pct: int):
        """Istio VirtualService ka weight update karo."""
        patch = {
            "spec": {
                "http": [{
                    "route": [
                        {
                            "destination": {"host": app_name, "subset": "stable"},
                            "weight": 100 - canary_pct
                        },
                        {
                            "destination": {"host": app_name, "subset": "canary"},
                            "weight": canary_pct
                        }
                    ]
                }]
            }
        }
        self.custom_api.patch_namespaced_custom_object(
            group="networking.istio.io",
            version="v1alpha3",
            namespace=namespace,
            plural="virtualservices",
            name=app_name,
            body=patch
        )

    def _rollback(self, app_name: str, namespace: str):
        """Saara traffic stable pe wapas, canary delete karo."""
        logger.info("⏪  Rolling back: 100% traffic → stable")
        self._set_traffic_split(app_name, namespace, canary_pct=0)

        try:
            self.apps_v1.delete_namespaced_deployment(
                name=f"{app_name}-canary",
                namespace=namespace,
                body=client.V1DeleteOptions(grace_period_seconds=0)
            )
            logger.info(f"🗑  Canary deployment deleted.")
        except ApiException as e:
            logger.warning(f"Canary delete failed (already gone?): {e.reason}")

        logger.info("✅  Rollback complete. System stable.")

    def _finalize_promotion(self, app_name: str, namespace: str):
        """Canary image ko stable mein promote karo, canary pod hata do."""
        logger.info("🔁  Finalizing: updating stable with canary image...")

        # Canary ka image lo
        canary_dep  = self.apps_v1.read_namespaced_deployment(
            name=f"{app_name}-canary", namespace=namespace
        )
        canary_image = canary_dep.spec.template.spec.containers[0].image

        # Stable ka image update karo
        stable_dep = self.apps_v1.read_namespaced_deployment(
            name=f"{app_name}-stable", namespace=namespace
        )
        stable_dep.spec.template.spec.containers[0].image = canary_image
        self.apps_v1.patch_namespaced_deployment(
            name=f"{app_name}-stable", namespace=namespace, body=stable_dep
        )
        logger.info(f"✅  Stable updated → image: {canary_image}")

        # Canary hata do
        self.apps_v1.delete_namespaced_deployment(
            name=f"{app_name}-canary", namespace=namespace,
            body=client.V1DeleteOptions(grace_period_seconds=5)
        )
        logger.info("🗑  Canary deployment removed. Promotion complete.")

    def _canary_exists(self, app_name: str, namespace: str) -> bool:
        try:
            self.apps_v1.read_namespaced_deployment(
                name=f"{app_name}-canary", namespace=namespace
            )
            return True
        except ApiException:
            return False

    def _wait_for_canary_ready(
        self, app_name: str, namespace: str,
        timeout_sec: int = 120, poll_interval: int = 5
    ):
        """Canary pods Running hone tak wait karo."""
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            dep = self.apps_v1.read_namespaced_deployment(
                name=f"{app_name}-canary", namespace=namespace
            )
            ready = dep.status.ready_replicas or 0
            desired = dep.spec.replicas or 1
            if ready >= desired:
                logger.info(f"✅  Canary pods ready ({ready}/{desired})")
                return
            logger.info(f"⏳  Waiting for canary pods ({ready}/{desired} ready)...")
            time.sleep(poll_interval)

        raise TimeoutError(f"Canary pods {timeout_sec}s mein ready nahi hue!")
