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

Fixes applied:
  - TimeoutError now caught in run() → returns ROLLEDBACK gracefully
  - _set_traffic_split uses read-modify-write to preserve timeout/retry config
  - get_error_rate failure now treated as unhealthy (None → rollback)
  - _finalize_promotion looks up container by name, not index [0]
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
EVALUATION_WINDOW_SEC = 120    # FIX: 30s → 120s; matches PromQL "2m" window
TRAFFIC_STEPS         = [10, 30, 50, 80, 100]  # canary traffic %

# ─── Container name for the main app container ────────────────────────────
# FIX: use name-based lookup instead of containers[0] assumption
APP_CONTAINER_NAME = "app"  # change if your container has a different name

# ─── Result codes ──────────────────────────────────────────────────────────
PROMOTED   = "PROMOTED"
ROLLEDBACK = "ROLLEDBACK"
NOT_FOUND  = "NOT_FOUND"


@dataclass
class HealthSnapshot:
    error_rate: Optional[float]   # None = metrics unavailable
    latency_p99: Optional[float]  # None = metrics unavailable
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

        # FIX: TimeoutError catch karo — crash nahi, ROLLEDBACK return karo
        try:
            self._wait_for_canary_ready(app_name, namespace)
        except TimeoutError as e:
            logger.error(f"⏰  {e}")
            logger.error("🚨  Canary pods ready nahi hue — rolling back.")
            self._rollback(app_name, namespace)
            return ROLLEDBACK

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

        # Should never reach here; loop always hits canary_pct == 100 first
        return PROMOTED

    # ──────────────────────────────────────────────────────────────────────
    # HEALTH EVALUATION
    # ──────────────────────────────────────────────────────────────────────

    def _evaluate_health(self, app_name: str) -> HealthSnapshot:
        # FIX: treat None (metrics unavailable) as unhealthy — fail safe
        error_rate  = self.metrics.get_error_rate(app_name, "canary")
        latency_p99 = self.metrics.get_p99_latency(app_name, "canary")

        issues = []

        if error_rate is None:
            issues.append("error_rate=UNAVAILABLE (Prometheus unreachable — treating as unhealthy)")
        elif error_rate > ERROR_RATE_THRESHOLD:
            issues.append(f"error_rate={error_rate:.2%} > threshold={ERROR_RATE_THRESHOLD:.2%}")

        if latency_p99 is None:
            issues.append("latency_p99=UNAVAILABLE (Prometheus unreachable — treating as unhealthy)")
        elif latency_p99 > LATENCY_P99_THRESHOLD:
            issues.append(f"p99={latency_p99:.0f}ms > threshold={LATENCY_P99_THRESHOLD}ms")

        is_healthy = len(issues) == 0
        reason = " | ".join(issues) if issues else "OK"

        return HealthSnapshot(error_rate, latency_p99, is_healthy, reason)

    def _log_health(self, h: HealthSnapshot, canary_pct: int):
        status = "✅ HEALTHY" if h.is_healthy else "❌ UNHEALTHY"
        error_str   = f"{h.error_rate:.2%}"   if h.error_rate   is not None else "N/A"
        latency_str = f"{h.latency_p99:.0f}ms" if h.latency_p99 is not None else "N/A"
        logger.info(
            f"{status} | canary={canary_pct}% | "
            f"error_rate={error_str} | p99_latency={latency_str}"
        )

    # ──────────────────────────────────────────────────────────────────────
    # KUBERNETES OPERATIONS
    # ──────────────────────────────────────────────────────────────────────

    def _set_traffic_split(self, app_name: str, namespace: str, canary_pct: int):
        """
        Istio VirtualService ka weight update karo.

        FIX: Read-modify-write pattern — existing timeout/retry/fault config
        preserve hoga. Pehle wala approach (direct patch) poora http[0] object
        replace kar deta tha aur timeout/retries silently delete ho jaate the.
        """
        try:
            vs = self.custom_api.get_namespaced_custom_object(
                group="networking.istio.io",
                version="v1alpha3",
                namespace=namespace,
                plural="virtualservices",
                name=app_name,
            )
        except ApiException as e:
            logger.error(f"VirtualService read failed: {e.reason}")
            raise

        # Sirf weights update karo — baaki config (timeout, retries) untouched
        http_rules = vs.get("spec", {}).get("http", [])
        if not http_rules:
            raise ValueError(f"VirtualService '{app_name}' mein koi http rule nahi mila")

        route = http_rules[0].get("route", [])
        for destination_entry in route:
            subset = destination_entry.get("destination", {}).get("subset")
            if subset == "stable":
                destination_entry["weight"] = 100 - canary_pct
            elif subset == "canary":
                destination_entry["weight"] = canary_pct

        self.custom_api.patch_namespaced_custom_object(
            group="networking.istio.io",
            version="v1alpha3",
            namespace=namespace,
            plural="virtualservices",
            name=app_name,
            body=vs,
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
        """
        Canary image ko stable mein promote karo, canary pod hata do.

        FIX: container name se image lookup karo, index[0] nahi.
        Multi-container pods (e.g., with sidecars) ke liye safe.
        """
        logger.info("🔁  Finalizing: updating stable with canary image...")

        canary_dep = self.apps_v1.read_namespaced_deployment(
            name=f"{app_name}-canary", namespace=namespace
        )

        # FIX: named container lookup
        canary_image = self._get_container_image(canary_dep, APP_CONTAINER_NAME)
        if canary_image is None:
            # Fallback: use containers[0] but log a warning
            logger.warning(
                f"Container '{APP_CONTAINER_NAME}' not found in canary pod spec. "
                f"Falling back to containers[0]. Set APP_CONTAINER_NAME correctly."
            )
            canary_image = canary_dep.spec.template.spec.containers[0].image

        stable_dep = self.apps_v1.read_namespaced_deployment(
            name=f"{app_name}-stable", namespace=namespace
        )

        # Update matching container in stable, or containers[0] as fallback
        updated = self._set_container_image(stable_dep, APP_CONTAINER_NAME, canary_image)
        if not updated:
            logger.warning(
                f"Container '{APP_CONTAINER_NAME}' not found in stable pod spec. "
                f"Falling back to containers[0]."
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

    def _get_container_image(self, deployment, container_name: str) -> Optional[str]:
        """Named container ka image return karo, ya None if not found."""
        for c in deployment.spec.template.spec.containers:
            if c.name == container_name:
                return c.image
        return None

    def _set_container_image(self, deployment, container_name: str, image: str) -> bool:
        """Named container ka image set karo. Returns True if found and updated."""
        for c in deployment.spec.template.spec.containers:
            if c.name == container_name:
                c.image = image
                return True
        return False

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
            ready   = dep.status.ready_replicas or 0
            desired = dep.spec.replicas or 1
            if ready >= desired:
                logger.info(f"✅  Canary pods ready ({ready}/{desired})")
                return
            logger.info(f"⏳  Waiting for canary pods ({ready}/{desired} ready)...")
            time.sleep(poll_interval)

        raise TimeoutError(f"Canary pods {timeout_sec}s mein ready nahi hue!")
