"""
Prometheus Metrics Client
=========================
Istio ke sidecar proxies se metrics fetch karta hai via Prometheus.

Istio automatically yeh metrics expose karta hai:
  - istio_requests_total        → request count by status code
  - istio_request_duration_*    → latency histogram
"""

import logging
from typing import Optional
import requests

logger = logging.getLogger(__name__)

# Prometheus service address (Kubernetes ke andar)
# Format: http://<service-name>.<namespace>.svc.cluster.local:9090
PROMETHEUS_URL = "http://prometheus-operated.monitoring.svc.cluster.local:9090"
QUERY_TIMEOUT  = 10  # seconds


class MetricsClient:

    def __init__(self, prometheus_url: str = PROMETHEUS_URL):
        self.url = prometheus_url

    # ──────────────────────────────────────────────────────────────────────
    # PUBLIC METHODS
    # ──────────────────────────────────────────────────────────────────────

    def get_error_rate(self, app: str, version: str, window: str = "2m") -> float:
        """
        5xx error rate return karta hai (0.0 to 1.0).
        Example: 0.07 = 7% requests fail ho rahe hain.

        PromQL logic:
          5xx requests ka rate / total requests ka rate
        """
        promql = f"""
            sum(rate(istio_requests_total{{
                destination_app="{app}",
                destination_version="{version}",
                response_code=~"5.."
            }}[{window}]))
            /
            sum(rate(istio_requests_total{{
                destination_app="{app}",
                destination_version="{version}"
            }}[{window}]))
        """
        result = self._instant_query(promql)
        if result is None:
            logger.warning(f"Error rate query failed for {app}/{version}, defaulting to 0.0")
            return 0.0
        return float(result)

    def get_p99_latency(self, app: str, version: str, window: str = "2m") -> float:
        """
        P99 latency milliseconds mein return karta hai.
        Example: 342.5 = 99% requests 342ms se kam mein complete hue.

        PromQL logic:
          Istio request duration histogram se 99th percentile nikalna.
        """
        promql = f"""
            histogram_quantile(0.99,
                sum(rate(istio_request_duration_milliseconds_bucket{{
                    destination_app="{app}",
                    destination_version="{version}"
                }}[{window}])) by (le)
            )
        """
        result = self._instant_query(promql)
        if result is None:
            logger.warning(f"Latency query failed for {app}/{version}, defaulting to 0.0")
            return 0.0
        return float(result)

    def get_success_rate(self, app: str, version: str, window: str = "2m") -> float:
        """
        2xx success rate (0.0 to 1.0).
        Dissertation charts ke liye useful.
        """
        promql = f"""
            sum(rate(istio_requests_total{{
                destination_app="{app}",
                destination_version="{version}",
                response_code=~"2.."
            }}[{window}]))
            /
            sum(rate(istio_requests_total{{
                destination_app="{app}",
                destination_version="{version}"
            }}[{window}]))
        """
        result = self._instant_query(promql)
        return float(result) if result is not None else 0.0

    def get_request_rate(self, app: str, version: str, window: str = "2m") -> float:
        """Requests per second — traffic load verify karne ke liye."""
        promql = f"""
            sum(rate(istio_requests_total{{
                destination_app="{app}",
                destination_version="{version}"
            }}[{window}]))
        """
        result = self._instant_query(promql)
        return float(result) if result is not None else 0.0

    def health_check(self) -> bool:
        """Prometheus reachable hai ya nahi check karo."""
        try:
            resp = requests.get(f"{self.url}/-/healthy", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    # ──────────────────────────────────────────────────────────────────────
    # PRIVATE
    # ──────────────────────────────────────────────────────────────────────

    def _instant_query(self, promql: str) -> Optional[str]:
        """
        Prometheus instant query run karo.
        Returns: string value ya None (on error / no data).
        """
        try:
            resp = requests.get(
                f"{self.url}/api/v1/query",
                params={"query": promql.strip()},
                timeout=QUERY_TIMEOUT
            )
            resp.raise_for_status()
            data = resp.json()

            if data["status"] != "success":
                logger.error(f"Prometheus error: {data.get('error')}")
                return None

            results = data["data"]["result"]
            if not results:
                logger.debug("Prometheus returned empty result (no traffic yet?)")
                return None

            # result[0]["value"] = [timestamp, "value_string"]
            return results[0]["value"][1]

        except requests.exceptions.ConnectionError:
            logger.error(f"Cannot reach Prometheus at {self.url}")
            return None
        except requests.exceptions.Timeout:
            logger.error("Prometheus query timed out")
            return None
        except Exception as e:
            logger.error(f"Unexpected error querying Prometheus: {e}")
            return None
