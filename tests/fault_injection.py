"""
Fault Injection Script
======================
Dissertation experiments ke liye — controller ko trigger karo aur MTTR measure karo.

Run karo:
    python fault_injection.py --experiment E1
    python fault_injection.py --experiment E2
    python fault_injection.py --experiment E3
"""

import argparse
import subprocess
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Experiment Configs ────────────────────────────────────────────────────
EXPERIMENTS = {
    "E1": {
        "name": "High Error Rate (50% 500s)",
        "description": "50% requests 500 return karenge — rollback expected",
        "fault_yaml": """
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: myapp
spec:
  hosts:
  - myapp
  http:
  - fault:
      abort:
        percentage:
          value: 50.0
        httpStatus: 500
    route:
      - destination:
          host: myapp
          subset: canary
        weight: 100
      - destination:
          host: myapp
          subset: stable
        weight: 0
""",
    },
    "E2": {
        "name": "High Latency (2s delay)",
        "description": "Sab requests mein 2s delay — latency rollback expected",
        "fault_yaml": """
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: myapp
spec:
  hosts:
  - myapp
  http:
  - fault:
      delay:
        percentage:
          value: 100.0
        fixedDelay: 2s
    route:
      - destination:
          host: myapp
          subset: canary
        weight: 100
      - destination:
          host: myapp
          subset: stable
        weight: 0
""",
    },
    "E3": {
        "name": "Healthy Canary (no fault)",
        "description": "Koi fault nahi — full promotion expected (10→100%)",
        "fault_yaml": None,   # no fault injection
    },
}


def kubectl_apply_inline(yaml_str: str):
    """YAML string ko kubectl apply karo."""
    proc = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=yaml_str.encode(),
        capture_output=True
    )
    if proc.returncode != 0:
        logger.error(f"kubectl error: {proc.stderr.decode()}")
    return proc.returncode == 0


def kubectl_delete_fault():
    """Fault VirtualService hata do, original restore karo."""
    restore_yaml = """
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: myapp
spec:
  hosts:
  - myapp
  http:
  - route:
      - destination:
          host: myapp
          subset: stable
        weight: 100
      - destination:
          host: myapp
          subset: canary
        weight: 0
"""
    kubectl_apply_inline(restore_yaml)
    logger.info("✅  Fault removed, VirtualService restored.")


def run_experiment(exp_id: str):
    exp = EXPERIMENTS.get(exp_id)
    if not exp:
        logger.error(f"Unknown experiment: {exp_id}. Choices: {list(EXPERIMENTS.keys())}")
        return

    logger.info(f"\n{'='*55}")
    logger.info(f"  Experiment {exp_id}: {exp['name']}")
    logger.info(f"  {exp['description']}")
    logger.info(f"{'='*55}\n")

    start_time = time.time()

    # Fault inject karo
    if exp["fault_yaml"]:
        logger.info("💥  Injecting fault...")
        kubectl_apply_inline(exp["fault_yaml"])
        logger.info("⏱   Watching for controller rollback... (check controller logs)")
        logger.info("    kubectl logs -f deployment/canary-controller")
    else:
        logger.info("✅  No fault — watching for healthy promotion...")
        logger.info("    kubectl logs -f deployment/canary-controller")

    # Wait and measure
    input("\nPress ENTER jab controller ka action complete ho (rollback/promotion)...")
    elapsed = time.time() - start_time

    logger.info(f"\n📊  MTTR / Promotion Time: {elapsed:.1f} seconds")
    logger.info("    Yeh value apni dissertation table mein note kar lo!")

    # Cleanup
    if exp["fault_yaml"]:
        cleanup = input("\nFault remove karein? (y/n): ")
        if cleanup.lower() == "y":
            kubectl_delete_fault()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dissertation Fault Injection")
    parser.add_argument(
        "--experiment", choices=list(EXPERIMENTS.keys()),
        default="E1", help="Experiment ID (E1/E2/E3)"
    )
    args = parser.parse_args()
    run_experiment(args.experiment)
