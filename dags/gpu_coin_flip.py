"""
GPU Coin Flip Demo
Financial Services Demo | APC 2.0 + Airflow 3.x

What this does:
  Provisions a GPU node on demand, flips 1,000,000 coins on the GPU,
  files a summary report, then deprovisions the node.

Pipeline:
  [setup] provision_gpu_node
       → run_coin_flip
       → file_report
  [teardown] deprovision_gpu_node  ← always runs, even on failure
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag, task
from kubernetes.client import models as k8s

# GPU config
GPU_ASG_NAME = "eks-gpu-nodes-82cff5dd-545e-6906-54fe-782a969ffabd"
AWS_REGION   = "us-east-1"


@dag(
    dag_id="gpu_coin_flip",
    start_date=datetime(2024, 1, 1),
    schedule=None,       # trigger manually from the Airflow UI for demo
    catchup=False,
    tags=["gpu", "demo", "coin-flip"],
)
def gpu_coin_flip():

    # provision GPU
    # Scales the GPU node group from 0 → 1 and waits until the node is Ready.
    @task
    def provision_gpu_node() -> None:
        import boto3, time
        from kubernetes import client as k8s_client, config as k8s_config

        print(f"Scaling GPU node group to 1...")
        boto3.client("autoscaling", region_name=AWS_REGION).set_desired_capacity(
            AutoScalingGroupName=GPU_ASG_NAME,
            DesiredCapacity=1,
        )

        k8s_config.load_incluster_config()
        v1 = k8s_client.CoreV1Api()

        print("Waiting for GPU node to be Ready (up to 8 min)...")
        for elapsed in range(48):
            nodes = v1.list_node(label_selector="workload=gpu")
            ready = [n for n in nodes.items
                     if any(c.type == "Ready" and c.status == "True"
                            for c in n.status.conditions)]
            if ready:
                print(f"GPU node ready: {ready[0].metadata.name}")
                return
            print(f"  Waiting... ({elapsed * 10}s)")
            time.sleep(10)

        raise Exception("GPU node did not become Ready within 8 minutes")

    # Teardown GPU to avoid costs
    # Always runs — even if the coin flip fails. GPU never left idle.
    @task
    def deprovision_gpu_node() -> None:
        import boto3
        boto3.client("autoscaling", region_name=AWS_REGION).set_desired_capacity(
            AutoScalingGroupName=GPU_ASG_NAME,
            DesiredCapacity=0,
        )
        print("GPU node scaled to 0 — no idle GPU costs")

    # Coin flip code - non AI/ML
    run_coin_flip = KubernetesPodOperator(
        task_id="run_coin_flip",
        name="gpu-coin-flip",
        namespace="astronomer-universal-asteroid-7914",
        image="pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime",
        cmds=["python", "-c"],
        arguments=["""
import torch, time, json, os

print("=" * 58)
print("  GPU COIN FLIP")
print("=" * 58)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"  Device : {device}")
if torch.cuda.is_available():
    print(f"  GPU    : {torch.cuda.get_device_name(0)}")
print("=" * 58)

N = 1_000_000

torch.cuda.synchronize()
t0 = time.time()

flips = torch.randint(0, 2, (N,), device=device)
heads = int((flips == 1).sum())
tails = int((flips == 0).sum())

torch.cuda.synchronize()
elapsed = time.time() - t0

print(f"\\nFlipped {N:,} coins in {elapsed:.4f}s")
print(f"  Heads : {heads:,}")
print(f"  Tails : {tails:,}")

os.makedirs("/airflow/xcom", exist_ok=True)
with open("/airflow/xcom/return.json", "w") as f:
    json.dump({
        "heads": heads,
        "tails": tails,
        "n": N,
        "elapsed_sec": round(elapsed, 4),
    }, f)
print("\\nResults written to XCom.")
"""],
        node_selector={"workload": "gpu"},
        tolerations=[
            k8s.V1Toleration(key="nvidia.com/gpu", operator="Equal",
                             value="true", effect="NoSchedule")
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"nvidia.com/gpu": "1", "cpu": "2", "memory": "8Gi"},
            limits={"nvidia.com/gpu": "1", "cpu": "3", "memory": "12Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        do_xcom_push=True,
        startup_timeout_seconds=300,
    )

    # Log stuff
    @task
    def file_report(result: dict) -> None:
        n     = result["n"]
        heads = result["heads"]
        tails = result["tails"]
        print("Coin flip report")
        print(f"  Total flips : {n:,}")
        print(f"  Heads       : {heads:,} ({heads / n * 100:.2f}%)")
        print(f"  Tails       : {tails:,} ({tails / n * 100:.2f}%)")
        print(f"  GPU time    : {result['elapsed_sec']}s")

    # dependency
    setup    = provision_gpu_node()
    teardown = deprovision_gpu_node()
    report   = file_report(run_coin_flip.output)

    setup.as_setup()
    teardown.as_teardown(setups=setup)

    setup >> run_coin_flip >> report >> teardown


gpu_coin_flip()
