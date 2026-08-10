"""
Portfolio Risk Simulation Pipeline
Financial Services Demo | APC 2.0 + Airflow 3.x

What this does:
  Provisions a GPU node on demand, runs 1,000,000 Monte Carlo portfolio
  simulations in seconds, files a risk report, then deprovisions the node.
  The same job on a CPU takes ~15 minutes. On an NVIDIA T4: ~6 seconds.

Use case:
  Risk teams at banks and asset managers run Monte Carlo simulations daily
  to estimate Value at Risk (VaR) — the max loss expected under normal
  market conditions. GPU acceleration makes millions of paths feasible in
  real time, at a fraction of the cost of always-on GPU infrastructure.

Pipeline:
  [setup] provision_gpu_node
       → prepare_portfolio
       → run_monte_carlo_on_gpu
       → risk_gate
       → file_report / raise_alert
  [teardown] deprovision_gpu_node  ← always runs, even on failure

Airflow 3.x features:
  TaskFlow API | Setup/Teardown | Assets | @task.branch | KubernetesPodOperator
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Asset, dag, task
from kubernetes.client import models as k8s


# ── Assets ────────────────────────────────────────────────────────────────────
MARKET_DATA = Asset("s3://finserv-datalake/market-data/daily/")
RISK_REPORT = Asset("s3://finserv-reports/portfolio-risk/latest/")

# ── GPU Node Config ───────────────────────────────────────────────────────────
GPU_ASG_NAME = "eks-gpu-nodes-82cff5dd-545e-6906-54fe-782a969ffabd"
AWS_REGION   = "us-east-1"

# Alert if 95% VaR exceeds this % of portfolio value
VAR_ALERT_THRESHOLD_PCT = 8.0


@dag(
    dag_id="portfolio_risk_simulation",
    start_date=datetime(2024, 1, 1),
    schedule=None,       # trigger manually from the Airflow UI for demo
    catchup=False,
    tags=["risk", "gpu", "monte-carlo", "finserv"],
)
def portfolio_risk_simulation():

    # ── Setup: Provision GPU Node ─────────────────────────────────────────────
    # Scales the GPU node group from 0 → 1 and waits until the node is Ready.
    # Runs before everything else. If it fails, the pipeline is skipped cleanly.
    # The paired teardown always runs at the end — GPU never left idle.
    @task.setup
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

    # ── Teardown: Deprovision GPU Node ────────────────────────────────────────
    # Always runs — even if training or evaluation fails.
    # Guarantees the GPU node is never left running idle.
    @task.teardown
    def deprovision_gpu_node() -> None:
        import boto3
        boto3.client("autoscaling", region_name=AWS_REGION).set_desired_capacity(
            AutoScalingGroupName=GPU_ASG_NAME,
            DesiredCapacity=0,
        )
        print("GPU node scaled to 0 — no idle GPU costs")

    # ── Task 1: Define Portfolio ──────────────────────────────────────────────
    # In production: pull live positions from the portfolio management system.
    @task(inlets=[MARKET_DATA])
    def prepare_portfolio() -> dict:
        portfolio = {
            "name": "FinServ Core Fund",
            "value_usd": 10_000_000,
            "holdings": {
                "JPM": {"weight": 0.30, "annual_return": 0.12, "annual_vol": 0.22},
                "GS":  {"weight": 0.20, "annual_return": 0.15, "annual_vol": 0.28},
                "BAC": {"weight": 0.20, "annual_return": 0.09, "annual_vol": 0.25},
                "V":   {"weight": 0.15, "annual_return": 0.18, "annual_vol": 0.20},
                "MA":  {"weight": 0.15, "annual_return": 0.20, "annual_vol": 0.22},
            },
            "time_horizon_days": 252,
            "n_simulations": 1_000_000,
        }

        print(f"Portfolio : {portfolio['name']}  |  Value: ${portfolio['value_usd']:,}")
        for ticker, pos in portfolio["holdings"].items():
            print(f"  {ticker:4s}  weight={pos['weight']*100:.0f}%  "
                  f"return={pos['annual_return']*100:.0f}%  "
                  f"vol={pos['annual_vol']*100:.0f}%")
        print(f"\nRunning {portfolio['n_simulations']:,} simulations "
              f"over {portfolio['time_horizon_days']} trading days")
        return portfolio

    # ── Task 2: Monte Carlo on GPU ────────────────────────────────────────────
    run_monte_carlo = KubernetesPodOperator(
        task_id="run_monte_carlo_on_gpu",
        name="portfolio-risk-simulation",
        namespace="astronomer-universal-asteroid-7914",
        image="pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime",
        cmds=["python", "-c"],
        arguments=["""
import torch, numpy as np, time, json, os

print("=" * 58)
print("  PORTFOLIO RISK SIMULATION — GPU POD")
print("=" * 58)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"  Device : {device}")
if torch.cuda.is_available():
    print(f"  GPU    : {torch.cuda.get_device_name(0)}")
    print(f"  VRAM   : {torch.cuda.get_device_properties(0).total_memory/1e9:.1f} GB")
print("=" * 58)

# Portfolio parameters
tickers    = ["JPM",  "GS",   "BAC",  "V",    "MA"  ]
weights    = [0.30,   0.20,   0.20,   0.15,   0.15  ]
annual_ret = [0.12,   0.15,   0.09,   0.18,   0.20  ]
annual_vol = [0.22,   0.28,   0.25,   0.20,   0.22  ]
PORT_VAL   = 10_000_000
N_SIM      = 1_000_000
N_DAYS     = 252

w     = torch.tensor(weights, device=device)
mu    = torch.tensor([r / N_DAYS for r in annual_ret], device=device)
sigma = torch.tensor([v / N_DAYS**0.5 for v in annual_vol], device=device)

# CPU baseline — 10k paths so we can show the speedup live
print("\\nCPU baseline (10k paths)...")
t0 = time.time()
Z_cpu   = np.random.randn(10_000, N_DAYS, len(tickers))
r_cpu   = np.array(annual_ret)/N_DAYS + np.array(annual_vol)/N_DAYS**0.5 * Z_cpu
pf_cpu  = (r_cpu * weights).sum(axis=-1)
_       = PORT_VAL * np.cumprod(1 + pf_cpu, axis=1)[:, -1]
cpu_10k = time.time() - t0
cpu_est = cpu_10k * 100
print(f"  10k paths : {cpu_10k:.2f}s  →  1M paths est: {cpu_est:.0f}s on CPU")

# GPU — 1M paths
print(f"\\nGPU: running {N_SIM:,} paths...")
torch.cuda.synchronize()
t0 = time.time()

Z          = torch.randn(N_SIM, N_DAYS, len(tickers), device=device)
daily_ret  = mu + sigma * Z
port_daily = (daily_ret * w).sum(dim=-1)
port_paths = PORT_VAL * (1 + port_daily).cumprod(dim=1)
final_vals = port_paths[:, -1]
pct_ret    = (final_vals - PORT_VAL) / PORT_VAL

torch.cuda.synchronize()
gpu_time   = time.time() - t0

print(f"  {N_SIM:,} paths : {gpu_time:.2f}s")
print(f"  Speedup        : {cpu_est/gpu_time:.0f}x faster than CPU")

# Risk metrics
var_95_pct = float(torch.quantile(pct_ret, 0.05)) * 100
var_99_pct = float(torch.quantile(pct_ret, 0.01)) * 100
var_95_usd = abs(var_95_pct / 100 * PORT_VAL)
exp_return = float(pct_ret.mean()) * 100
prob_loss  = float((pct_ret < 0).float().mean()) * 100
sharpe     = float(pct_ret.mean() / pct_ret.std()) * (252**0.5)

print("\\n" + "=" * 58)
print("  RISK METRICS  (1,000,000 Monte Carlo scenarios)")
print("=" * 58)
print(f"  Expected Annual Return : {exp_return:+.2f}%")
print(f"  Sharpe Ratio           : {sharpe:.2f}")
print(f"  Probability of Loss    : {prob_loss:.1f}%")
print(f"  VaR 95% (1yr)          : -{abs(var_95_pct):.2f}%  (${var_95_usd:>10,.0f})")
print(f"  VaR 99% (1yr)          : -{abs(var_99_pct):.2f}%  (${abs(var_99_pct/100*PORT_VAL):>10,.0f})")
print("=" * 58)
print(f"  In 95% of scenarios the $10M fund does not")
print(f"  lose more than ${var_95_usd:,.0f} over one year.")

os.makedirs("/airflow/xcom", exist_ok=True)
with open("/airflow/xcom/return.json", "w") as f:
    json.dump({
        "var_95_pct":      round(abs(var_95_pct), 2),
        "var_99_pct":      round(abs(var_99_pct), 2),
        "var_95_usd":      round(var_95_usd, 0),
        "expected_return": round(exp_return, 2),
        "sharpe_ratio":    round(sharpe, 2),
        "prob_loss_pct":   round(prob_loss, 1),
        "gpu_time_sec":    round(gpu_time, 2),
        "speedup_vs_cpu":  round(cpu_est / gpu_time, 0),
        "n_simulations":   N_SIM,
    }, f)
print("\\nMetrics written to XCom.")
"""],
        node_selector={"workload": "gpu"},
        tolerations=[
            k8s.V1Toleration(key="nvidia.com/gpu", operator="Equal",
                             value="true", effect="NoSchedule")
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"nvidia.com/gpu": "1", "cpu": "2", "memory": "8Gi"},
            limits={"nvidia.com/gpu": "1", "cpu": "4", "memory": "14Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        do_xcom_push=True,
        startup_timeout_seconds=300,
    )

    # ── Task 3: Risk Gate ─────────────────────────────────────────────────────
    @task.branch
    def risk_gate(metrics: dict) -> str:
        var = metrics["var_95_pct"]
        print(f"VaR 95% : {var:.2f}%  |  Threshold: {VAR_ALERT_THRESHOLD_PCT}%")
        if var > VAR_ALERT_THRESHOLD_PCT:
            print("VaR exceeds threshold — raising alert")
            return "raise_alert"
        print("VaR within limits — filing report")
        return "file_report"

    # ── Task 4a: File Report ──────────────────────────────────────────────────
    @task(outlets=[RISK_REPORT])
    def file_report(metrics: dict) -> None:
        print("Filing daily risk report...")
        print(f"  Portfolio       : FinServ Core Fund ($10,000,000)")
        print(f"  Simulations     : {metrics['n_simulations']:,} in {metrics['gpu_time_sec']}s")
        print(f"  GPU speedup     : {metrics['speedup_vs_cpu']:.0f}x vs CPU")
        print(f"  Expected Return : +{metrics['expected_return']:.2f}%")
        print(f"  Sharpe Ratio    : {metrics['sharpe_ratio']:.2f}")
        print(f"  VaR 95% (1yr)   : {metrics['var_95_pct']:.2f}%  (${metrics['var_95_usd']:,.0f})")
        print("Report saved — downstream compliance DAG will trigger")

    # ── Task 4b: Raise Alert ──────────────────────────────────────────────────
    @task
    def raise_alert(metrics: dict) -> None:
        print("RISK ALERT: VaR exceeds acceptable threshold")
        print(f"  VaR 95%  : {metrics['var_95_pct']:.2f}%  (limit: {VAR_ALERT_THRESHOLD_PCT}%)")
        print(f"  USD at risk: ${metrics['var_95_usd']:,.0f}")
        print("  Notifying risk committee + flagging for rebalancing")

    # ── Wire ──────────────────────────────────────────────────────────────────
    setup    = provision_gpu_node()
    teardown = deprovision_gpu_node()
    portfolio = prepare_portfolio()
    gate     = risk_gate(run_monte_carlo.output)
    report   = file_report(run_monte_carlo.output)
    alert    = raise_alert(run_monte_carlo.output)

    setup >> portfolio >> run_monte_carlo >> gate >> [report, alert] >> teardown


portfolio_risk_simulation()
