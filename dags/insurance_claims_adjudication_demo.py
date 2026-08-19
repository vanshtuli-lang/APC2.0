"""
Insurance Claims Adjudication - Stateful Tasks & Selective Retries Demo
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

DAG_DOCUMENTATION = """
### The Core Problem Solved
Traditional Airflow task retries are all-or-nothing: if a task fails at 90% completion, the retry starts over at 0%. Standard retries also retry on any error—wasting compute retrying bad data payloads that will never succeed. This pattern fixes both problems using standard Airflow building blocks.

### How the Code Works
* **Selective Retries (`AirflowFailException`)**: Transient infrastructure failures like `ConnectionError` or `TimeoutError` are left to propagate normally, so Airflow's standard `retries`/`retry_delay` handle them. If bad claim data triggers a `KeyError` or schema mismatch instead, the task catches it and raises `AirflowFailException`, which fails the task immediately (no further retries) to alert your team on Slack instead of retrying blindly.
* **Checkpointing via XCom**: XCom is only cleared on a manual "Clear and rerun," not on an automatic retry — so a value pushed to XCom before a transient failure is still there on the next retry attempt within the same DAG run.

### Step-by-Step Execution Scenario
Imagine an overnight batch run at 2:00 AM processing a batch of 500 auto claims (IDs 1,000,001 through 1,000,500):

1. **2:00 AM — Execution 1 (First Attempt)**
   * The task calls `ti.xcom_pull(key="last_processed_claim_id")`. Since it's the first run, nothing is found, so it falls back to default `1000000`.
   * The task iterates through claims, successfully sending claims #1,000,001 through #1,000,250 to a third-party DMV fraud-scoring API.
   * It pushes the XCom checkpoint: `last_processed_claim_id = 1000250`.
   * At claim #1,000,251, the external DMV API drops its connection and throws a `ConnectionError`.

2. **2:02 AM — Retry Decision**
   * The task lets the `ConnectionError` propagate uncaught. Airflow's normal `retries`/`retry_delay` config schedules a retry attempt (the exception isn't converted to `AirflowFailException`, so it isn't treated as a fatal, fail-fast error).

3. **2:05 AM — Execution 2 (Retry Attempt)**
   * The retried task starts up and calls `ti.xcom_pull(key="last_processed_claim_id")` again — this XCom value survived the retry.
   * Instead of starting from scratch at claim #1,000,001, it reads **`1000250`**.
   * It skips the first 250 claims completely and resumes API processing directly at claim **#1,000,251**.

### Business Impact to Highlight in Your Demo
* **Cost Prevention**: Avoid re-querying or paying external per-call API fees for the 250 claims already processed.
* **SLA Protection**: A 2-hour long ingestion job that drops connection at 1 hour and 50 minutes takes 10 minutes to finish on retry instead of another full 2 hours.
"""


@dag(
    dag_id="insurance_claims_adjudication_demo",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md=DAG_DOCUMENTATION,
    tags=["insurance", "claims", "stateful", "demo"],
)
def insurance_claims_adjudication():

    # Retry transient third-party underwriting API drops, fail fast on bad claim schemas
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def evaluate_claims_batch(**context):
        ti = context["ti"]

        # Pull last successfully processed claim ID from XCom (survives automatic retries)
        last_processed_claim = ti.xcom_pull(key="last_processed_claim_id") or 1000000

        print(f"Resuming overnight claims batch from Claim ID: {last_processed_claim}")

        # Simulated batch of incoming auto/property claims
        claims_batch = [
            {"claim_id": last_processed_claim + i, "policy_type": "AUTO", "amount": 1450.00}
            for i in range(1, 501)
        ]

        try:
            for claim in claims_batch:
                # Process claim adjudication logic / third-party scoring API call...
                pass
        except (KeyError, ValueError) as exc:
            # Bad claim data / schema mismatch: fail fast, don't waste retries on it
            raise AirflowFailException(f"Unrecoverable claim data error: {exc}")
        # ConnectionError / TimeoutError are left uncaught so Airflow's normal retry kicks in

        # Save checkpoint to XCom before finishing or if the next batch fails
        latest_checkpoint = claims_batch[-1]["claim_id"]
        ti.xcom_push(key="last_processed_claim_id", value=latest_checkpoint)
        print(f"Batch complete. Saved checkpoint at Claim ID: {latest_checkpoint}")

    evaluate_claims_batch()


insurance_claims_adjudication()