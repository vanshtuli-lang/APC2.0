"""
Insurance Claims Adjudication - Stateful Tasks & Selective Retries Demo
"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.policies import RetryOnExceptionPolicy

DAG_DOCUMENTATION = """
### The Core Problem Solved
Traditional Airflow task retries are all-or-nothing: if a task fails at 90% completion, the retry starts over at 0%. Standard retries also retry on any error—wasting compute retrying bad data payloads that will never succeed. This Airflow 3.3 pattern fixes both problems.

### How the Code Works
* **Selective Retries (`RetryOnExceptionPolicy`)**: Tells Airflow to retry only on transient infrastructure failures like `ConnectionError` or `TimeoutError`. If bad claim data triggers a `KeyError` or schema mismatch, the task fails fast immediately to alert your team on Slack instead of retrying blindly.
* **Task State Store (`context["task_instance"].get_state_store()`)**: Serves as an isolated key-value store tied to that exact task execution. Unlike standard XComs (which are wiped clean when a task retries), Task State Store key-value pairs survive task retries and worker crashes within the same DAG run.

### Step-by-Step Execution Scenario
Imagine an overnight batch run at 2:00 AM processing a batch of 500 auto claims (IDs 1,000,001 through 1,000,500):

1. **2:00 AM — Execution 1 (First Attempt)**
   * The task checks `state_store.get("last_processed_claim_id")`. Since it's the first run, nothing is found, so it falls back to default `1000000`.
   * The task iterates through claims, successfully sending claims #1,000,001 through #1,000,250 to a third-party DMV fraud-scoring API.
   * It updates the state store checkpoint: `last_processed_claim_id = 1000250`.
   * At claim #1,000,251, the external DMV API drops its connection and throws a `ConnectionError`.

2. **2:02 AM — Retry Decision**
   * Airflow catches the exception. Because `ConnectionError` matches the `RetryOnExceptionPolicy` list, Airflow schedules a retry attempt.

3. **2:05 AM — Execution 2 (Retry Attempt)**
   * The retried task starts up and calls `state_store.get("last_processed_claim_id")`.
   * Instead of starting from scratch at claim #1,000,001, it reads **`1000250`** from the state store.
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
    @task(retry_policy=RetryOnExceptionPolicy(exceptions=[ConnectionError, TimeoutError]))
    def evaluate_claims_batch(**context):
        state_store = context["task_instance"].get_state_store()

        # Pull last successfully processed claim ID from Task State Store
        last_processed_claim = state_store.get("last_processed_claim_id") or 1000000

        print(f"Resuming overnight claims batch from Claim ID: {last_processed_claim}")

        # Simulated batch of incoming auto/property claims
        claims_batch = [
            {"claim_id": last_processed_claim + i, "policy_type": "AUTO", "amount": 1450.00}
            for i in range(1, 501)
        ]

        for claim in claims_batch:
            # Process claim adjudication logic / third-party scoring API call...
            pass

        # Save checkpoint to state store before finishing or if next batch fails
        latest_checkpoint = claims_batch[-1]["claim_id"]
        state_store.set("last_processed_claim_id", latest_checkpoint)
        print(f"Batch complete. Saved state checkpoint at Claim ID: {latest_checkpoint}")

    evaluate_claims_batch()


insurance_claims_adjudication()