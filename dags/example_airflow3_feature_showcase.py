"""
## Airflow 3 Feature Showcase: SpaceX Launch Pipeline

A tour of headline Airflow 3 capabilities, built around a small, self-contained
SpaceX launch pipeline. No connections or credentials required.

Features demonstrated:

- **TaskFlow API** (`@task`) for writing plain Python functions as tasks.
- **Assets** (`Asset`) for data-aware scheduling — the `example_airflow3_consumer`
  DAG below is triggered automatically whenever this DAG updates the asset,
  instead of running on its own schedule.
- **Dynamic task mapping** (`.expand()`) to fan out one task per launch, with the
  number of mapped tasks decided at runtime.
- **`@task.branch`** for TaskFlow-native conditional routing (no separate
  `BranchPythonOperator` needed).
- **Setup/teardown tasks** (`.as_setup()` / `.as_teardown()`) to scope a resource
  to the tasks that use it, with guaranteed cleanup even on failure.
- **Typed `Param`s** so the DAG can be triggered from the UI with a validated,
  self-documenting input form.

For more, see: https://www.astronomer.io/docs/learn/airflow-3-overview
"""

from __future__ import annotations

from airflow.sdk import Asset, Param, dag, get_current_context, task
from pendulum import datetime
import requests

UPCOMING_LAUNCHES_ASSET = Asset("spacex_upcoming_launches")


@dag(
    dag_id="example_airflow3_producer",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 2},
    tags=["example", "airflow3"],
    params={
        "max_launches": Param(
            5,
            type="integer",
            minimum=1,
            maximum=20,
            title="Max launches to process",
            description="How many upcoming SpaceX launches to pull and process this run.",
        ),
    },
)
def example_airflow3_producer():
    @task
    def create_scratch_space() -> str:
        """Setup task: stands up a resource scoped to this run."""
        print("Creating scratch space for this run...")
        return "scratch-space-ready"

    @task
    def drop_scratch_space(scratch_space: str) -> None:
        """Teardown task: always runs to clean up, even if a task above fails."""
        print(f"Tearing down {scratch_space}...")

    @task(outlets=[UPCOMING_LAUNCHES_ASSET])
    def get_upcoming_launches() -> list[dict]:
        """Fetch upcoming SpaceX launches. Updating this Asset triggers the consumer DAG."""
        context = get_current_context()
        max_launches = context["params"]["max_launches"]

        try:
            response = requests.get("https://api.spacexdata.com/v4/launches/upcoming", timeout=10)
            response.raise_for_status()
            launches = [
                {
                    "name": launch["name"],
                    "rocket_type": "Falcon Heavy" if launch.get("flight_number", 0) % 5 == 0 else "Falcon 9",
                    "flight_number": launch["flight_number"],
                }
                for launch in response.json()
            ]
        except Exception:
            print("SpaceX API unavailable, using hardcoded fallback data.")
            launches = [
                {"name": "Starlink Group 10-1", "rocket_type": "Falcon 9", "flight_number": 421},
                {"name": "Crew-12", "rocket_type": "Falcon 9", "flight_number": 422},
                {"name": "Europa Clipper Resupply", "rocket_type": "Falcon Heavy", "flight_number": 425},
            ]

        return launches[:max_launches]

    @task.branch
    def check_for_heavy_launches(launches: list[dict]) -> str:
        """TaskFlow-native branching: route based on whether a Falcon Heavy is in the batch."""
        has_heavy_launch = any(launch["rocket_type"] == "Falcon Heavy" for launch in launches)
        return "notify_heavy_launch" if has_heavy_launch else "log_standard_launches"

    @task
    def notify_heavy_launch(launches: list[dict]) -> None:
        heavy_launches = [launch["name"] for launch in launches if launch["rocket_type"] == "Falcon Heavy"]
        print(f"Falcon Heavy launch(es) detected, paging launch ops: {heavy_launches}")

    @task
    def log_standard_launches(launches: list[dict]) -> None:
        print(f"No Falcon Heavy launches this run — logging {len(launches)} standard launch(es).")

    @task
    def summarize_launch(launch: dict) -> str:
        """Dynamically mapped: one task instance per launch, decided at runtime."""
        summary = f"{launch['name']} ({launch['rocket_type']}, flight #{launch['flight_number']})"
        print(summary)
        return summary

    scratch_space = create_scratch_space()
    launches = get_upcoming_launches()
    branch = check_for_heavy_launches(launches)
    heavy_path = notify_heavy_launch(launches)
    standard_path = log_standard_launches(launches)
    summaries = summarize_launch.expand(launch=launches)

    scratch_space >> launches >> branch >> [heavy_path, standard_path]
    scratch_space.as_setup()
    drop_task = drop_scratch_space(scratch_space)
    drop_task.as_teardown(setups=scratch_space)
    [heavy_path, standard_path, summaries] >> drop_task


@dag(
    dag_id="example_airflow3_consumer",
    start_date=datetime(2026, 1, 1),
    schedule=[UPCOMING_LAUNCHES_ASSET],
    catchup=False,
    doc_md="Runs automatically whenever `example_airflow3_producer` updates the "
    "`spacex_upcoming_launches` Asset — no polling, no fixed schedule.",
    default_args={"owner": "Astro", "retries": 2},
    tags=["example", "airflow3"],
)
def example_airflow3_consumer():
    @task
    def report_new_launch_data() -> None:
        print("New upcoming-launch data is available — kicking off downstream reporting.")

    report_new_launch_data()


example_airflow3_producer()
example_airflow3_consumer()
