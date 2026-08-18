"""
Dynamic Task Mapping Demo - Regional Hospital EHR & Lab Ingestion
"""

import random
from datetime import datetime
from airflow.decorators import dag, task

DAG_DOCUMENTATION = """
### Real-World Healthcare Use Case: Multi-Facility EHR & Lab Ingestion
Large health systems operate dozens of hospitals, outpatient clinics, and diagnostic labs. Overnight, each facility exports FHIR/HL7 patient updates and lab results to database staging tables for central clinical data warehousing and analytics.

### The Core Problem Solved
* Old Way (Fixed Topologies):** Data teams either hardcoded 50 tasks for every clinic (causing UI clutter and executing empty worker tasks when a clinic had 0 updates) or processed all facilities sequentially in one giant loop (causing 4-hour delays for morning clinical dashboards).
* Airflow Dynamic Task Mapping:** The pipeline queries the staging database at runtime to detect active facility batches. Airflow then instantly spawns dedicated, parallel tasks in the Astro UI—exactly 1 task per active hospital facility.

### How the Code Works
1. `query_pending_facility_batches()`**: Mocks a database staging query that detects between 1 and 10 active hospital facility batches awaiting ingestion.
2. `.expand()`**: Dynamically clones the `process_facility_records` task across worker nodes for every facility returned by the DB query.
3. Task Reduction**: The `aggregate_ingestion_summary` task collates all dynamic task outputs into a single audit log for HIPAA compliance monitoring.
"""


@dag(
    dag_id="healthcare_ehr_ingestion_demo",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md=DAG_DOCUMENTATION,
    tags=["healthcare", "ehr", "dynamic_mapping", "taskflow", "demo"],
)
def healthcare_ehr_ingestion():

    @task
    def query_pending_facility_batches() -> list[dict]:
        """Mock DB call: Returns a variable number of pending hospital facility batches (1 to 10)."""
        facilities = [
            "FACILITY_BOSTON_MAIN",
            "FACILITY_CAMBRIDGE_CLINIC",
            "FACILITY_WORCESTER_HEALTH",
            "FACILITY_SPRINGFIELD_MED",
            "FACILITY_NEWTON_PEDIATRICS",
            "FACILITY_QUINCY_URGENT",
            "FACILITY_LOWELL_REHAB",
            "FACILITY_AMHERST_CENTER",
            "FACILITY_PLYMOUTH_CARE",
            "FACILITY_CAPE_COMMUNITY",
        ]

        # Simulates nightly variation where 1 to 10 facilities have pending updates
        active_count = random.randint(1, 10)
        selected_facilities = facilities[:active_count]

        print(f"Staging DB query returned {active_count} active facility batches requiring ingestion.")

        return [
            {
                "facility_id": facility,
                "patient_record_count": random.randint(500, 5000),
                "lab_result_count": random.randint(200, 1500),
            }
            for facility in selected_facilities
        ]

    @task
    def process_facility_records(facility_info: dict) -> dict:
        """Mapped Task: Executes independently on worker nodes for each active facility."""
        facility = facility_info["facility_id"]
        records = facility_info["patient_record_count"]
        labs = facility_info["lab_result_count"]

        print(f"Ingesting {records} EHR records and {labs} lab results for {facility}")
        # ... HL7/FHIR parsing, HIPAA validation, and loading into Snowflake/Databricks ...

        return {
            "facility": facility,
            "records_processed": records,
            "labs_processed": labs,
            "status": "INGESTED",
        }

    @task
    def aggregate_ingestion_summary(facility_outputs: list[dict]):
        """Reduces all mapped task outputs into a single compliance audit record."""
        total_records = sum(item["records_processed"] for item in facility_outputs)
        total_labs = sum(item["labs_processed"] for item in facility_outputs)
        facilities_processed = [item["facility"] for item in facility_outputs]

        print("Nightly EHR Ingestion Complete.")
        print(f"Facilities Processed ({len(facilities_processed)}): {', '.join(facilities_processed)}")
        print(f"Total Patient Records Ingested: {total_records}")
        print(f"Total Lab Results Ingested: {total_labs}")

    # Pipeline Flow
    pending_batches = query_pending_facility_batches()

    # Dynamic Task Mapping occurs here via .expand()
    mapped_facility_tasks = process_facility_records.expand(facility_info=pending_batches)

    aggregate_ingestion_summary(mapped_facility_tasks)


healthcare_ehr_ingestion()