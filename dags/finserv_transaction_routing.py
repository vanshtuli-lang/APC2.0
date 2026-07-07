"""
Real-Time Transaction Routing: VIP Alerts and Compliance Flagging.

Business flow:
1. Fetch the latest enriched transaction batch from Snowflake.
2. Apply priority routing rules:
   - Priority 1: suspicious account holder -> quarantine and notify Compliance/Risk.
   - Priority 2: high-value clean transaction -> notify Account Management/VIP Relations.
   - Priority 3: standard clean transaction -> load to the core production table.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, get_current_context, task
from pendulum import datetime as pendulum_datetime

SNOWFLAKE_CONN_ID = "snowflake"

TRANSACTIONS_TABLE = "FIN_SERV.OPERATIONS.CUSTOMER_TRANSACTIONS"
SUSPICIOUS_ACCOUNTS_TABLE = "FIN_SERV.REFERENCE.SUSPICIOUS_ACCOUNTS"
QUARANTINE_TABLE = "FIN_SERV.RISK.TRANSACTION_QUARANTINE"
CORE_PRODUCTION_TABLE = "FIN_SERV.CORE.APPROVED_TRANSACTIONS"

COMPLIANCE_WEBHOOK_VARIABLE = "compliance_risk_webhook_url"
VIP_WEBHOOK_VARIABLE = "vip_relations_webhook_url"


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def _normalize_records(columns: list[str], rows: list[tuple]) -> list[dict[str, Any]]:
    return [
        {column.lower(): _json_safe(value) for column, value in zip(columns, row)}
        for row in rows
    ]


def _post_webhook(variable_key: str, payload: dict[str, Any]) -> str:
    webhook_url = Variable.get(variable_key, default=None)
    if not webhook_url:
        return f"Skipped webhook; Airflow Variable {variable_key!r} is not configured."

    import requests

    response = requests.post(webhook_url, json=payload, timeout=10)
    response.raise_for_status()
    return f"Webhook delivered with HTTP {response.status_code}."


def _insert_payload_rows(table_name: str, transactions: list[dict[str, Any]], status: str) -> None:
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            transaction_id STRING,
            account_holder_name STRING,
            amount NUMBER(18, 2),
            routing_status STRING,
            routed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            payload VARIANT
        )
        """
    )

    # Batch MERGE — one round-trip for all rows; safe to re-run (idempotent on transaction_id)
    row_placeholders = ",\n            ".join(["(%s, %s, %s, %s, %s)"] * len(transactions))
    params = []
    for t in transactions:
        params.extend([
            t["transaction_id"],
            t["account_holder_name"],
            t["amount"],
            status,
            json.dumps(t),
        ])

    hook.run(
        f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT TRANSACTION_ID, ACCOUNT_HOLDER_NAME, AMOUNT, ROUTING_STATUS,
                   PARSE_JSON(PAYLOAD) AS PAYLOAD
            FROM VALUES
                {row_placeholders}
            AS source(TRANSACTION_ID, ACCOUNT_HOLDER_NAME, AMOUNT, ROUTING_STATUS, PAYLOAD)
        ) AS source
        ON target.TRANSACTION_ID = source.TRANSACTION_ID
            AND target.ROUTING_STATUS = source.ROUTING_STATUS
        WHEN MATCHED THEN UPDATE SET
            AMOUNT           = source.AMOUNT,
            ROUTING_STATUS   = source.ROUTING_STATUS,
            ROUTED_AT        = CURRENT_TIMESTAMP(),
            PAYLOAD          = source.PAYLOAD
        WHEN NOT MATCHED THEN INSERT (
            TRANSACTION_ID, ACCOUNT_HOLDER_NAME, AMOUNT, ROUTING_STATUS, PAYLOAD
        ) VALUES (
            source.TRANSACTION_ID, source.ACCOUNT_HOLDER_NAME, source.AMOUNT,
            source.ROUTING_STATUS, source.PAYLOAD
        )
        """,
        parameters=params,
    )

    print(f"Batch MERGE committed via hook.run() — {len(transactions)} rows → {table_name}")


@dag(
    dag_id="finserv_real_time_transaction_routing",
    start_date=pendulum_datetime(2026, 6, 27, tz="America/New_York"),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "finserv-operations", "retries": 2},
    tags=["finserv", "snowflake", "compliance", "vip-routing", "operations"],
    doc_md=__doc__,
    params={
        "batch_limit": 100,
        "vip_amount_threshold": 10000,
    },
)
def finserv_real_time_transaction_routing() -> None:
    @task
    def fetch_transactions() -> list[dict[str, Any]]:
        context = get_current_context()
        batch_limit = int(context["params"]["batch_limit"])

        sql = f"""
        SELECT
            t.transaction_id,
            t.account_id,
            t.account_holder_name,
            t.transaction_ts,
            t.amount,
            t.currency,
            t.merchant_name,
            COALESCE(s.flagged, 'N') AS flagged
        FROM {TRANSACTIONS_TABLE} t
        LEFT JOIN {SUSPICIOUS_ACCOUNTS_TABLE} s
            ON UPPER(TRIM(t.account_holder_name)) = UPPER(TRIM(s.account_name))
        WHERE t.transaction_ts >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY t.transaction_id
            ORDER BY t.transaction_ts DESC
        ) = 1
        ORDER BY t.transaction_ts DESC
        LIMIT {batch_limit}
        """

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(sql)
        columns = [
            "transaction_id",
            "account_id",
            "account_holder_name",
            "transaction_ts",
            "amount",
            "currency",
            "merchant_name",
            "flagged",
        ]
        return _normalize_records(columns, rows)

    @task
    def evaluate_routing_rules(
        transactions: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        context = get_current_context()
        vip_amount_threshold = Decimal(str(context["params"]["vip_amount_threshold"]))

        routes: dict[str, list[dict[str, Any]]] = {"compliance": [], "vip": [], "standard": []}
        for transaction in transactions:
            is_flagged = str(transaction.get("flagged", "N")).upper() == "Y"
            amount = Decimal(str(transaction.get("amount") or "0"))

            if is_flagged:
                routes["compliance"].append(transaction)
            elif amount > vip_amount_threshold:
                routes["vip"].append(transaction)
            else:
                routes["standard"].append(transaction)

        return routes

    @task
    def route_to_compliance(routes: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        transactions = routes["compliance"]
        if not transactions:
            return {"routed": 0, "path": "compliance"}

        _insert_payload_rows(
            table_name=QUARANTINE_TABLE,
            transactions=transactions,
            status="QUARANTINED_SUSPICIOUS_ACCOUNT",
        )
        notification_status = _post_webhook(
            COMPLIANCE_WEBHOOK_VARIABLE,
            {
                "title": "Urgent Compliance Alert: Suspicious Account Transaction",
                "priority": "P1",
                "transaction_count": len(transactions),
                "transactions": transactions,
            },
        )

        return {
            "routed": len(transactions),
            "path": "compliance",
            "notification_status": notification_status,
        }

    @task
    def route_to_vip(routes: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        transactions = routes["vip"]
        if not transactions:
            return {"routed": 0, "path": "vip"}

        vip_amount_threshold = get_current_context()["params"]["vip_amount_threshold"]
        notification_status = _post_webhook(
            VIP_WEBHOOK_VARIABLE,
            {
                "title": "High-Value Transaction Alert: VIP Service Review",
                "priority": "P2",
                "transaction_count": len(transactions),
                "threshold": vip_amount_threshold,
                "transactions": transactions,
            },
        )

        return {
            "routed": len(transactions),
            "path": "vip",
            "notification_status": notification_status,
        }

    @task
    def standard_db_load(routes: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        transactions = routes["standard"]
        if not transactions:
            return {"routed": 0, "path": "standard"}

        _insert_payload_rows(
            table_name=CORE_PRODUCTION_TABLE,
            transactions=transactions,
            status="APPROVED_STANDARD_PROCESSING",
        )

        return {"routed": len(transactions), "path": "standard"}

    routes = evaluate_routing_rules(fetch_transactions())
    route_to_compliance(routes)
    route_to_vip(routes)
    standard_db_load(routes)


finserv_real_time_transaction_routing()
