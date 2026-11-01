
#step 1
import json
import os
import random

import pendulum

from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["tutorial", "deep_dive"],
)
def order_processing_pipeline():
    """Order processing pipeline demonstrating TaskFlow, branching, and retries."""

    @task()
    def extract():
        """Extract order data (naive XCom approach for initial version)."""
        orders = {
            "1001": 301.27,
            "1002": 433.21,
            "1003": 502.22,
            "1004": 125.89,
            "1005": 678.50,
        }
        return orders

    @task(multiple_outputs=True)
    def transform(orders: dict):
        """Compute order summary statistics."""
        total_value = sum(orders.values())
        order_count = len(orders)
        return {
            "total_value": total_value,
            "order_count": order_count,
        }

    @task()
    def load(total_value: float, order_count: int):
        """Print results."""
        print(f"Total value: {total_value}, Order count: {order_count}")

    orders = extract()
    summary = transform(orders)
    load(summary["total_value"], summary["order_count"])


order_processing_pipeline()