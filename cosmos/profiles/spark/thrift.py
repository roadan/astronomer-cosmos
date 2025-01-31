"Maps Airflow Spark connections to dbt profiles if they use a thrift connection."
from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class SparkThriftProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Spark connections to dbt profiles if they use a thrift connection.
    https://docs.getdbt.com/reference/warehouse-setups/spark-setup#thrift
    https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark.html
    """

    airflow_connection_type: str = "spark"
    is_community: bool = True

    required_fields = [
        "schema",
        "host",
    ]

    airflow_param_mapping = {
        "host": "host",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """
        Return a dbt Spark profile based on the Airflow Spark connection.
        """
        profile_vars = {
            "type": "spark",
            "method": "thrift",
            "schema": self.schema,
            "host": self.host,
            "port": self.conn.port,
            **self.profile_args,
        }

        # remove any null values
        return self.filter_null(profile_vars)
