"Maps Airflow Postgres connections using user + password authentication to dbt profiles."
from __future__ import annotations

from typing import Any

from ..base import BaseProfileMapping


class ClickhouseUserPasswordProfileMapping(BaseProfileMapping):
    """
    Maps Airflow generic connections using user + password authentication to dbt Clickhouse profiles.
    https://docs.getdbt.com/docs/core/connect-data-platform/clickhouse-setup
    """

    airflow_connection_type: str = "generic"
    default_port = 8123

    required_fields = [
        "host",
        "user",
        "password",
        "port",
        # "dbname",
        "schema",
    ]
    secret_fields = [
        "password",
    ]
    airflow_param_mapping = {
        "host": "host",
        "user": "login",
        "password": "password",
        "port": "port",
        "schema": "schema",
        # "keepalives_idle": "extra.keepalives_idle",
        # "sslmode": "extra.sslmode",
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        "Gets profile. The password is stored in an environment variable."
        profile = {
            "type": "clickhouse",
            "schema": self.conn.schema,
            "user": self.conn.login,
            "password": self.get_env_var_format("password"),
            "driver": "http",
            "port": self.conn.port or self.default_port,
            "host": self.conn.host,
            "secure": False,
            "keepalives_idle": self.conn.extra_dejson.get("keepalives_idle"),
            "sslmode": self.conn.extra_dejson.get("sslmode"),
            **self.profile_args,
            # password should always get set as env var
            
        }

        return self.filter_null(profile)
