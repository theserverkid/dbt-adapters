import pytest

from dbt.adapters.iceberg.connections import SparkConnectionMethod, SparkCredentials
from dbt_common.exceptions import DbtRuntimeError


def test_credentials_server_side_parameters_keys_and_values_are_strings() -> None:
    credentials = SparkCredentials(
        host="localhost",
        method=SparkConnectionMethod.THRIFT,  # type:ignore
        database="tests",
        schema="tests",
        server_side_parameters={"spark.configuration": "10"},
    )
    assert credentials.server_side_parameters["spark.configuration"] == "10"


def test_credentials_server_side_parameters_ansi_disabled_cannot_be_overridden() -> None:
    credentials = SparkCredentials(
        host="localhost",
        method=SparkConnectionMethod.THRIFT,  # type:ignore
        database="tests",
        schema="tests",
        server_side_parameters={"spark.sql.ansi.enabled": "true"},
    )
    assert credentials.server_side_parameters["spark.sql.ansi.enabled"] == "false"


def test_credentials_server_side_parameters_ansi_disabled_default() -> None:
    credentials = SparkCredentials(
        host="localhost",
        method=SparkConnectionMethod.THRIFT,  # type:ignore
        database="tests",
        schema="tests",
    )
    assert credentials.server_side_parameters["spark.sql.ansi.enabled"] == "false"


def test_credentials_server_side_parameters_default_catalog_preserved() -> None:
    credentials = SparkCredentials(
        host="localhost",
        method=SparkConnectionMethod.THRIFT,  # type:ignore
        database="tests",
        schema="tests",
        server_side_parameters={"spark.sql.defaultCatalog": "turk_catalog"},
    )
    assert credentials.server_side_parameters["spark.sql.defaultCatalog"] == "turk_catalog"


# --- Kubernetes method credential tests ---


def test_kubernetes_credentials_valid() -> None:
    credentials = SparkCredentials(
        method=SparkConnectionMethod.KUBERNETES,  # type:ignore
        schema="my_schema",
        iceberg_rest_uri="http://iceberg-rest:8181",
        iceberg_warehouse="s3://warehouse/",
        image_registry="myregistry.io/dbt-iceberg",
        kubernetes_namespace="dbt-jobs",
        kubernetes_service_account="dbt-runner",
    )
    assert credentials.iceberg_rest_uri == "http://iceberg-rest:8181"
    assert credentials.iceberg_warehouse == "s3://warehouse/"
    assert credentials.image_registry == "myregistry.io/dbt-iceberg"
    assert credentials.kubernetes_namespace == "dbt-jobs"
    assert credentials.kubernetes_service_account == "dbt-runner"
    assert credentials.kubernetes_job_timeout == 3600  # default
    assert credentials.host is None


def test_kubernetes_credentials_with_thrift_host() -> None:
    """kubernetes method can also have a host for SQL introspection via thrift."""
    credentials = SparkCredentials(
        method=SparkConnectionMethod.KUBERNETES,  # type:ignore
        schema="my_schema",
        host="thrift-server",
        port=10000,
        iceberg_rest_uri="http://iceberg-rest:8181",
        iceberg_warehouse="s3://warehouse/",
        image_registry="myregistry.io/dbt-iceberg",
    )
    assert credentials.host == "thrift-server"
    assert credentials.port == 10000


def test_kubernetes_credentials_with_optional_token() -> None:
    credentials = SparkCredentials(
        method=SparkConnectionMethod.KUBERNETES,  # type:ignore
        schema="my_schema",
        iceberg_rest_uri="http://iceberg-rest:8181",
        iceberg_warehouse="s3://warehouse/",
        iceberg_token="my-secret-token",
        image_registry="myregistry.io/dbt-iceberg",
    )
    assert credentials.iceberg_token == "my-secret-token"


def test_kubernetes_credentials_missing_rest_uri_raises() -> None:
    with pytest.raises(DbtRuntimeError, match="iceberg_rest_uri"):
        SparkCredentials(
            method=SparkConnectionMethod.KUBERNETES,  # type:ignore
            schema="my_schema",
            iceberg_warehouse="s3://warehouse/",
            image_registry="myregistry.io/dbt-iceberg",
        )


def test_kubernetes_credentials_missing_warehouse_raises() -> None:
    with pytest.raises(DbtRuntimeError, match="iceberg_warehouse"):
        SparkCredentials(
            method=SparkConnectionMethod.KUBERNETES,  # type:ignore
            schema="my_schema",
            iceberg_rest_uri="http://iceberg-rest:8181",
            image_registry="myregistry.io/dbt-iceberg",
        )


def test_kubernetes_credentials_missing_image_registry_raises() -> None:
    with pytest.raises(DbtRuntimeError, match="image_registry"):
        SparkCredentials(
            method=SparkConnectionMethod.KUBERNETES,  # type:ignore
            schema="my_schema",
            iceberg_rest_uri="http://iceberg-rest:8181",
            iceberg_warehouse="s3://warehouse/",
        )


def test_kubernetes_credentials_custom_timeout() -> None:
    credentials = SparkCredentials(
        method=SparkConnectionMethod.KUBERNETES,  # type:ignore
        schema="my_schema",
        iceberg_rest_uri="http://iceberg-rest:8181",
        iceberg_warehouse="s3://warehouse/",
        image_registry="myregistry.io/dbt-iceberg",
        kubernetes_job_timeout=7200,
    )
    assert credentials.kubernetes_job_timeout == 7200
