import re
from unittest import mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.iceberg.python_submissions import (
    KubernetesPythonJobHelper,
    EXTENSION_IMAGE_MAP,
    DEFAULT_IMAGE_SUFFIX,
)


class MockCredentials:
    iceberg_rest_uri = "http://iceberg-rest:8181"
    iceberg_warehouse = "s3://warehouse/"
    iceberg_token = None
    kubernetes_namespace = "dbt-jobs"
    kubernetes_service_account = "dbt-runner"
    kubernetes_job_timeout = 3600
    kubernetes_job_image_pull_policy = "IfNotPresent"
    image_registry = "myregistry.io/dbt-iceberg"
    data_folder = "/data"


def _make_helper(file_extension=None, data_folder=None, kubernetes_image=None, timeout=None):
    config = {}
    if file_extension is not None:
        config["file_extension"] = file_extension
    if data_folder is not None:
        config["data_folder"] = data_folder
    if kubernetes_image is not None:
        config["kubernetes_image"] = kubernetes_image
    if timeout is not None:
        config["timeout"] = timeout
    parsed_model = {
        "alias": "my_model",
        "schema": "analytics",
        "config": config,
    }
    return KubernetesPythonJobHelper(parsed_model, MockCredentials())


# --- Image selection ---


def test_image_selection_xlsx():
    h = _make_helper(file_extension=".xlsx")
    assert h._get_docker_image() == "myregistry.io/dbt-iceberg/dbt-iceberg-excel:latest"


def test_image_selection_xlsb():
    h = _make_helper(file_extension=".xlsb")
    assert h._get_docker_image() == "myregistry.io/dbt-iceberg/dbt-iceberg-excel:latest"


def test_image_selection_pdf():
    h = _make_helper(file_extension=".pdf")
    assert h._get_docker_image() == "myregistry.io/dbt-iceberg/dbt-iceberg-pdf:latest"


def test_image_selection_csv():
    h = _make_helper(file_extension=".csv")
    assert h._get_docker_image() == "myregistry.io/dbt-iceberg/dbt-iceberg-csv:latest"


def test_image_selection_tsv():
    h = _make_helper(file_extension=".tsv")
    assert h._get_docker_image() == "myregistry.io/dbt-iceberg/dbt-iceberg-csv:latest"


def test_image_selection_jpeg():
    h = _make_helper(file_extension=".jpeg")
    assert h._get_docker_image() == "myregistry.io/dbt-iceberg/dbt-iceberg-image:latest"


def test_image_selection_extension_without_dot():
    h = _make_helper(file_extension="xlsx")
    assert h._get_docker_image() == "myregistry.io/dbt-iceberg/dbt-iceberg-excel:latest"


def test_image_selection_explicit_override():
    h = _make_helper(kubernetes_image="custom-registry/my-custom-image:v2")
    assert h._get_docker_image() == "custom-registry/my-custom-image:v2"


def test_image_selection_folder_scan():
    h = _make_helper(data_folder="invoices")
    with mock.patch("os.listdir", return_value=["file1.xlsx", "file2.xlsx", "readme.txt"]):
        result = h._get_docker_image()
    assert result == "myregistry.io/dbt-iceberg/dbt-iceberg-excel:latest"


def test_image_selection_folder_scan_pdf():
    h = _make_helper(data_folder="reports")
    with mock.patch("os.listdir", return_value=["report.pdf"]):
        result = h._get_docker_image()
    assert result == "myregistry.io/dbt-iceberg/dbt-iceberg-pdf:latest"


def test_image_selection_fallback_to_base():
    h = _make_helper()
    with mock.patch("os.listdir", return_value=[]):
        result = h._get_docker_image()
    assert result == f"myregistry.io/dbt-iceberg/dbt-iceberg-{DEFAULT_IMAGE_SUFFIX}:latest"


def test_image_selection_folder_scan_oserror_falls_back():
    h = _make_helper(data_folder="missing")
    with mock.patch("os.listdir", side_effect=OSError("not found")):
        result = h._get_docker_image()
    assert result == f"myregistry.io/dbt-iceberg/dbt-iceberg-{DEFAULT_IMAGE_SUFFIX}:latest"


# --- Resource name ---


def test_resource_name_is_k8s_safe():
    h = _make_helper()
    name = h._resource_name()
    assert re.match(r"^[a-z0-9-]+$", name), f"Name {name!r} contains invalid characters"
    assert len(name) <= 63, f"Name {name!r} exceeds 63 characters"


def test_resource_name_sanitises_underscores():
    parsed_model = {"alias": "my_model", "schema": "my_schema", "config": {}}
    h = KubernetesPythonJobHelper(parsed_model, MockCredentials())
    name = h._resource_name()
    assert "_" not in name


def test_resource_name_is_unique():
    h = _make_helper()
    names = {h._resource_name() for _ in range(20)}
    assert len(names) == 20  # all unique due to uuid suffix


# --- Timeout ---


def test_timeout_default_from_credentials():
    h = _make_helper()
    assert h.timeout == 3600


def test_timeout_override_from_model_config():
    h = _make_helper(timeout=7200)
    assert h.timeout == 7200


def test_timeout_override_from_model_config_string():
    h = _make_helper(timeout="600")
    assert h.timeout == 600


# --- Extension map completeness ---


def test_extension_image_map_contains_expected_extensions():
    expected = {".xlsx", ".xlsb", ".xls", ".pdf", ".csv", ".tsv", ".jpeg", ".jpg", ".png"}
    assert expected.issubset(set(EXTENSION_IMAGE_MAP.keys()))


# --- submit() orchestration ---


def _make_helper_with_mocked_k8s(file_extension=".csv"):
    h = _make_helper(file_extension=file_extension)
    h._k8s_batch = mock.MagicMock()
    h._k8s_core = mock.MagicMock()
    return h


def test_submit_calls_create_configmap_and_job():
    h = _make_helper_with_mocked_k8s()
    with mock.patch.object(h, "_load_k8s_client"), \
         mock.patch.object(h, "_create_configmap") as mock_cm, \
         mock.patch.object(h, "_poll_job") as mock_poll, \
         mock.patch.object(h, "_cleanup"):
        h.submit("def model(dbt, session): import polars as pl; return pl.DataFrame()")
    mock_cm.assert_called_once()
    mock_poll.assert_called_once()


def test_submit_calls_cleanup_on_success():
    h = _make_helper_with_mocked_k8s()
    with mock.patch.object(h, "_load_k8s_client"), \
         mock.patch.object(h, "_create_configmap"), \
         mock.patch.object(h, "_poll_job"), \
         mock.patch.object(h, "_cleanup") as mock_cleanup:
        h.submit("def model(dbt, session): import polars as pl; return pl.DataFrame()")
    mock_cleanup.assert_called_once()


def test_submit_calls_cleanup_on_poll_failure():
    h = _make_helper_with_mocked_k8s()
    with mock.patch.object(h, "_load_k8s_client"), \
         mock.patch.object(h, "_create_configmap"), \
         mock.patch.object(h, "_poll_job", side_effect=DbtRuntimeError("job failed")), \
         mock.patch.object(h, "_cleanup") as mock_cleanup:
        with pytest.raises(DbtRuntimeError, match="job failed"):
            h.submit("def model(dbt, session): import polars as pl; return pl.DataFrame()")
    mock_cleanup.assert_called_once()  # cleanup must run even on failure


def test_submit_calls_cleanup_on_configmap_failure():
    h = _make_helper_with_mocked_k8s()
    with mock.patch.object(h, "_load_k8s_client"), \
         mock.patch.object(h, "_create_configmap", side_effect=Exception("K8s error")), \
         mock.patch.object(h, "_poll_job") as mock_poll, \
         mock.patch.object(h, "_cleanup") as mock_cleanup:
        with pytest.raises(Exception, match="K8s error"):
            h.submit("def model(dbt, session): pass")
    mock_poll.assert_not_called()
    mock_cleanup.assert_called_once()


def test_submit_resource_names_are_consistent():
    """ConfigMap and Job should use the same resource name."""
    h = _make_helper_with_mocked_k8s()
    captured_names = {}

    def capture_cm(name, compiled_code):
        captured_names["cm"] = name

    def capture_job(name, cm_name, image):
        captured_names["job"] = name
        captured_names["job_cm_ref"] = cm_name
        return mock.MagicMock()

    with mock.patch.object(h, "_load_k8s_client"), \
         mock.patch.object(h, "_create_configmap", side_effect=capture_cm), \
         mock.patch.object(h, "_build_job_manifest", side_effect=capture_job), \
         mock.patch.object(h, "_poll_job"), \
         mock.patch.object(h, "_cleanup"):
        h.submit("def model(dbt, session): import polars as pl; return pl.DataFrame()")

    assert captured_names["cm"] == captured_names["job"]
    assert captured_names["job"] == captured_names["job_cm_ref"]
