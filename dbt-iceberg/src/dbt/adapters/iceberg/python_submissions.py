import base64
import os
import re
import time
import requests
from typing import Any, Dict, Callable, Iterable, Optional
import uuid

from dbt.adapters.base import PythonJobHelper
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.iceberg import IcebergCredentials
from dbt.adapters.iceberg import __version__

DEFAULT_POLLING_INTERVAL = 10
SUBMISSION_LANGUAGE = "python"
DEFAULT_TIMEOUT = 60 * 60 * 24
DBT_SPARK_VERSION = __version__.version


class BaseDatabricksHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: IcebergCredentials) -> None:
        self.credentials = credentials
        self.identifier = parsed_model["alias"]
        self.schema = parsed_model["schema"]
        self.parsed_model = parsed_model
        self.timeout = self.get_timeout()
        self.polling_interval = DEFAULT_POLLING_INTERVAL
        self.check_credentials()
        self.auth_header = {
            "Authorization": f"Bearer {self.credentials.token}",
            "User-Agent": f"dbt-labs-dbt-spark/{DBT_SPARK_VERSION} (Databricks)",
        }

    @property
    def cluster_id(self) -> str:
        return self.parsed_model["config"].get("cluster_id", self.credentials.cluster_id)

    def get_timeout(self) -> int:
        timeout = self.parsed_model["config"].get("timeout", DEFAULT_TIMEOUT)
        if timeout <= 0:
            raise ValueError("Timeout must be a positive integer")
        return timeout

    def check_credentials(self) -> None:
        raise NotImplementedError(
            "Overwrite this method to check specific requirement for current submission method"
        )

    def _create_work_dir(self, path: str) -> None:
        response = requests.post(
            f"https://{self.credentials.host}/api/2.0/workspace/mkdirs",
            headers=self.auth_header,
            json={
                "path": path,
            },
        )
        if response.status_code != 200:
            raise DbtRuntimeError(
                f"Error creating work_dir for python notebooks\n {response.content!r}"
            )

    def _upload_notebook(self, path: str, compiled_code: str) -> None:
        b64_encoded_content = base64.b64encode(compiled_code.encode()).decode()
        response = requests.post(
            f"https://{self.credentials.host}/api/2.0/workspace/import",
            headers=self.auth_header,
            json={
                "path": path,
                "content": b64_encoded_content,
                "language": "PYTHON",
                "overwrite": True,
                "format": "SOURCE",
            },
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error creating python notebook.\n {response.content!r}")

    def _submit_job(self, path: str, cluster_spec: dict) -> str:
        job_spec = {
            "run_name": f"{self.schema}-{self.identifier}-{uuid.uuid4()}",
            "notebook_task": {
                "notebook_path": path,
            },
        }
        job_spec.update(cluster_spec)  # updates 'new_cluster' config
        # PYPI packages
        packages = self.parsed_model["config"].get("packages", [])
        # additional format of packages
        additional_libs = self.parsed_model["config"].get("additional_libs", [])
        libraries = []
        for package in packages:
            libraries.append({"pypi": {"package": package}})
        for lib in additional_libs:
            libraries.append(lib)
        job_spec.update({"libraries": libraries})  # type: ignore
        submit_response = requests.post(
            f"https://{self.credentials.host}/api/2.1/jobs/runs/submit",
            headers=self.auth_header,
            json=job_spec,
        )
        if submit_response.status_code != 200:
            raise DbtRuntimeError(f"Error creating python run.\n {submit_response.content!r}")
        return submit_response.json()["run_id"]

    def _submit_through_notebook(self, compiled_code: str, cluster_spec: dict) -> None:
        # it is safe to call mkdirs even if dir already exists and have content inside
        work_dir = f"/Shared/dbt_python_model/{self.schema}/"
        self._create_work_dir(work_dir)
        # add notebook
        whole_file_path = f"{work_dir}{self.identifier}"
        self._upload_notebook(whole_file_path, compiled_code)

        # submit job
        run_id = self._submit_job(whole_file_path, cluster_spec)

        self.polling(
            status_func=requests.get,
            status_func_kwargs={
                "url": f"https://{self.credentials.host}/api/2.1/jobs/runs/get?run_id={run_id}",
                "headers": self.auth_header,
            },
            get_state_func=lambda response: response.json()["state"]["life_cycle_state"],
            terminal_states=("TERMINATED", "SKIPPED", "INTERNAL_ERROR"),
            expected_end_state="TERMINATED",
            get_state_msg_func=lambda response: response.json()["state"]["state_message"],
        )

        # get end state to return to user
        run_output = requests.get(
            f"https://{self.credentials.host}" f"/api/2.1/jobs/runs/get-output?run_id={run_id}",
            headers=self.auth_header,
        )
        json_run_output = run_output.json()
        result_state = json_run_output["metadata"]["state"]["result_state"]
        if result_state != "SUCCESS":
            raise DbtRuntimeError(
                "Python model failed with traceback as:\n"
                "(Note that the line number here does not "
                "match the line number in your code due to dbt templating)\n"
                f"{json_run_output['error_trace']}"
            )

    def submit(self, compiled_code: str) -> None:
        raise NotImplementedError(
            "BasePythonJobHelper is an abstract class and you should implement submit method."
        )

    def polling(
        self,
        status_func: Callable,
        status_func_kwargs: Dict,
        get_state_func: Callable,
        terminal_states: Iterable[str],
        expected_end_state: str,
        get_state_msg_func: Callable,
    ) -> Dict:
        state = None
        start = time.time()
        exceeded_timeout = False
        response: Dict = {}
        while state is None or state not in terminal_states:
            if time.time() - start > self.timeout:
                exceeded_timeout = True
                break
            # should we do exponential backoff?
            time.sleep(self.polling_interval)
            response = status_func(**status_func_kwargs)
            state = get_state_func(response)
        if exceeded_timeout:
            raise DbtRuntimeError("python model run timed out")
        if state != expected_end_state:
            raise DbtRuntimeError(
                "python model run ended in state"
                f"{state} with state_message\n{get_state_msg_func(response)}"
            )
        return response


class JobClusterPythonJobHelper(BaseDatabricksHelper):
    def check_credentials(self) -> None:
        if not self.parsed_model["config"].get("job_cluster_config", None):
            raise ValueError("job_cluster_config is required for commands submission method.")

    def submit(self, compiled_code: str) -> None:
        cluster_spec = {"new_cluster": self.parsed_model["config"]["job_cluster_config"]}
        self._submit_through_notebook(compiled_code, cluster_spec)


class DBContext:
    def __init__(self, credentials: IcebergCredentials, cluster_id: str, auth_header: dict) -> None:
        self.auth_header = auth_header
        self.cluster_id = cluster_id
        self.host = credentials.host

    def create(self) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#create-an-execution-context
        response = requests.post(
            f"https://{self.host}/api/1.2/contexts/create",
            headers=self.auth_header,
            json={
                "clusterId": self.cluster_id,
                "language": SUBMISSION_LANGUAGE,
            },
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error creating an execution context.\n {response.content!r}")
        return response.json()["id"]

    def destroy(self, context_id: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#delete-an-execution-context
        response = requests.post(
            f"https://{self.host}/api/1.2/contexts/destroy",
            headers=self.auth_header,
            json={
                "clusterId": self.cluster_id,
                "contextId": context_id,
            },
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error deleting an execution context.\n {response.content!r}")
        return response.json()["id"]


class DBCommand:
    def __init__(self, credentials: IcebergCredentials, cluster_id: str, auth_header: dict) -> None:
        self.auth_header = auth_header
        self.cluster_id = cluster_id
        self.host = credentials.host

    def execute(self, context_id: str, command: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#run-a-command
        response = requests.post(
            f"https://{self.host}/api/1.2/commands/execute",
            headers=self.auth_header,
            json={
                "clusterId": self.cluster_id,
                "contextId": context_id,
                "language": SUBMISSION_LANGUAGE,
                "command": command,
            },
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error creating a command.\n {response.content!r}")
        return response.json()["id"]

    def status(self, context_id: str, command_id: str) -> Dict[str, Any]:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#get-information-about-a-command
        response = requests.get(
            f"https://{self.host}/api/1.2/commands/status",
            headers=self.auth_header,
            params={
                "clusterId": self.cluster_id,
                "contextId": context_id,
                "commandId": command_id,
            },
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error getting status of command.\n {response.content!r}")
        return response.json()


class AllPurposeClusterPythonJobHelper(BaseDatabricksHelper):
    def check_credentials(self) -> None:
        if not self.cluster_id:
            raise ValueError(
                "Databricks cluster_id is required for all_purpose_cluster submission method with running with notebook."
            )

    def submit(self, compiled_code: str) -> None:
        if self.parsed_model["config"].get("create_notebook", False):
            self._submit_through_notebook(compiled_code, {"existing_cluster_id": self.cluster_id})
        else:
            context = DBContext(self.credentials, self.cluster_id, self.auth_header)
            command = DBCommand(self.credentials, self.cluster_id, self.auth_header)
            context_id = context.create()
            try:
                command_id = command.execute(context_id, compiled_code)
                # poll until job finish
                response = self.polling(
                    status_func=command.status,
                    status_func_kwargs={
                        "context_id": context_id,
                        "command_id": command_id,
                    },
                    get_state_func=lambda response: response["status"],
                    terminal_states=("Cancelled", "Error", "Finished"),
                    expected_end_state="Finished",
                    get_state_msg_func=lambda response: response.json()["results"]["data"],
                )
                if response["results"]["resultType"] == "error":
                    raise DbtRuntimeError(
                        f"Python model failed with traceback as:\n"
                        f"{response['results']['cause']}"
                    )
            finally:
                context.destroy(context_id)


# File extension → Docker image suffix mapping
# Client projects must build and push images named:
#   {image_registry}/dbt-iceberg-{suffix}:latest
# See documentation for the required image interface.
EXTENSION_IMAGE_MAP: Dict[str, str] = {
    ".xlsx": "excel",
    ".xlsb": "excel",
    ".xls":  "excel",
    ".pdf":  "pdf",
    ".csv":  "csv",
    ".tsv":  "csv",
    ".jpeg": "image",
    ".jpg":  "image",
    ".png":  "image",
}
DEFAULT_IMAGE_SUFFIX = "base"


class KubernetesPythonJobHelper(PythonJobHelper):
    """Submit a Python model as a Kubernetes Job running a Docker container.

    The container uses Polars + PyIceberg to process data and write output
    to an Iceberg REST catalog. Image selection is automatic based on the
    file extension configured in the model config (file_extension field).

    The compiled model code (including the dbt materialization wrapper) is
    passed via a Kubernetes ConfigMap mounted at /opt/dbt/model.py.

    Profile fields used:
      iceberg_rest_uri            – Iceberg REST catalog URI (required).
      iceberg_warehouse           – Iceberg warehouse path (required).
      iceberg_token               – Optional Bearer token for REST catalog.
      kubernetes_namespace        – Namespace for Job pods (default: "default").
      kubernetes_service_account  – ServiceAccount name (default: "default").
      kubernetes_job_timeout      – Max seconds to wait (default: 3600).
      kubernetes_job_image_pull_policy – Image pull policy (default: "IfNotPresent").
      image_registry              – Docker image registry prefix (required).
      data_folder                 – Base data folder path passed into the container.

    Model config fields:
      file_extension    – File extension to auto-select image (e.g. ".xlsx").
      data_folder       – Subfolder under profile's data_folder for this model.
      kubernetes_image  – Explicit image override (skips auto-selection).
      kubernetes_resource_limits   – Dict of K8s resource limits.
      kubernetes_resource_requests – Dict of K8s resource requests.
      timeout           – Job timeout in seconds (overrides profile default).
    """

    def __init__(self, parsed_model: Dict, credentials: "IcebergCredentials") -> None:  # type: ignore[name-defined]
        self.parsed_model = parsed_model
        self.credentials = credentials
        self.identifier = parsed_model["alias"]
        self.schema = parsed_model["schema"]
        self.timeout = self._get_timeout()
        self._k8s_batch: Any = None
        self._k8s_core: Any = None

    def _get_timeout(self) -> int:
        model_timeout = self.parsed_model["config"].get("timeout", None)
        if model_timeout is not None:
            return int(model_timeout)
        return getattr(self.credentials, "kubernetes_job_timeout", 3600)

    def _load_k8s_client(self) -> None:
        """Load the Kubernetes client using in-cluster config."""
        try:
            from kubernetes import client, config as k8s_config
        except ImportError:
            raise DbtRuntimeError(
                "kubernetes Python package is required for the kubernetes submission method. "
                "Install with: pip install dbt-iceberg[kubernetes]"
            )
        try:
            k8s_config.load_incluster_config()
        except Exception:
            # Fallback for local development / testing outside a cluster
            k8s_config.load_kube_config()
        self._k8s_batch = client.BatchV1Api()
        self._k8s_core = client.CoreV1Api()

    def _get_namespace(self) -> str:
        return getattr(self.credentials, "kubernetes_namespace", "default")

    def _get_service_account(self) -> str:
        return getattr(self.credentials, "kubernetes_service_account", "default")

    def _get_docker_image(self) -> str:
        """Determine the Docker image to run.

        Priority:
        1. Explicit ``kubernetes_image`` in model config.
        2. Auto-detect from ``file_extension`` model config key.
        3. Scan ``data_folder/model_folder`` directory for a known extension.
        4. Fallback to ``{image_registry}/dbt-iceberg-base:latest``.
        """
        registry: str = self.credentials.image_registry  # type: ignore[assignment]

        explicit = self.parsed_model["config"].get("kubernetes_image", None)
        if explicit:
            return explicit

        file_ext = self.parsed_model["config"].get("file_extension", None)
        if file_ext:
            ext = file_ext if file_ext.startswith(".") else f".{file_ext}"
            suffix = EXTENSION_IMAGE_MAP.get(ext.lower(), DEFAULT_IMAGE_SUFFIX)
            return f"{registry}/dbt-iceberg-{suffix}:latest"

        model_folder = self.parsed_model["config"].get("data_folder", None)
        base_folder = getattr(self.credentials, "data_folder", None)
        if model_folder and base_folder:
            full_path = os.path.join(base_folder, model_folder)
            try:
                for fname in os.listdir(full_path):
                    ext = os.path.splitext(fname)[1].lower()
                    if ext in EXTENSION_IMAGE_MAP:
                        suffix = EXTENSION_IMAGE_MAP[ext]
                        return f"{registry}/dbt-iceberg-{suffix}:latest"
            except OSError:
                pass

        return f"{registry}/dbt-iceberg-{DEFAULT_IMAGE_SUFFIX}:latest"

    def _resource_name(self) -> str:
        """Generate a unique K8s-safe resource name (max 63 chars)."""
        safe_schema = re.sub(r"[^a-z0-9-]", "-", self.schema.lower())
        safe_id = re.sub(r"[^a-z0-9-]", "-", self.identifier.lower())
        unique = uuid.uuid4().hex[:8]
        base = f"dbt-{safe_schema}-{safe_id}"[:50]
        return f"{base}-{unique}"

    def _create_configmap(self, name: str, compiled_code: str) -> None:
        from kubernetes import client

        configmap = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=self._get_namespace(),
                labels={
                    "app": "dbt-iceberg",
                    "dbt-schema": self.schema,
                    "dbt-model": self.identifier,
                },
            ),
            data={"model.py": compiled_code},
        )
        self._k8s_core.create_namespaced_config_map(
            namespace=self._get_namespace(),
            body=configmap,
        )

    def _build_job_manifest(self, job_name: str, configmap_name: str, image: str) -> Any:
        from kubernetes import client

        creds = self.credentials
        env_vars = [
            client.V1EnvVar(name="ICEBERG_REST_URI",  value=creds.iceberg_rest_uri),
            client.V1EnvVar(name="ICEBERG_WAREHOUSE", value=creds.iceberg_warehouse),
            client.V1EnvVar(name="DBT_SCHEMA",        value=self.schema),
            client.V1EnvVar(name="DBT_IDENTIFIER",    value=self.identifier),
            client.V1EnvVar(name="DATA_FOLDER",       value=getattr(creds, "data_folder", "") or ""),
            client.V1EnvVar(name="MODEL_FOLDER",      value=self.parsed_model["config"].get("data_folder", "")),
        ]
        if creds.iceberg_token:
            env_vars.append(client.V1EnvVar(name="ICEBERG_TOKEN", value=creds.iceberg_token))

        env_secret = getattr(creds, "kubernetes_env_secret", None)
        env_from = (
            [client.V1EnvFromSource(
                secret_ref=client.V1SecretEnvSource(name=env_secret)
            )]
            if env_secret
            else None
        )

        resource_requirements = self._get_resource_requirements()
        container = client.V1Container(
            name="dbt-model-runner",
            image=image,
            image_pull_policy=getattr(creds, "kubernetes_job_image_pull_policy", "IfNotPresent"),
            env=env_vars,
            env_from=env_from,
            volume_mounts=[
                client.V1VolumeMount(
                    name="model-code",
                    mount_path="/opt/dbt/model.py",
                    sub_path="model.py",
                    read_only=True,
                )
            ],
            resources=resource_requirements,
        )
        image_pull_secret = getattr(creds, "kubernetes_image_pull_secret", None)
        image_pull_secrets = (
            [client.V1LocalObjectReference(name=image_pull_secret)]
            if image_pull_secret
            else None
        )
        pod_spec = client.V1PodSpec(
            restart_policy="Never",
            service_account_name=self._get_service_account(),
            containers=[container],
            image_pull_secrets=image_pull_secrets,
            volumes=[
                client.V1Volume(
                    name="model-code",
                    config_map=client.V1ConfigMapVolumeSource(name=configmap_name),
                )
            ],
        )
        return client.V1Job(
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=self._get_namespace(),
                labels={
                    "app": "dbt-iceberg",
                    "dbt-schema": self.schema,
                    "dbt-model": self.identifier,
                },
            ),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={
                            "app": "dbt-iceberg",
                            "dbt-schema": self.schema,
                            "dbt-model": self.identifier,
                        }
                    ),
                    spec=pod_spec,
                ),
                backoff_limit=0,
                ttl_seconds_after_finished=300,
            ),
        )

    def _get_resource_requirements(self) -> Optional[Any]:
        try:
            from kubernetes import client
        except ImportError:
            return None
        config = self.parsed_model.get("config", {})
        limits = config.get("kubernetes_resource_limits", None)
        requests = config.get("kubernetes_resource_requests", None)
        if limits or requests:
            return client.V1ResourceRequirements(limits=limits, requests=requests)
        return None

    def _poll_job(self, job_name: str) -> None:
        """Poll the Kubernetes Job until completion, timeout, or failure."""
        from dbt.adapters.events.logging import AdapterLogger
        _logger = AdapterLogger("Iceberg")

        namespace = self._get_namespace()
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time
            if elapsed > self.timeout:
                raise DbtRuntimeError(
                    f"Kubernetes Job {job_name} timed out after {self.timeout} seconds "
                    f"for model {self.schema}.{self.identifier}"
                )
            time.sleep(DEFAULT_POLLING_INTERVAL)

            job = self._k8s_batch.read_namespaced_job(name=job_name, namespace=namespace)
            status = job.status

            if status.succeeded and status.succeeded >= 1:
                return

            if status.failed and status.failed >= 1:
                logs = self._get_pod_logs(job_name)
                raise DbtRuntimeError(
                    f"Kubernetes Job {job_name} failed for model "
                    f"{self.schema}.{self.identifier}.\n"
                    f"Pod logs:\n{logs}"
                )

            active = status.active or 0
            _logger.debug(f"Job {job_name}: active={active}, elapsed={elapsed:.0f}s")

    def _get_pod_logs(self, job_name: str) -> str:
        """Retrieve logs from the pod created by this Job."""
        namespace = self._get_namespace()
        try:
            pods = self._k8s_core.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"job-name={job_name}",
            )
            if not pods.items:
                return "(no pods found)"
            pod_name = pods.items[0].metadata.name
            return self._k8s_core.read_namespaced_pod_log(
                name=pod_name,
                namespace=namespace,
                tail_lines=200,
            )
        except Exception as e:
            return f"(could not retrieve pod logs: {e})"

    def _cleanup(self, job_name: str, configmap_name: str) -> None:
        """Delete the Job and ConfigMap; ignore 404 errors."""
        from dbt.adapters.events.logging import AdapterLogger
        _logger = AdapterLogger("Iceberg")

        namespace = self._get_namespace()
        try:
            from kubernetes.client.rest import ApiException
        except ImportError:
            ApiException = Exception  # type: ignore[misc,assignment]

        for resource, delete_fn, name in [
            ("Job", self._k8s_batch.delete_namespaced_job, job_name),
            ("ConfigMap", self._k8s_core.delete_namespaced_config_map, configmap_name),
        ]:
            try:
                delete_fn(name=name, namespace=namespace)
                _logger.debug(f"Deleted {resource} {name}")
            except ApiException as e:
                if hasattr(e, "status") and e.status != 404:
                    _logger.warning(f"Could not delete {resource} {name}: {e}")
            except Exception as e:
                _logger.warning(f"Could not delete {resource} {name}: {e}")

    def submit(self, compiled_code: str) -> None:
        from dbt.adapters.events.logging import AdapterLogger
        _logger = AdapterLogger("Iceberg")

        self._load_k8s_client()
        resource_name = self._resource_name()
        image = self._get_docker_image()

        _logger.debug(
            f"Submitting Kubernetes Job for {self.schema}.{self.identifier} "
            f"using image {image}"
        )
        try:
            self._create_configmap(resource_name, compiled_code)
            job_manifest = self._build_job_manifest(resource_name, resource_name, image)
            self._k8s_batch.create_namespaced_job(
                namespace=self._get_namespace(),
                body=job_manifest,
            )
            self._poll_job(resource_name)
        finally:
            self._cleanup(resource_name, resource_name)
