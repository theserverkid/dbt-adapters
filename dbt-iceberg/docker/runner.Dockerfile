# dbt-iceberg runner pod image
# Runs dbt itself inside Kubernetes, submitting Python model Jobs to the cluster.
# Requires an in-cluster ServiceAccount with permission to create/get/delete
# Jobs and ConfigMaps in the configured kubernetes_namespace.
#
# Build:
#   docker build -f docker/runner.Dockerfile -t myregistry/dbt-iceberg-runner:latest .
#
# Required RBAC (minimum):
#   - configmaps: create, get, delete
#   - batch/jobs: create, get, delete, watch, list
#   - pods: get, list, watch
#   - pods/log: get

ARG py_version=3.11
FROM python:${py_version}-slim-bookworm AS base

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    gcc \
    git \
    libsasl2-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

RUN pip install --no-cache-dir --upgrade pip

FROM base AS dbt-iceberg-runner

# Install dbt-iceberg with PyHive (for SQL via thrift) and kubernetes extras.
# Copy the source so the runner has the same version as the project being run.
COPY . /build/dbt-iceberg/
RUN pip install --no-cache-dir "/build/dbt-iceberg[PyHive,kubernetes]"

WORKDIR /usr/app/dbt/

ENTRYPOINT ["dbt"]
