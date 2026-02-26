<p align="center">
    <img
        src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg"
        alt="dbt logo"
        width="500"
    />
</p>

<p align="center">
    <a href="https://pypi.org/project/dbt-spark/">
        <img src="https://badge.fury.io/py/dbt-spark.svg" />
    </a>
    <a target="_blank" href="https://pypi.org/project/dbt-spark/" style="background:none">
        <img src="https://img.shields.io/pypi/pyversions/dbt-spark">
    </a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" />
    </a>
    <a href="https://github.com/python/mypy">
        <img src="https://www.mypy-lang.org/static/mypy_badge.svg" />
    </a>
    <a href="https://pepy.tech/project/dbt-spark">
        <img src="https://static.pepy.tech/badge/dbt-spark/month" />
    </a>
</p>

# dbt

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-spark

`dbt-spark` enables dbt to work with Apache Spark.
For more information on using dbt with Spark, consult [the docs](https://docs.getdbt.com/docs/profile-spark).

# Getting started

Review the repository [README.md](../README.md) as most of that information pertains to `dbt-spark`.

## Running locally

A `docker-compose` environment starts three services:

| Service | Description | Port |
|---|---|---|
| `dbt-spark3-thrift` | Spark Thrift Server (`HiveThriftServer2`) — long-running JDBC endpoint | `10000` |
| `dbt-spark-history` | Spark History Server — UI for completed queries and jobs | `18080` |
| `dbt-hive-metastore` | Postgres-backed Hive Metastore | internal |

Note: dbt-spark now supports Spark 4.1.0.

The following command starts all containers:

```sh
docker-compose up -d
```

It will take a bit of time for the instance to start; you can check the logs of the containers.
If the instance doesn't start correctly, try the complete reset command listed below and then try again.

Create a profile using the `spark_sql` method in thrift server mode:

```yaml
spark_testing:
  target: local
  outputs:
    local:
      type: spark
      method: spark_sql
      host: 127.0.0.1
      port: 10000
      user: dbt
      schema: analytics
      connect_retries: 5
      connect_timeout: 60
      retry_all: true
      spark_history_server: http://localhost:18080
```

Or using the `thrift` method directly:

```yaml
spark_testing:
  target: local
  outputs:
    local:
      type: spark
      method: thrift
      host: 127.0.0.1
      port: 10000
      user: dbt
      schema: analytics
      connect_retries: 5
      connect_timeout: 60
      retry_all: true
```

Connecting to the local Spark instance:

* **Spark UI** (active queries): [http://localhost:4040/sqlserver/](http://localhost:4040/sqlserver/)
* **Spark History Server** (completed queries): [http://localhost:18080](http://localhost:18080)
* **Thrift endpoint**: `jdbc:hive2://localhost:10000` — credentials `dbt`:`dbt`

Note that the Hive metastore data is persisted under `./.hive-metastore/`, Spark warehouse data under `./.spark-warehouse/`, and event logs under `./.spark-events/`. To completely reset your environment run:

```sh
docker-compose down
rm -rf ./.hive-metastore/ ./.spark-warehouse/ ./.spark-events/
```

## Additional Configuration for MacOS

If installing on MacOS, use `homebrew` to install required dependencies.
   ```sh
   brew install unixodbc
   ```

## Configuring spark-submit and spark-sql methods

The `spark_submit` and `spark_sql` connection methods run SQL and Python models using either a long-running Spark Thrift Server or the Spark CLI tools directly.

### Requirements

- A working Spark installation accessible via `SPARK_HOME` or on `PATH` (CLI mode), or a running `HiveThriftServer2` (thrift server mode).
- For `spark_submit`: `spark-submit` must be executable.
- For `spark_sql` CLI mode: `spark-sql` must be executable.
- For `spark_sql` thrift server mode: `pyhive` must be installed (`pip install dbt-spark[PyHive]`).

### spark-sql — thrift server mode (recommended)

When `host` is set, `spark_sql` connects to a long-running Spark Thrift Server (`HiveThriftServer2`) via the Thrift protocol. This avoids the JVM startup cost of spawning a new Spark application for every dbt invocation, and lets you share a single Spark context across many dbt runs. The Spark UI and History Server remain accessible throughout.

```yaml
my_spark_project:
  target: dev
  outputs:
    dev:
      type: spark
      method: spark_sql
      host: 127.0.0.1
      port: 10000
      schema: analytics
      user: dbt
      connect_retries: 5
      connect_timeout: 60
      retry_all: true
      # Optional: URL of the Spark History Server UI (informational — logged at startup)
      spark_history_server: http://127.0.0.1:18080
      # Optional: server-side Spark configuration applied to each session
      server_side_parameters:
        spark.executor.memory: "4g"
        spark.eventLog.enabled: "true"
        spark.eventLog.dir: /tmp/spark-events
```

Start a thrift server locally using the bundled docker-compose:

```sh
docker-compose up -d
```

Or start one manually:

```sh
$SPARK_HOME/sbin/start-thriftserver.sh \
  --master local[*] \
  --conf spark.sql.ansi.enabled=false \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/tmp/spark-events
```

### spark-sql — CLI mode

When `host` is **not** set, dbt executes each statement with `spark-sql -e '...'` (or `-f` for batches). A new Spark application is launched per dbt invocation.

```yaml
my_spark_project:
  target: dev
  outputs:
    dev:
      type: spark
      method: spark_sql
      schema: analytics
      # Optional: explicit path to SPARK_HOME if not set in environment
      spark_home: /opt/spark
      # Optional: extra CLI flags passed to spark-sql
      spark_sql_args:
        - "--master"
        - "local[*]"
        - "--conf"
        - "spark.executor.memory=4g"
```

### spark-submit

Use `spark_submit` when your project includes Python models. dbt executes Python models via `spark-submit`. SQL statements (e.g. catalog introspection) fall back to thrift when `host` is set, or to the `spark-sql` CLI otherwise.

**With a thrift server (recommended):**

```yaml
my_spark_project:
  target: dev
  outputs:
    dev:
      type: spark
      method: spark_submit
      host: 127.0.0.1
      port: 10000
      schema: analytics
      user: dbt
      spark_history_server: http://127.0.0.1:18080
      # Extra CLI flags for spark-submit (Python models only)
      spark_submit_args:
        - "--master"
        - "local[*]"
        - "--conf"
        - "spark.executor.memory=4g"
      spark_submit_timeout: 3600
```

**CLI only (no thrift server):**

```yaml
my_spark_project:
  target: dev
  outputs:
    dev:
      type: spark
      method: spark_submit
      schema: analytics
      spark_home: /opt/spark
      spark_submit_args:
        - "--master"
        - "local[*]"
        - "--conf"
        - "spark.executor.memory=4g"
      spark_sql_args:
        - "--master"
        - "local[*]"
      spark_submit_timeout: 3600
```

### Profile fields

| Field | Required | Default | Description |
|---|---|---|---|
| `method` | Yes | — | `spark_sql` or `spark_submit` |
| `schema` | Yes | — | The default schema (database) to use |
| `host` | No | — | Thrift server hostname. When set, `spark_sql` and `spark_submit` connect via Thrift instead of CLI |
| `port` | No | `443` | Thrift server port (typically `10000`) |
| `user` | No | — | Username for thrift server authentication |
| `auth` | No | — | Auth mechanism for thrift (e.g. `NONE`, `LDAP`, `KERBEROS`) |
| `use_ssl` | No | `false` | Use SSL/TLS for the thrift connection |
| `spark_history_server` | No | — | URL of the Spark History Server UI (informational) |
| `spark_home` | No | `$SPARK_HOME` env var | Path to your Spark installation (CLI mode) |
| `spark_sql_args` | No | `[]` | Extra CLI flags for `spark-sql` (CLI mode only) |
| `spark_submit_args` | No | `[]` | Extra CLI flags for `spark-submit` (Python models only) |
| `spark_submit_timeout` | No | `null` (no timeout) | Max seconds to wait for a `spark-submit` job |
| `poll_interval` | No | `5` | Seconds between status polls for thrift queries |
| `query_timeout` | No | `null` (no timeout) | Max seconds for a single thrift query |
| `query_retries` | No | `1` | Times to retry on connection loss during query polling |
| `server_side_parameters` | No | `{}` | Spark configuration sent to the thrift server per session |

### Notes

- Thrift server mode (`host` set) requires `pip install dbt-spark[PyHive]` for both `spark_sql` and `spark_submit`.
- `spark_submit` only invokes `spark-submit` for Python models. SQL statements use thrift when `host` is set, otherwise `spark-sql` CLI.
- `spark_sql_args` is only used in CLI mode; it is ignored when `host` is set.
- If `spark_home` is not set in the profile, dbt falls back to the `SPARK_HOME` environment variable, then to `spark-sql`/`spark-submit` on `PATH`.
- The Spark History Server requires event logging on the thrift server (`spark.eventLog.enabled=true`). The bundled docker-compose configures this automatically.

## Contribute

- Want to help us build `dbt-spark`? Check out the [Contributing Guide](CONTRIBUTING.md).
