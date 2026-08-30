# Orchestration image: Airflow plus a Spark client.
#
# Airflow submits the cold-path jobs in **client mode**, which means the Spark
# driver runs inside this container rather than on the cluster. That is why a
# JVM and a spark-submit binary have to be here at all, and why the scheduler
# service mounts the lake: the driver reads Parquet metadata and commits the
# output, even though the executors do the actual writing.
#
# Pinned to the same Spark version as the cluster and the batch_jobs client. A
# submit client newer than the cluster fails at task deserialization, so this
# pin, batch_jobs/pyproject.toml and the spark image tags move together.

FROM apache/airflow:2.10.5-python3.11

USER root

# ---------------------------------------------------------------------------
# JDK 17 — what Spark 3.5.x targets.
#
# Installed from apt rather than a JDK distribution image or a devcontainer
# feature, for the reason recorded in .devcontainer/Dockerfile: apt's openjdk
# registers /usr/bin/java through alternatives, so it lands on PATH by
# construction with no plumbing that can silently fail to compose.
#
# `procps` because Spark's launcher scripts call `ps`, which the slim Airflow
# base does not ship — a missing `ps` produces a confusing launcher error
# rather than an obvious "command not found".
# ---------------------------------------------------------------------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps; \
    rm -rf /var/lib/apt/lists/*; \
    # Resolve the real install directory (arch-dependent: java-17-openjdk-amd64
    # vs -arm64) by walking back from the alternatives symlink, then pin a
    # stable name so JAVA_HOME need not know the architecture.
    ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/current; \
    # Fail the build loudly here rather than at the first DAG run.
    java -version

# PySpark checks JAVA_HOME before falling back to PATH. Set it explicitly so
# neither route depends on shell profile files being sourced — Airflow runs
# tasks through a non-login shell.
ENV JAVA_HOME=/usr/lib/jvm/current

USER airflow

# pyspark ships the spark-submit binary, so it doubles as the Spark client and
# saves installing a full Spark distribution for the ~2% of it we use.
#
# No provider packages are installed: the stock apache/airflow image already
# carries postgres and common.sql, which is everything the DAGs import. Adding
# more would risk the resolver moving Airflow itself.
RUN pip install --no-cache-dir pyspark==3.5.3

# Prove the client is reachable at build time, not at the first task run.
RUN spark-submit --version
