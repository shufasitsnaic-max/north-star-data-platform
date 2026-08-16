# Dev container base image.
#
# Exists solely to work around an upstream defect in the Microsoft image, which
# would otherwise be usable via a plain `"image"` key in devcontainer.json.
#
# The problem: mcr.microsoft.com/devcontainers/python:1-3.11-bookworm ships a
# preconfigured Yarn apt source (dl.yarnpkg.com) whose signing key has since
# rotated. Any `apt-get update` in the image fails with:
#
#     NO_PUBKEY 62D54FD4003F6525
#     E: The repository 'https://dl.yarnpkg.com/debian stable InRelease' is not signed.
#
# Dev container *features* run `apt-get update` during the build, so this makes
# the docker-in-docker feature abort (exit 100), which fails container creation
# outright and lands the codespace in an Alpine recovery container with no
# Docker daemon at all. Observed 2026-08-16; see docs/DECISIONS.md.
#
# The fix: this project has no use for Yarn, so drop the source rather than
# chase the rotated key. Matching on the URL (not a filename) keeps this working
# whether the image uses the legacy .list format or deb822 .sources.

FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

RUN set -eux; \
    # Delete any apt source referencing the Yarn repository.
    grep -rl 'dl\.yarnpkg\.com' /etc/apt/sources.list.d/ 2>/dev/null | xargs -r rm -f; \
    # ...and the same entry if it was written into the main sources list.
    if [ -f /etc/apt/sources.list ]; then \
        sed -i '/dl\.yarnpkg\.com/d' /etc/apt/sources.list; \
    fi; \
    # Prove apt is healthy now. `set -e` means a still-broken repo fails the
    # image build here, with a clear error, instead of failing later inside an
    # opaque feature-install step.
    apt-get update; \
    rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# JDK 17, for PySpark.
#
# batch_jobs' adapter and its conformance test run a local SparkSession with no
# cluster, no Kafka and no database — which still needs a JVM on this machine.
# 17 is what Spark 3.5.x targets; it moves when the pyspark pin in
# batch_jobs/pyproject.toml and the spark image tags move.
#
# Installed here rather than with ghcr.io/devcontainers/features/java, which was
# tried first and left `java: command not found`. That feature installs into
# SDKMAN and reaches PATH only via a *nested* containerEnv reference
# (${SDKMAN_DIR}/candidates/java/current/bin:${PATH}); this devcontainer.json
# also overrides PATH in remoteEnv for uv, and the two layers don't compose
# reliably. apt's openjdk registers /usr/bin/java through alternatives, so it is
# on PATH by construction and needs no plumbing at all.
#
# Same reasoning that put the Yarn fix in this file: a step we control beats a
# feature we have to debug from inside a container we can't ssh into.
# ---------------------------------------------------------------------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless; \
    rm -rf /var/lib/apt/lists/*; \
    # Resolve the real install directory (arch-dependent: .../java-17-openjdk-amd64
    # vs -arm64) by walking back from the alternatives symlink, then pin a stable
    # name so JAVA_HOME below doesn't have to know the architecture.
    ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/current; \
    # Fail the build loudly here rather than at the first pytest run.
    java -version

# PySpark checks JAVA_HOME before falling back to PATH; set it explicitly so
# neither path depends on shell profile files being sourced.
ENV JAVA_HOME=/usr/lib/jvm/current
