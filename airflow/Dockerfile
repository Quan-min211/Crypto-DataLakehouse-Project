FROM apache/airflow:2.8.1-python3.10

# =============================================================================
# Custom Airflow image with Docker CLI
# =============================================================================
# All Airflow DAGs use `docker exec spark-master ...` to submit Spark jobs.
# The base Airflow image does NOT include Docker CLI, causing:
#   "docker: command not found"
#
# This Dockerfile installs ONLY the Docker CLI (not the daemon).
# The Docker daemon runs on the host — Airflow talks to it via the
# mounted socket (/var/run/docker.sock).
# =============================================================================

USER root

# Install Docker CLI only (not the full daemon)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    rm -rf /var/lib/apt/lists/*

# Allow airflow user to use Docker socket
RUN groupadd -f docker && usermod -aG docker airflow

USER airflow
