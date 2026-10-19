# Local only. Runs as root: it starts flow containers through the Docker socket, which is root on the
# VM anyway. On AWS the ECS worker (prefect-worker-ecs) is not root.
FROM prefecthq/prefect:3.8.8-python3.12
# Security fixes released since the base image was built (trivy).
RUN apt-get update && apt-get -y upgrade && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir prefect-docker==0.7.4
