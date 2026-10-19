# The Prefect worker on AWS: starts each flow run as an ECS task (the toolbox task definition).
FROM prefecthq/prefect:3.8.8-python3.12
# Security fixes released since the base image was built (trivy).
RUN apt-get update && apt-get -y upgrade && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir prefect-aws==0.7.12
RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin worker
USER worker
