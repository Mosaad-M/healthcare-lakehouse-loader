#!/usr/bin/env bash
# Security scanners, the same locally (`make scan`) and in CI. Fails on any finding.
#   pip-audit  locked Python dependencies against known vulnerabilities
#   gitleaks   secrets anywhere in the git history (.gitleaksignore: reviewed false positives)
#   semgrep    static analysis of the code (nosemgrep comments say why a line is accepted)
#   trivy      Terraform, Dockerfiles and compose (config), and the built images (vulnerabilities
#              with a fix available, and secrets); .trivyignore.yaml and #trivy:ignore say why
# Tools run from pinned images, so results do not depend on what is installed.
set -euo pipefail
cd "$(dirname "$0")/.."

GITLEAKS=zricethezav/gitleaks:v8.28.0
SEMGREP=semgrep/semgrep:1.140.0
TRIVY=aquasec/trivy:0.67.2
IMAGES=(lakehouse-loader/toolbox:dev lakehouse-loader/prefect-worker:dev lakehouse-loader/prefect-worker-ecs:dev)

echo "== pip-audit"
uv export --frozen --no-hashes --no-emit-project --all-groups --format requirements-txt > /tmp/scan-requirements.txt
uvx pip-audit==2.10.1 --disable-pip --no-deps --strict -r /tmp/scan-requirements.txt

echo "== gitleaks"
docker run --rm -v "$PWD":/repo "$GITLEAKS" git /repo --redact --no-banner

echo "== semgrep"
docker run --rm -v "$PWD":/src -w /src "$SEMGREP" semgrep scan --metrics=off --error --quiet \
  --config p/python --config p/security-audit --config p/secrets .

echo "== trivy config"
docker run --rm -v "$PWD":/src -w /src "$TRIVY" config --quiet --severity HIGH,CRITICAL \
  --ignorefile .trivyignore.yaml --exit-code 1 .

echo "== trivy images"
docker compose -f stack/compose.yaml --env-file .env build bootstrap prefect-worker
docker build -q -t lakehouse-loader/prefect-worker-ecs:dev stack/prefect-worker-ecs >/dev/null
for image in "${IMAGES[@]}"; do
  echo "-- $image"
  docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v trivy-cache:/root/.cache "$TRIVY" image \
    --quiet --scanners vuln,secret --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 "$image"
done
echo "scan: clean"
