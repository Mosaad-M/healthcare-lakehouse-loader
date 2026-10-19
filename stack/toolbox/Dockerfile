# The loader image. It also runs the bootstrap and the test suites, so every client talks to
# the stack from inside the compose network and sees the same endpoints Polaris vends.
FROM python:3.12-slim

# Security fixes released since the base image was built (trivy).
# libpq for psycopg (tests): Debian's, patched with the OS, instead of the copy psycopg-binary bundles.
RUN apt-get update && apt-get -y upgrade && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_CACHE=1 \
    PATH=/opt/venv/bin:$PATH \
    LOADER_DUCKDB_EXTENSION_DIR=/opt/duckdb-extensions

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --group dev

# Bake DuckDB extensions into the image: the loader never downloads code at run time.
RUN python -c "import duckdb, os; c = duckdb.connect(); \
c.execute(\"SET extension_directory = '\" + os.environ['LOADER_DUCKDB_EXTENSION_DIR'] + \"'\"); \
[c.install_extension(e) for e in ('httpfs', 'aws', 'iceberg', 'avro')]"

COPY . .
RUN uv sync --frozen --group dev

# Not root. Directories that get named volumes (state; the CDC sink's spool) are made here, so
# a new volume mounted on them belongs to the user.
RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin loader \
    && mkdir -p /state /spool && chown loader /state /spool
USER loader
