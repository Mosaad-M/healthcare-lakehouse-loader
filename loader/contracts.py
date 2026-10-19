"""Source contracts: one YAML file per source in contracts/, validated with pydantic.

A contract is the only thing a new file-drop source needs. It declares the file format, the
columns with their Iceberg types and PHI classification, the primary key, and how Silver merges.
"""
import re
from enum import Enum
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

NAME = r"^[a-z][a-z0-9_]{0,62}$"
ICEBERG_PRIMITIVES = {
    "boolean": "BOOLEAN", "int": "INTEGER", "long": "BIGINT", "float": "FLOAT", "double": "DOUBLE",
    "date": "DATE", "timestamp": "TIMESTAMP", "timestamptz": "TIMESTAMPTZ", "string": "VARCHAR",
    "binary": "BLOB",
}
DECIMAL = re.compile(r"^decimal\((\d{1,2}),\s*(\d{1,2})\)$")


def canonical_type(value: str) -> str:
    """Iceberg's own spelling (`decimal(12, 2)`), so contract and catalog types compare equal."""
    value = value.strip().lower()
    match = DECIMAL.match(value)
    if match:
        return f"decimal({int(match[1])}, {int(match[2])})"
    if value not in ICEBERG_PRIMITIVES:
        raise ValueError(f"unsupported Iceberg type {value!r}")
    return value


class Phi(str, Enum):
    """HIPAA classification of a column. The default is `phi`: unclassified data is treated as PHI.

    - none: not health information about a person (claim IDs, amounts, provider NPIs).
    - phi: health information or quasi-identifiers (dates of service, diagnosis codes, ZIP).
      Stored in the phi catalog, kept out of metadata (metrics `counts`), never a partition.
    - identifier: one of HIPAA Safe Harbor's 18 identifiers (names, SSNs, MRNs, emails ...).
      Encrypted per subject before Bronze or dropped.
    """

    NONE = "none"
    PHI = "phi"
    IDENTIFIER = "identifier"


class Column(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=NAME)
    type: str
    # The field's name in the landed file when it differs (e.g. Debezium's `__op`); the stored
    # column is always `name`. Lets a contract map a capture tool's format without rewriting bytes.
    source: str | None = None
    phi: Phi = Phi.PHI
    action: Literal["keep", "drop"] = "keep"
    description: str = ""

    @field_validator("type")
    @classmethod
    def _known_type(cls, value: str) -> str:
        return canonical_type(value)

    @property
    def file_field(self) -> str:
        return self.source or self.name

    @property
    def duckdb_type(self) -> str:
        match = DECIMAL.match(self.type)
        return f"DECIMAL({match[1]},{match[2]})" if match else ICEBERG_PRIMITIVES[self.type]


class FileFormat(BaseModel):
    """How a landed file is read. `json_pages`: a JSON array of API response bodies, records under
    `records_path` in each (what the http_api fetcher lands)."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["csv", "ndjson", "parquet", "json_pages"]
    delimiter: str = ","
    header: bool = True
    records_path: str | None = Field(default=None, pattern=NAME)

    @model_validator(mode="after")
    def _records_path(self) -> "FileFormat":
        if (self.type == "json_pages") != (self.records_path is not None):
            raise ValueError("records_path is required for json_pages and only allowed there")
        return self


class FileDrop(BaseModel):
    """The partner puts files in the drop prefix themselves (the default)."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["file_drop"] = "file_drop"


class Pagination(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page_param: str = "page"
    size_param: str = "page_size"
    page_size: int = Field(default=500, ge=1, le=10_000)
    next_field: str = "next_page"  # null or absent on the last page
    max_pages: int = Field(default=1000, ge=1)


class HttpApi(BaseModel):
    """Pull from a partner's HTTP API. Each fetch requests a window of `days_back` days ending today
    (partners restate recent days) and lands all pages as one file, so one fetch is one batch.

    `token_secret` names the bearer token: LOADER_SOURCE_SECRET_<NAME> locally, Secrets Manager
    `lakehouse/sources/<name>` on AWS. It never appears in contracts, logs or the raw zone.
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["http_api"]
    url: str = Field(pattern=r"^https?://")
    token_secret: str = Field(pattern=NAME)
    window_from_param: str = "from"
    window_to_param: str = "to"
    days_back: int = Field(default=7, ge=1, le=366)
    pagination: Pagination = Pagination()
    timeout_seconds: float = Field(default=30, gt=0)
    max_retries: int = Field(default=4, ge=0, le=10)


class Silver(BaseModel):
    """How Silver absorbs a Bronze batch.

    - append: add rows whose primary key is not in Silver yet (late duplicates are ignored).
    - snapshot: each batch is a full extract and replaces Silver.
    - window: each batch restates a range of `window_column` (a partner resending recent days);
      Silver rows in the batch's range are replaced (narshe's partner "window" reload).
    - cdc: each batch holds change events (`op_column`: I or U = upsert, D = delete, as AWS DMS
      writes them) ordered by `order_column` (the source's change sequence). Per key the latest change wins; a change older than
      the row Silver already holds is ignored, so replays and redeliveries are harmless.
    """

    model_config = ConfigDict(extra="forbid")

    semantics: Literal["append", "snapshot", "window", "cdc"]
    window_column: str | None = None
    op_column: str | None = None
    order_column: str | None = None


class Subject(BaseModel):
    """The columns that identify a person within this source (e.g. a member ID). They resolve to a
    random per-source subject ID through the identity service; every identifier in the row is then
    encrypted with that subject's key."""

    model_config = ConfigDict(extra="forbid")

    key: list[str] = Field(min_length=1)


class Email(BaseModel):
    """Attachments mailed to a mailbox. Inbound mail is stored as raw MIME under
    raw/inbox/<mailbox>/ (SES inbound to S3 on AWS; a small SMTP receiver locally). Only messages from
    `allowed_senders` count; their attachments matching `attachment_glob` are dropped, and the
    message itself is deleted: bodies are never kept."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["email"]
    mailbox: str = Field(pattern=r"^[a-z0-9._+-]+@[a-z0-9.-]+$")
    allowed_senders: list[str] = Field(min_length=1)
    attachment_glob: str = "*.csv"
    max_attachment_bytes: int = Field(default=50 * 1024 * 1024, ge=1)

    @field_validator("allowed_senders")
    @classmethod
    def _lower(cls, value: list[str]) -> list[str]:
        return [v.strip().lower() for v in value]


class Staged(BaseModel):
    """Files a capture tool writes to its own staging bucket (AWS DMS on AWS; cdc-sink locally),
    under `prefix`. Each fetch moves them, oldest first, into the drop prefix. The tool never gets
    access to the raw zone, not even to list it (DMS's bucket check needs list rights)."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["staged"]
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")  # logical name, e.g. cdc-staging
    prefix: str = Field(pattern=r"^[a-z0-9_]+/$")


class Stream(BaseModel):
    """Events published to the Kafka topic named after the source. The stream sink (Redpanda Connect
    locally, Firehose on AWS) writes them into the drop prefix as one NDJSON file every few seconds,
    so each file is one batch. Delivery is at least once: an event can arrive twice, in one file or
    two, so the primary key must be the event's own ID and Silver should use `append`."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["stream"]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(pattern=NAME)
    entity: str = Field(pattern=NAME)
    owner: str
    description: str = ""
    format: FileFormat
    columns: list[Column] = Field(min_length=1)
    primary_key: list[str] = Field(min_length=1)
    silver: Silver
    subject: Subject | None = None
    fetch: FileDrop | HttpApi | Email | Staged | Stream = Field(default_factory=FileDrop, discriminator="kind")
    poll_seconds: int = Field(default=60, ge=10)
    # Larger files are quarantined, not loaded. Defaults fit the loader's 8 GB task (benchmark:
    # 10M claims rows loaded, 30M ran out of memory); raise them with the task size.
    max_file_bytes: int = Field(default=2 * 1024 ** 3, ge=1)
    max_rows: int = Field(default=10_000_000, ge=1)

    @model_validator(mode="after")
    def _consistent(self) -> "Contract":
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError("duplicate column names")
        if any(n.startswith("_") or n in ("pk_bucket", "subject_id") for n in names):
            raise ValueError("column names starting with '_', 'pk_bucket' and 'subject_id' are reserved")
        kept = {c.name: c for c in self.kept}
        for key in self.primary_key:
            if key not in kept:
                raise ValueError(f"primary key column {key!r} must be a kept column")
            if kept[key].phi is Phi.IDENTIFIER:
                # Identifier keys are replaced by surrogate keys (the identity service).
                raise ValueError(f"primary key column {key!r} is an identifier; use a surrogate key")
        if self.identifiers and not self.subject:
            raise ValueError("kept identifier columns need a subject key to be encrypted with")
        for key in (self.subject.key if self.subject else []):
            if key not in kept or kept[key].phi is not Phi.IDENTIFIER:
                raise ValueError(f"subject key column {key!r} must be a kept identifier column")
        if isinstance(self.fetch, HttpApi) and self.format.type != "json_pages":
            raise ValueError("http_api sources land json_pages files")
        if isinstance(self.fetch, Stream) and (self.format.type != "ndjson" or self.silver.semantics != "append"):
            raise ValueError("stream sources land ndjson files and use append semantics")
        if self.silver.semantics == "cdc":
            op, order = kept.get(self.silver.op_column or ""), kept.get(self.silver.order_column or "")
            if op is None or op.type != "string" or op.phi is not Phi.NONE:
                raise ValueError("cdc semantics need op_column: a kept, non-PHI string column")
            orderable = order is not None and (order.type in ("long", "int", "timestamp", "timestamptz")
                                               or DECIMAL.match(order.type))
            if not orderable or order.phi is not Phi.NONE:
                raise ValueError("cdc semantics need order_column: a kept, non-PHI long, decimal or timestamp column")
        if self.silver.semantics == "window":
            col = kept.get(self.silver.window_column or "")
            if col is None or col.type not in ("date", "timestamp", "timestamptz"):
                raise ValueError("window semantics need window_column: a kept date or timestamp column")
        return self

    @property
    def kept(self) -> list[Column]:
        return [c for c in self.columns if c.action == "keep"]

    @property
    def identifiers(self) -> list[Column]:
        """Kept identifier columns: these must be encrypted before Bronze."""
        return [c for c in self.kept if c.phi is Phi.IDENTIFIER]

    @property
    def table(self) -> str:
        """Table name shared by the Bronze and Silver namespaces."""
        return f"{self.source}__{self.entity}"

    @property
    def principal(self) -> str:
        """The Polaris principal that owns this source's tables (one per pipeline)."""
        return f"src_{self.source}"


def load(path: Path) -> Contract:
    return Contract.model_validate(yaml.safe_load(path.read_text()))


def load_all(directory: Path = Path("contracts")) -> dict[str, Contract]:
    contracts = [load(p) for p in sorted(directory.glob("*.yaml"))]
    by_source = {c.source: c for c in contracts}
    if len(by_source) != len(contracts):
        raise ValueError("two contracts share a source name")
    return by_source
