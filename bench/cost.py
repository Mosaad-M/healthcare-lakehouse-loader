"""AWS cost of measured work: counts from a benchmark step times AWS list prices.

Prices are us-east-1 on-demand list prices as known in October 2026; check them against the AWS
price list before relying on a figure. Only usage-driven costs are modelled; fixed infrastructure
(NAT gateway, RDS for Polaris and Prefect, Doris hosts, Prefect services) is listed separately in
the report because it does not grow with data.
"""
from dataclasses import dataclass

PRICES = {
    # S3 Standard requests and storage
    "s3.write_per_1k": 0.005,  # PUT, COPY, POST, LIST
    "s3.read_per_1k": 0.0004,  # GET, HEAD
    "s3.gb_month": 0.023,
    # S3 Intelligent-Tiering: automatic tiers only (no retrieval fees); objects under 128 KB are not
    # monitored and stay at the Frequent rate
    "s3it.frequent_gb_month": 0.023,
    "s3it.infrequent_gb_month": 0.0125,  # not read for 30 days
    "s3it.archive_instant_gb_month": 0.004,  # not read for 90 days
    "s3it.monitoring_per_1k_objects": 0.0025,
    # Fargate Spot: up to 70% off on-demand Fargate (interruptible, two minutes' notice)
    "fargate.spot_discount": 0.70,
    # KMS: every wrap (Encrypt) and unwrap (Decrypt) of a subject's data key is one request
    "kms.per_10k": 0.03,
    # DynamoDB on demand: one request unit per item (up to 1 KB written, 4 KB strongly read)
    "ddb.write_per_million": 0.625,
    "ddb.read_per_million": 0.125,
    "ddb.gb_month": 0.25,
    # Fargate, ARM: the loader task is 2 vCPU and 8 GB
    "fargate.vcpu_hour": 0.03238,
    "fargate.gb_hour": 0.00356,
    # Firehose Direct PUT: per GB ingested, each record rounded up to 5 KB
    "firehose.gb": 0.029,
    "firehose.record_rounding_bytes": 5 * 1024,
}
TASK_VCPU, TASK_GB = 2, 8
TASK_HOUR = TASK_VCPU * PRICES["fargate.vcpu_hour"] + TASK_GB * PRICES["fargate.gb_hour"]


@dataclass
class Usage:
    """Usage-driven quantities, in the units AWS bills."""
    s3_writes: float = 0
    s3_reads: float = 0
    kms_requests: float = 0
    ddb_writes: float = 0
    ddb_reads: float = 0
    task_seconds: float = 0

    def __add__(self, other: "Usage") -> "Usage":
        return Usage(**{k: getattr(self, k) + getattr(other, k) for k in self.__dataclass_fields__})

    def scaled(self, factor: float) -> "Usage":
        return Usage(**{k: getattr(self, k) * factor for k in self.__dataclass_fields__})

    def dollars(self) -> dict[str, float]:
        p = PRICES
        out = {
            "s3_requests": self.s3_writes / 1000 * p["s3.write_per_1k"] + self.s3_reads / 1000 * p["s3.read_per_1k"],
            "kms": self.kms_requests / 10_000 * p["kms.per_10k"],
            "dynamodb": self.ddb_writes / 1e6 * p["ddb.write_per_million"] + self.ddb_reads / 1e6 * p["ddb.read_per_million"],
            "compute": self.task_seconds / 3600 * TASK_HOUR,
        }
        out["total"] = sum(out.values())
        return out


def usage(result: dict) -> Usage:
    """The billable usage of one benchmark step (its RESULT)."""
    c = result.get("counts", {})
    writes = sum(v for k, v in c.items() if k.split(".")[-1] in ("PUT", "POST")) \
        + sum(c.get(f"aws.s3.{op}", 0) for op in ("PutObject", "CopyObject", "ListObjectsV2", "DeleteObject"))
    reads = sum(v for k, v in c.items() if k.startswith("http.storage.") and k.split(".")[-1] in ("GET", "HEAD")) \
        + sum(c.get(f"aws.s3.{op}", 0) for op in ("GetObject", "HeadObject"))
    # Catalog requests go to Polaris, not S3; only storage requests are S3 requests. DeleteObject
    # is free on S3 but counted as a write here, which overstates slightly.
    writes -= sum(v for k, v in c.items() if k.startswith("http.catalog.") and k.split(".")[-1] in ("PUT", "POST"))
    return Usage(s3_writes=writes, s3_reads=reads, kms_requests=c.get("keys.wrap", 0) + c.get("keys.unwrap", 0),
                 ddb_writes=c.get("ddb.items.write", 0), ddb_reads=c.get("ddb.items.read", 0),
                 task_seconds=result.get("wall_s", 0))


def firehose_dollars(events: int, event_bytes: int) -> float:
    """Firehose rounds every record up to 5 KB: small events cost far more than their size."""
    billed = events * max(event_bytes, PRICES["firehose.record_rounding_bytes"])
    return billed / 1024 ** 3 * PRICES["firehose.gb"]
