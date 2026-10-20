"""Partners: the organizations sources come from.

A contract names its partner. The partner is the unit of key isolation (each partner's key-encryption
keys are wrapped by its own master key) and of offboarding: freeze, report and an approved delete
(loader/offboarding.py).

Raw-zone access is scoped tighter still, to one source: every load works with short-lived
credentials whose session policy admits only that source's prefixes (`raw_scope`), so one source's
load can never read another's files, let alone another partner's. The same mechanism serves both
platforms: STS AssumeRole with a session policy, on AWS against a raw-access role, locally against
RustFS.
"""
import json

from loader.contracts import Contract, Email, Staged

RAW = "raw"  # the raw zone's logical bucket


def source_prefixes(source: str) -> list[str]:
    """Where one source's files live in the raw zone (raw.py has the layout)."""
    return [f"drops/{source}/", f"landed/{source}/", f"quarantine/{source}/"]


def raw_scope(contract: Contract) -> dict[str, list[str]]:
    """Logical bucket -> prefixes a load of this source may touch."""
    scope = {RAW: source_prefixes(contract.source)}
    if isinstance(contract.fetch, Email):
        scope[RAW] += [f"inbox/{contract.fetch.mailbox}/", f"quarantine/inbox/{contract.fetch.mailbox}/"]
    if isinstance(contract.fetch, Staged):
        scope[contract.fetch.bucket] = [contract.fetch.prefix]
    return scope


def session_policy(buckets: dict[str, list[str]], kms: bool) -> str:
    """An IAM session policy admitting only `buckets` (physical bucket -> prefixes): objects under
    the prefixes may be read, written and deleted, and only those prefixes may be listed."""
    statements = []
    for bucket, prefixes in sorted(buckets.items()):
        statements.append({"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                           "Resource": [f"arn:aws:s3:::{bucket}/{p}*" for p in prefixes]})
        statements.append({"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [f"arn:aws:s3:::{bucket}"],
                           "Condition": {"StringLike": {"s3:prefix": [f"{p}*" for p in prefixes]}}})
    if kms:  # the raw buckets are SSE-KMS on AWS; the role's own policy names the key
        statements.append({"Effect": "Allow", "Action": ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"],
                           "Resource": "*"})
    return json.dumps({"Version": "2012-10-17", "Statement": statements}, separators=(",", ":"))
