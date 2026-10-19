"""Offboarding a partner: freeze, report, and a delete that needs approval.

Deleting a past partner's data is a decision for leadership and legal, so nothing irreversible
happens without it:

- **freeze** (routine, reversible): the partner's master key is disabled, so every identifier of the
  partner is unreadable in every layer, snapshot, backup and copy at once; its inbound storage
  identity is disabled; its schedules pause; the loader refuses its sources. `unfreeze` undoes all
  of it. A freeze can stand indefinitely (a legal hold).
- **report** (read-only): everything a delete would remove, as counts and names (never values), with
  a hash, recorded in the journal, that the approval refers to.
- **delete** (manual, after sign-off): refuses unless the partner is frozen, two different approvers
  are named, and a fresh report has the approved hash. It records the approval in the write-once
  journal first, then removes the partner's master key, tables, raw files, identity and key entries,
  principals and schedules, and (by choice) the Gold tables and Doris copies built from its data.
  Each step is recorded, so a delete that stops part-way resumes where it stopped.

On AWS the loader cannot delete a master key: only the partner-deletion role may schedule that,
assumed by an operator (infra/sandbox/kms.tf). The delete waits until the key is pending deletion,
which leaves KMS's 7 to 30 day window as the last chance to cancel.
"""
import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from loader import admin, duck, journal, partner_keys, raw, tables
from loader.contracts import Contract, Email
from loader.polaris import Polaris, vended_s3
from loader.settings import Settings

log = logging.getLogger(__name__)
PARTNERS = "partners"
ACTIVE, FROZEN, DELETING, DELETED = "active", "frozen", "deleting", "deleted"
DBT_MODELS = Path(__file__).resolve().parents[1] / "dbt" / "models"
STEPS = ("master_key", "gold", "tables", "raw", "identity", "access")


class PartnerFrozen(RuntimeError):
    """The partner is frozen (or being deleted): nothing of it is loaded."""


class Refused(RuntimeError):
    """A delete precondition does not hold."""


class NeedsKeyDeletion(RuntimeError):
    """On AWS an operator must schedule deletion of the partner's master key first."""


# ---------------------------------------------------------------- state

def state(s: Settings, partner: str) -> dict:
    item = s.dynamodb().get_item(TableName=PARTNERS, Key={"partner": {"S": partner}}, ConsistentRead=True).get("Item")
    if not item:
        return {"partner": partner, "state": ACTIVE, "done_steps": []}
    out = {k: v.get("S", v.get("SS")) for k, v in item.items()}
    out["done_steps"] = sorted(item.get("done_steps", {}).get("SS", []))
    return out


def _set_state(s: Settings, partner: str, new: str, **fields) -> None:
    item = {"partner": {"S": partner}, "state": {"S": new}, "since": {"S": _now()},
            **{k: {"S": str(v)} for k, v in fields.items()}}
    s.dynamodb().put_item(TableName=PARTNERS, Item=item)


def check_active(s: Settings, contract: Contract) -> None:
    """Called before a source is loaded."""
    current = state(s, contract.partner)["state"]
    if current != ACTIVE:
        raise PartnerFrozen(f"partner {contract.partner} is {current}: {contract.source} is not loaded")


# ---------------------------------------------------------------- which sources

def sources_of(s: Settings, partner: str, contracts: dict[str, Contract]) -> list[str]:
    """Every source of the partner: the contracts that name it, and any source whose KEK its master
    key wraps (a source whose contract is gone still has data)."""
    from loader.vault import KEKS

    found = {c.source for c in contracts.values() if c.partner == partner}
    for item in _scan(s, KEKS):
        if item.get("partner", {}).get("S") == partner:
            found.add(item["source"]["S"])
    return sorted(found)


def _scan(s: Settings, table: str, **kwargs) -> list[dict]:
    ddb, items, kwargs = s.dynamodb(), [], {"TableName": table, "ConsistentRead": True, **kwargs}
    while True:
        page = ddb.scan(**kwargs)
        items += page.get("Items", [])
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


# ---------------------------------------------------------------- inbound storage identity

def inbound_user(partner: str) -> str:
    return f"partner-{partner}"


def ensure_inbound(s: Settings, contract: Contract) -> None:
    """Locally: the partner's own storage user, put-only into the drop prefixes of its sources (on AWS a
    role per partner, from Terraform). Each deployed source is added to the partner's set."""
    if s.is_aws:
        return
    path = s.state_dir / "partner-sources.json"
    known = json.loads(path.read_text()) if path.exists() else {}
    sources = sorted(set(known.get(contract.partner, [])) | {contract.source})
    known[contract.partner] = sources
    path.write_text(json.dumps(known, indent=1))
    bucket = s.bucket(raw.LOGICAL)
    admin.ensure_storage_user(s, inbound_user(contract.partner), [bucket], statements=[
        {"Effect": "Allow", "Action": ["s3:PutObject"],
         "Resource": [f"arn:aws:s3:::{bucket}/drops/{source}/*" for source in sources]}])
    if state(s, contract.partner)["state"] != ACTIVE:
        _set_inbound(s, contract.partner, enabled=False)  # deploying a frozen partner's source keeps it frozen


def _set_inbound(s: Settings, partner: str, enabled: bool) -> None:
    if s.is_aws:
        import boto3

        iam, role = boto3.client("iam"), f"{os.environ['LOADER_PARTNER_ROLE_PREFIX']}{partner}"
        if enabled:
            try:
                iam.delete_role_policy(RoleName=role, PolicyName="frozen")
            except iam.exceptions.NoSuchEntityException:
                pass
        else:
            iam.put_role_policy(RoleName=role, PolicyName="frozen", PolicyDocument=json.dumps(
                {"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Action": "*", "Resource": "*"}]}))
        return
    if inbound_user(partner) in json.loads((s.state_dir / "storage.json").read_text()):
        status, body = admin.rustfs_admin(s, "PUT", f"/set-user-status?accessKey={inbound_user(partner)}"
                                          f"&status={'enabled' if enabled else 'disabled'}")
        if status != 200:
            raise RuntimeError(f"set-user-status {inbound_user(partner)}: HTTP {status} {body[:200]!r}")


def _schedules(sources: list[str], action: str) -> list[str]:
    """Pause, resume or delete the sources' ingest deployments; returns the names acted on."""
    from prefect.client.orchestration import get_client
    from prefect.exceptions import ObjectNotFound

    async def run() -> list[str]:
        done = []
        async with get_client() as client:
            for source in sources:
                try:
                    deployment = await client.read_deployment_by_name(f"ingest/{source}")
                except ObjectNotFound:
                    continue
                if action == "pause":
                    await client.pause_deployment(deployment.id)
                elif action == "resume":
                    await client.resume_deployment(deployment.id)
                else:
                    await client.delete_deployment(deployment.id)
                done.append(f"ingest/{source}")
        return done

    return asyncio.run(run())


# ---------------------------------------------------------------- freeze

def freeze(s: Settings, partner: str, contracts: dict[str, Contract], actor: str, reason: str) -> dict:
    current = state(s, partner)["state"]
    if current in (DELETING, DELETED):
        raise Refused(f"partner {partner} is {current}")
    sources = sources_of(s, partner, contracts)
    journal.record_partner_event(s, partner, {"event": "freeze", "actor": actor, "reason": reason, "sources": sources})
    _set_state(s, partner, FROZEN, actor=actor, reason=reason)  # the loader stops first
    keys = s.partner_keys()
    if keys.state(partner) == partner_keys.ENABLED:
        keys.set_state(partner, partner_keys.DISABLED)
    partner_keys.forget(partner)
    _set_inbound(s, partner, enabled=False)
    paused = _schedules(sources, "pause")
    log.warning("partner %s frozen by %s (%d sources)", partner, actor, len(sources))
    return {"partner": partner, "state": FROZEN, "sources": sources, "paused": paused,
            "master_key": keys.state(partner)}


def unfreeze(s: Settings, partner: str, contracts: dict[str, Contract], actor: str, reason: str) -> dict:
    current = state(s, partner)["state"]
    if current != FROZEN:
        raise Refused(f"partner {partner} is {current}, not frozen")
    sources = sources_of(s, partner, contracts)
    journal.record_partner_event(s, partner, {"event": "unfreeze", "actor": actor, "reason": reason})
    keys = s.partner_keys()
    keys.set_state(partner, partner_keys.ENABLED)
    partner_keys.forget(partner)
    _set_inbound(s, partner, enabled=True)
    resumed = _schedules(sources, "resume")
    _set_state(s, partner, ACTIVE, actor=actor, reason=reason)
    log.warning("partner %s unfrozen by %s", partner, actor)
    return {"partner": partner, "state": ACTIVE, "resumed": resumed, "master_key": keys.state(partner)}


# ---------------------------------------------------------------- report

def gold_dependents(sources: list[str]) -> list[dict]:
    """Gold models that read any of the sources' Silver tables, and their Doris copies."""
    out = []
    for path in sorted(DBT_MODELS.rglob("*.sql")):
        reads = set(re.findall(r"source\(\s*'silver'\s*,\s*'([a-z0-9_]+)'\s*\)", path.read_text()))
        hit = sorted(t for t in reads if any(t.startswith(f"{src}__") for src in sources))
        if hit:
            catalog = path.parent.name  # dbt/models/<catalog>/<model>.sql
            out.append({"model": f"{catalog}.gold.{path.stem}", "reads": hit, "doris_copy": f"serving.{path.stem}",
                        "deidentified": catalog == "deid"})
    return out


def _tables(s: Settings, source: str) -> list[tuple[str, str]]:
    """(namespace, table) of the source's Bronze and Silver tables."""
    root, found = Polaris(s), []
    for layer in (tables.BRONZE, tables.SILVER):
        status, body = root.rest("GET", root.table_url(tables.CATALOG, layer) + "/tables")
        if status == 200:
            found += [(layer, i["name"]) for i in body.get("identifiers", []) if i["name"].startswith(f"{source}__")]
    return sorted(found)


def _table_stats(s: Settings, namespace: str, table: str) -> dict:
    meta = Polaris(s, "maintenance").load_table(tables.CATALOG, namespace, table)["metadata"]
    current = meta.get("current-snapshot-id")
    stats = {"table": f"{tables.CATALOG}.{namespace}.{table}", "snapshots": len(meta.get("snapshots", [])),
             "rows": 0, "data_files": 0, "data_bytes": 0}
    if current not in (None, -1):
        con = duck.connect(s, tables.CATALOG, principal="maintenance")
        rows, files = con.execute(
            f"SELECT coalesce(sum(record_count), 0), count(*) FROM iceberg_metadata('ice.{namespace}.{table}') "
            "WHERE status <> 'DELETED' AND manifest_content = 'DATA'").fetchone()
        summary = next((sn.get("summary", {}) for sn in meta.get("snapshots", []) if sn["snapshot-id"] == current), {})
        stats.update(rows=int(rows), data_files=int(files), data_bytes=int(summary.get("total-files-size", 0)))
    return stats


def _raw_prefixes(source: str, contracts: dict[str, Contract]) -> list[str]:
    from loader import partners

    prefixes = partners.source_prefixes(source)
    contract = contracts.get(source)
    if contract and isinstance(contract.fetch, Email):
        prefixes += [f"inbox/{contract.fetch.mailbox}/", f"quarantine/inbox/{contract.fetch.mailbox}/"]
    return prefixes


def _raw_objects(s: Settings, prefixes: list[str]) -> list[dict]:
    base = raw.RawZone(s)  # the operator's view (report and delete run with the admin identity)
    out = []
    for prefix in prefixes:
        for page in base.s3.get_paginator("list_objects_v2").paginate(Bucket=base.bucket, Prefix=prefix):
            out += page.get("Contents", [])
    return out


# What an approval binds: what would be deleted. Snapshot and file counts are left out (maintenance
# compacts and expires a frozen partner's tables too, without changing what a delete removes).
_VOLATILE = ("snapshots", "data_files", "data_bytes")


def inventory_hash(inventory: dict) -> str:
    stable = {**inventory, "master_key": {"id": inventory["master_key"]["id"]},
              "tables": [{k: v for k, v in t.items() if k not in _VOLATILE} for t in inventory["tables"]]}
    return hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest()


def report(s: Settings, partner: str, contracts: dict[str, Contract], actor: str = "report") -> dict:
    """Everything a delete would remove, as counts and names. Read-only, apart from recording its hash
    in the journal."""
    from loader.identity import SUBJECTS
    from loader.shred import BATCH_SUBJECTS
    from loader.state import MANIFEST
    from loader.vault import KEKS, KEY_VAULT

    sources = sources_of(s, partner, contracts)
    keks = [i["kek_id"]["S"] for i in _scan(s, KEKS) if i["source"]["S"] in sources]
    subjects = [i for i in _scan(s, SUBJECTS) if i.get("source", {}).get("S") in sources]
    keys = s.partner_keys()
    inventory = {
        "partner": partner,
        "sources": sources,
        "master_key": {"id": keys.key_id(partner), "state": keys.state(partner)},
        "tables": [_table_stats(s, ns, t) for src in sources for ns, t in _tables(s, src)],
        "raw": [{"prefix": p, "objects": len(objs), "bytes": sum(o["Size"] for o in objs)}
                for src in sources for p in _raw_prefixes(src, contracts) for objs in [_raw_objects(s, [p])]],
        "batches": sum(1 for i in _scan(s, MANIFEST) if i["source"]["S"] in sources),
        "batch_subject_entries": sum(1 for i in _scan(s, BATCH_SUBJECTS)
                                     if any(i["batch_id"]["S"].startswith(f"{src}-") for src in sources)),
        "subjects": len(subjects),
        "persons": len({i["person_id"]["S"] for i in subjects}),
        "keks": sorted(keks),
        "subject_keys": sum(1 for i in _scan(s, KEY_VAULT) if i.get("kek_id", {}).get("S") in keks),
        "principals": [f"src_{src}" for src in sources],
        "schedules": [f"ingest/{src}" for src in sources],
        "gold": gold_dependents(sources),
        "kept": "the write-once journal and decrypt audit entries (IDs and counts, no PHI) and CloudTrail",
    }
    digest = inventory_hash(inventory)
    journal.record_partner_event(s, partner, {"event": "report", "actor": actor, "report_hash": digest})
    notes = []
    if inventory["gold"]:
        notes.append("Gold models read this partner's tables: change them in dbt (a reviewed pull request) "
                     "before the next Gold run, whichever Gold option the delete uses")
    return {"generated_at": _now(), "report_hash": digest, "inventory": inventory, "notes": notes}


# ---------------------------------------------------------------- delete

def delete(s: Settings, partner: str, contracts: dict[str, Contract], request: str, approvers: list[str],
           report_hash: str, gold: str, actor: str) -> dict:
    """Remove all of the partner's data after approval (module docstring). Resumable by request."""
    if gold not in ("drop", "keep"):
        raise ValueError("gold must be drop or keep")
    current = state(s, partner)
    if current["state"] == DELETED:
        raise Refused(f"partner {partner} is already deleted")
    if current["state"] == DELETING:
        if current.get("request") != request:
            raise Refused(f"a delete of {partner} under request {current.get('request')} is in progress; resume it")
        gold = current.get("gold", gold)
    else:
        if current["state"] != FROZEN:
            raise Refused(f"partner {partner} must be frozen before a delete (it is {current['state']})")
        if len({a.strip().lower() for a in approvers if a.strip()}) < 2:
            raise Refused("a delete needs two different approvers")
        fresh = report(s, partner, contracts, actor=actor)["report_hash"]
        if fresh != report_hash:
            raise Refused("the partner's inventory changed since the approved report; report again and re-approve")
        journal.record_partner_event(s, partner, {"event": "delete-approved", "request": request, "actor": actor,
                                                  "approvers": sorted(approvers), "report_hash": report_hash, "gold": gold})
        _set_state(s, partner, DELETING, request=request, approvers=",".join(sorted(approvers)),
                   report_hash=report_hash, gold=gold, actor=actor)
    sources = sources_of(s, partner, contracts)
    done = set(state(s, partner)["done_steps"])
    results = {}
    for step in STEPS:
        if step in done:
            continue
        results[step] = _STEP[step](s, partner, sources, contracts, gold)
        s.dynamodb().update_item(TableName=PARTNERS, Key={"partner": {"S": partner}},
                                 UpdateExpression="ADD done_steps :s", ExpressionAttributeValues={":s": {"SS": [step]}})
        log.warning("delete of partner %s: %s done", partner, step)
    journal.record_partner_event(s, partner, {"event": "delete-completed", "request": request, "actor": actor,
                                              "results": results})
    _set_state(s, partner, DELETED, request=request, actor=actor)
    return {"partner": partner, "state": DELETED, "steps": results}


def _step_master_key(s, partner, sources, contracts, gold) -> str:
    keys = s.partner_keys()
    current = keys.state(partner)
    if s.is_aws:
        if current not in (partner_keys.PENDING_DELETION, partner_keys.DELETED):
            raise NeedsKeyDeletion(
                f"schedule deletion of {keys.alias(partner)} as the partner-deletion role "
                f"(scripts/sandbox.py schedule-key-deletion {partner}), then rerun this delete to resume")
        return current
    if current != partner_keys.DELETED:
        keys.set_state(partner, partner_keys.DELETED)
    partner_keys.forget(partner)
    return partner_keys.DELETED


def _drop_table(s: Settings, catalog: str, namespace: str, table: str) -> int:
    """Delete the table's files with credentials vended for it, then drop it without purge (Polaris's
    purge does not work on AWS with session tags). The lake keeps deleted objects' earlier versions
    for 3 days, then they expire."""
    maintenance, root = Polaris(s, "maintenance"), Polaris(s)
    status, _ = root.rest("GET", root.table_url(catalog, namespace, table))
    if status == 404:
        return 0
    body = maintenance.load_table(catalog, namespace, table, vended=True)
    s3 = s.s3(vended_s3(body))
    bucket, prefix = body["metadata"]["location"].removeprefix("s3://").split("/", 1)
    deleted = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys[i:i + 1000], "Quiet": True})
        deleted += len(keys)
    status, body = root.rest("DELETE", root.table_url(catalog, namespace, table))
    if status not in (204, 404):
        raise RuntimeError(f"dropping {catalog}.{namespace}.{table}: HTTP {status}")
    return deleted


def _step_gold(s, partner, sources, contracts, gold) -> dict:
    dependents = gold_dependents(sources)
    if gold == "keep":
        return {"kept": [d["model"] for d in dependents]}
    from loader import doris

    dropped = []
    for d in dependents:
        catalog, namespace, table = d["model"].split(".")
        _drop_table(s, catalog, namespace, table)
        try:
            doris.query(doris.connect(), f"DROP TABLE IF EXISTS {d['doris_copy']}")
        except Exception as err:  # noqa: BLE001 (Doris is optional in the local stack)
            log.warning("Doris copy %s not dropped: %s", d["doris_copy"], type(err).__name__)
        dropped.append(d["model"])
    return {"dropped": dropped}


def _step_tables(s, partner, sources, contracts, gold) -> dict:
    return {f"{tables.CATALOG}.{ns}.{t}": _drop_table(s, tables.CATALOG, ns, t) for src in sources for ns, t in _tables(s, src)}


def _step_raw(s, partner, sources, contracts, gold) -> int:
    base, deleted = raw.RawZone(s), 0
    for src in sources:
        for obj in _raw_objects(s, _raw_prefixes(src, contracts)):
            base.delete(obj["Key"])
            deleted += 1
    return deleted


def _step_identity(s, partner, sources, contracts, gold) -> dict:
    from loader.identity import SUBJECTS, Identity
    from loader.shred import BATCH_SUBJECTS
    from loader.state import MANIFEST
    from loader.vault import KEKS, KEY_VAULT

    ddb = s.dynamodb()
    keks = {i["kek_id"]["S"] for i in _scan(s, KEKS) if i["source"]["S"] in sources}
    subject_ids = [i["subject_id"]["S"] for i in _scan(s, SUBJECTS) if i.get("source", {}).get("S") in sources]
    Identity(s).forget(subject_ids)
    key_items = [i for i in _scan(s, KEY_VAULT) if i.get("kek_id", {}).get("S") in keks]
    for i in key_items:
        ddb.delete_item(TableName=KEY_VAULT, Key={"subject_id": i["subject_id"]})
    for kid in keks:
        ddb.delete_item(TableName=KEKS, Key={"kek_id": {"S": kid}})
    batches = [i for i in _scan(s, MANIFEST) if i["source"]["S"] in sources]
    for i in batches:
        ddb.delete_item(TableName=MANIFEST, Key={"batch_id": i["batch_id"]})
    entries = [i for i in _scan(s, BATCH_SUBJECTS) if any(i["batch_id"]["S"].startswith(f"{src}-") for src in sources)]
    for i in entries:
        ddb.delete_item(TableName=BATCH_SUBJECTS, Key={"batch_id": i["batch_id"]})
    return {"subjects": len(subject_ids), "subject_keys": len(key_items), "keks": len(keks), "batches": len(batches),
            "batch_subject_entries": len(entries)}


def _step_access(s, partner, sources, contracts, gold) -> dict:
    root, removed = Polaris(s), []
    for src in sources:
        principal = f"src_{src}"
        for path in (f"/principals/{principal}", f"/principal-roles/{principal}_role",
                     f"/catalogs/{tables.CATALOG}/catalog-roles/{principal}"):
            root.mgmt("DELETE", path)
        s.delete_principal(principal)
        removed.append(principal)
    schedules = _schedules(sources, "delete")
    if not s.is_aws and inbound_user(partner) in json.loads((s.state_dir / "storage.json").read_text()):
        admin.rustfs_admin(s, "DELETE", f"/remove-user?accessKey={inbound_user(partner)}")
    return {"principals": removed, "schedules": schedules,
            "inbound_identity": "role stays denied until the partner leaves the contracts and Terraform" if s.is_aws
            else "removed"}


_STEP = {"master_key": _step_master_key, "gold": _step_gold, "tables": _step_tables, "raw": _step_raw,
         "identity": _step_identity, "access": _step_access}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
