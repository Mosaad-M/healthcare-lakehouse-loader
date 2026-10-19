"""Operator commands for the identity, erasure and re-identification paths (run in the toolbox).

    python -m loader.cli reidentify --source acme_claims --subject <id> --caller me@example.org \\
        --role care_coordinator --purpose treatment
    python -m loader.cli link --subject <id> --subject <id>
    python -m loader.cli shred --person <id> --request <ticket> --actor me@example.org --reason patient_request
    python -m loader.cli check-integrity [--table phi.bronze.acme_claims__claims] [--repair]
    python -m loader.cli repair --recreate acme_claims --layer bronze --yes     (operator, root)
    python -m loader.cli vault-backup > backup.json
    python -m loader.cli vault-restore backup.json

Output never includes identifier values except from `reidentify`, which is audited.
"""
import argparse
import json
import sys
from pathlib import Path

from loader import contracts, decrypt, journal, settings
from loader.identity import Identity
from loader.shred import shred
from loader.vault import KeyVault


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="loader.cli")
    parser.add_argument("--contracts-dir", default="contracts", help="where the source contracts are")
    sub = parser.add_subparsers(dest="command", required=True)
    r = sub.add_parser("reidentify")
    r.add_argument("--source", required=True)
    r.add_argument("--subject", action="append", required=True)
    r.add_argument("--caller", required=True)
    r.add_argument("--role", required=True)
    r.add_argument("--purpose", required=True)
    r.add_argument("--column", action="append")
    link = sub.add_parser("link")
    link.add_argument("--subject", action="append", required=True)
    sh = sub.add_parser("shred")
    target = sh.add_mutually_exclusive_group(required=True)
    target.add_argument("--person")
    target.add_argument("--subject", action="append")
    sh.add_argument("--request", required=True)
    sh.add_argument("--actor", required=True)
    sh.add_argument("--reason", required=True)
    integ = sub.add_parser("check-integrity", help="every referenced file exists; --repair rolls back and re-queues")
    integ.add_argument("--table", action="append", help="catalog.namespace.table (default: every table)")
    integ.add_argument("--repair", action="store_true")
    rec = sub.add_parser("repair", help="operator only (root): recreate a table that has no whole snapshot")
    rec.add_argument("--recreate", required=True, help="the source whose table to recreate")
    rec.add_argument("--layer", choices=["bronze", "silver"], required=True)
    rec.add_argument("--yes", action="store_true", help="confirm: the table is dropped and reloaded from raw")
    sub.add_parser("vault-backup")
    restore = sub.add_parser("vault-restore")
    restore.add_argument("backup")
    args = parser.parse_args(argv)
    s = settings.load()

    if args.command == "reidentify":
        contract = contracts.load_all(Path(args.contracts_dir))[args.source]
        try:
            result = decrypt.reidentify(s, args.caller, args.role, args.purpose, contract, args.subject, args.column)
        except decrypt.Refused as err:
            sys.exit(f"refused: {err}")
        print(json.dumps({"audit_event": result.event_id, "unreadable": result.unreadable,
                          "rows": result.rows}, default=str, indent=2))
    elif args.command == "link":
        print(Identity(s).link(args.subject))
    elif args.command == "shred":
        result = shred(s, args.request, args.actor, args.reason, person_id=args.person, subject_ids=args.subject)
        print(json.dumps({"request": result.request_id, "subjects": len(result.subject_ids),
                          "raw_files_deleted": result.raw_files_deleted, "journal": result.journal_key}))
    elif args.command == "check-integrity":
        from loader import integrity, maintenance

        chosen = [tuple(t.split(".", 2)) for t in args.table] if args.table else maintenance.tables(s)
        failed = 0
        for catalog, namespace, table in chosen:
            report = integrity.check(s, catalog, namespace, table)
            out = {"table": report.table, "ok": report.ok, "referenced": report.referenced, "missing": report.missing[:20]}
            if not report.ok:
                failed += 1
                if args.repair:
                    try:
                        out["repair"] = integrity.repair(s, catalog, namespace, table)
                    except integrity.NeedsOperator as err:
                        out["repair"] = {"needs_operator": str(err)}
            print(json.dumps(out))
        if failed and not args.repair:
            sys.exit(f"{failed} tables reference missing files (rerun with --repair)")
    elif args.command == "repair":
        from loader import integrity

        if not args.yes:
            sys.exit("recreating drops the table and reloads every batch from the raw zone; add --yes")
        contract = contracts.load_all(Path(args.contracts_dir))[args.recreate]
        print(json.dumps(integrity.recreate(s, contract, args.layer)))
    elif args.command == "vault-backup":
        print(KeyVault(s).backup())
    elif args.command == "vault-restore":
        with open(args.backup) as fh:
            restored = KeyVault(s).restore(fh.read(), lambda: journal.shredded_subjects(s))
        print(f"restored {restored} keys; shred journal re-applied")


if __name__ == "__main__":
    main()
