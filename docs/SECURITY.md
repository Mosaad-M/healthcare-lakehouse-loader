# Security

How the loader protects regulated health data: what is worth protecting, the threats at each trust
boundary, the control for each and the test that fails if the control breaks, the cryptography, and
the risks accepted. **The stack is for synthetic data only; never put real PHI in it.**

To report a vulnerability, open a private security advisory on the repository (Security tab), not
a public issue.

## Assets

| Asset | Where | Why it matters |
|---|---|---|
| Plaintext PHI | Raw zone (drops, landed files, inbox, staging, quarantine); loader memory during Bronze | Readable by anyone who gets it |
| Identifier ciphertext | Bronze, Silver, Gold, Doris copies | Safe only while keys stay in the vault |
| Subject data keys | Key vault (DynamoDB), wrapped by a per-source key-encryption key, itself wrapped by the partner's master key (KMS) | An unwrapped key decrypts every identifier of its subject |
| Partner master keys | KMS, one per partner (a state-volume key locally) | Disabling one freezes a partner; deleting one is irreversible |
| Identity pepper | Secrets Manager (`.env` locally) | With it, guessed identifiers can be tested against the identity map |
| Principal and source credentials | Secrets Manager (a state volume locally) | Catalog access per table; partner API tokens |
| Audit records | Decrypt audit and shred journal (Object Lock), CloudTrail | Proof of who saw what, and that an erasure happened |
| Table integrity | Iceberg tables, batch manifest | Wrong or lost rows in clinical and financial data |

Actors: partners and source systems (untrusted input), the loader and its per-source principals,
Gold and maintenance jobs, serving users (Doris roles), the decrypt service and its callers,
operators (break-glass), CI and developers, the supply chain (images, packages), and an attacker
inside the network.

## Threats and controls

STRIDE per trust boundary. **Tested** means a test fails if the control breaks; **partial** and
**accepted** say what is missing and why.

### Outside world to the raw zone (drops, inbox, CDC staging, streams)

| Threat | Control | Status |
|---|---|---|
| S: a partner drops files for another source | Each partner drops with its own identity, put-only into its own sources' drop prefixes (a role per partner on AWS, a storage user locally); it cannot read, list or delete anything (`test_a_partner_can_only_drop_into_its_own_sources`, `test_a_partner_drop_role_reaches_only_its_own_drop_prefixes`) | tested |
| T: a writer into the raw zone alters landed files | Capture tools may only put, into their own prefix or bucket: the stream sink (`test_the_sink_can_only_put_into_drop_prefixes`), DMS only into `cdc-staging` | tested |
| I: patient names in file names reach logs and keys | Names never leave the drop prefix; only their hash is kept (`test_attachments_from_the_lab_are_loaded_and_the_message_is_not_kept`) | tested |
| I: mail bodies kept | Bodies deleted after extraction (`test_email.py`) | tested |
| D: a huge or decompression-bomb file exhausts the loader | Contract caps (`max_file_bytes`, 2 GiB; `max_rows`, 10M) checked before loading; over them the batch is quarantined (`test_oversized_files_are_quarantined_before_loading`); email attachments capped | tested |
| E: an `http_api` URL points inside the network (Polaris, the ECS credentials endpoint), or a partner redirects there with the bearer token | Loopback, link-local and metadata addresses always refused; private networks only with `LOADER_FETCH_ALLOW_PRIVATE=1`; redirects never followed (`test_fetch_guard.py`) | tested; DNS rebinding between check and connect accepted |

### Raw zone to the loader process (plaintext in memory)

| Threat | Control | Status |
|---|---|---|
| I: plaintext in a memory dump | Core dumps off in every process that imports the loader and in compose (`test_importing_the_loader_disables_core_dumps`) | tested |
| I: plaintext spilled to disk by DuckDB | Spill files encrypted (`temp_file_encryption`) in a private 0700 directory outside the working tree (`test_spill_files_are_encrypted_and_private`) | tested |
| I: plaintext on local disk | Drops are hashed as they stream, never copied locally; the staged Bronze batch lives in the private spill directory and is removed (`test_an_ingest_leaves_nothing_in_the_shared_temp_directory`); container disks are ephemeral (encrypted on Fargate) | tested |
| I: plaintext or tokens in DuckDB's HTTP log | Logging off; when on it records Authorization headers (`test_duckdb_http_logging_is_off`) | tested |
| I: plaintext in errors and logs | `duck.redact` strips literals; flows raise redacted errors only (`test_no_phi_leaks.py`, `test_redact_strips_values_from_errors`) | tested |
| I: plaintext in orchestration results | Prefect result persistence off; flow logs scanned for canary values (`test_canaries_are_absent_from_prefect_logs_and_states`) | tested |

### Loader to the lake (Polaris, vended credentials)

| Threat | Control | Status |
|---|---|---|
| E: a source principal touches another source's tables | Per-source principals granted their own two tables only (`tests/access`; on real S3, `test_vended_credentials_reach_one_table_on_real_s3`) | tested |
| E: direct S3 access to the lake | Bucket policies deny everyone but the catalog's vending roles, the restore role and break-glass, whatever IAM grants (`tests/aws/test_guardrail.py`) | tested on AWS |
| E: the restore role used to read or change the lake | It may only list versions, read earlier versions and copy an object back within its bucket; the bucket policy enforces the same; uploads, deletes and cross-bucket copies refused (`test_the_restore_identity_can_only_copy_versions_back`); every request in CloudTrail | tested |
| I: a load reads another source's or partner's raw files | Loads reach the raw zone only with short-lived credentials whose session policy admits their own source's prefixes (and mailbox or staging prefix); on AWS the flow role has no raw access of its own, only the right to assume the raw-access role (`test_a_load_reaches_only_its_own_source`) | tested |
| I: lost vended credentials send requests unsigned | A fail-closed catch-all storage secret: requests without a table's credentials go nowhere (`test_lake_requests_never_leave_the_stack`) | tested |
| I: PHI in Iceberg metadata (statistics, partition values, paths) | Column metrics limited to counts in the PHI catalog; partitions by ingest day only (`test_phi_metadata.py`) | tested |
| T: a writer changes table properties | Deploy re-asserts them; a catalog-side guard is not built | partial |
| R: no record of who read which table | CloudTrail data events; each vended session is named after its principal and table (`tests/aws/test_audit.py`) | tested on AWS |

### Lake to serving and to people (Gold, Doris, re-identification)

| Threat | Control | Status |
|---|---|---|
| I: analysts see identifiers | Gold is de-identified (small cells suppressed); Doris roles and row policies; copies hold ciphertext only (`tests/serving`) | tested |
| I: bulk re-identification | The decrypt service checks role, purpose and a subject cap per call, and audits every call (`test_erasure.py`) | tested |
| R: the decrypt audit is altered | Each event is first written as its own write-once object (Object Lock), then to the queryable table; no audit write, no decrypt; IAM forbids editing or deleting audit rows (`test_every_decrypt_event_is_kept_write_once`, `test_no_decrypt_without_the_write_once_audit`) | tested |
| T: spreadsheet formula injection from exported values | Doris copies prefix text starting with `=`, `+`, `-`, `@` with an apostrophe (`test_spreadsheet_formulas_are_neutralized_in_the_copies`) | tested |
| I: storage session tokens in serving logs | The Doris backend logs every vended S3 token at INFO, so it runs at WARNING (`test_the_backend_does_not_log_vended_tokens`); the frontend masks credentials | tested |

### Control plane (key vault, identity map, KMS, secrets)

| Threat | Control | Status |
|---|---|---|
| I: keys readable outside the loader | Subject keys wrapped by a per-source KEK, KEKs by the master key in KMS; unwrapped KEKs cached in process memory for 15 minutes (`test_key_hierarchy.py`) | tested |
| R: no per-subject record of key use | CloudTrail records KEK unwraps, not one per subject (one KMS request per batch is what keeps KMS cheap); re-identification is audited per person by the decrypt service | accepted |
| T: a restored vault brings shredded keys back | A restore locks the vault, re-applies every shred in the journal, then unlocks; reads check the gate before and after (`test_restoring_an_old_vault_backup_does_not_bring_keys_back`, `test_a_restore_that_starts_during_a_read_serves_no_keys`) | tested |
| T: a partner's data is deleted without approval, or too early | A delete refuses unless the partner is frozen, two different approvers are named and a fresh report has the approved hash; the approval is written to the write-once journal before anything is removed (`test_a_delete_refuses_without_freeze_approvals_or_the_approved_report`). On AWS no task can delete a partner's master key: its key policy reserves deletion for a role only the operator's SSO identity can assume (`test_no_task_role_can_schedule_deletion_of_a_partner_key`), and KMS waits 30 days before deleting, cancelable | tested |
| I: a frozen partner's identifiers stay readable | Its master key is disabled, so its KEKs cannot be unwrapped; cached KEKs are dropped at once in the freezing process and within 60 s elsewhere; its sources are not loaded and its drop identity is disabled (`test_a_freeze_stops_everything_and_unfreeze_restores_it`) | tested |
| T: a delete stops part-way | Each step is recorded; rerunning the same request resumes, another request is refused (`test_an_approved_delete_removes_everything_and_leaves_other_partners`) | tested |
| T: a vault backup brings a deleted partner back | The backup holds wrapped keys only; with the partner's master key gone they cannot be unwrapped (same test) | tested |
| E: every task holds administrator credentials | Flow runs get only what a flow needs (no catalog root, no admin secrets); Doris refreshes as a refresher role; bootstrap and tests use a separate admin task (`test_least_privilege.py`, `test_the_refresher_reads_gold_only`) | tested |

### Orchestration, supply chain and operations

| Threat | Control | Status |
|---|---|---|
| E: anyone in the network triggers or edits flows | Prefect API authentication on server, worker, flows and tests (`test_the_prefect_api_refuses_unauthenticated_requests`) | tested |
| T: a vulnerable or compromised dependency or image | Pinned lockfile and base images; OS packages patched at build; images run as a non-root user (the local Docker worker excepted); `make scan` on every pull request | tested |
| I: secrets in git or in images | gitleaks over the whole history; `.dockerignore` admits only what images need; trivy scans images for secrets | tested |

### Integrity under failure

| Threat | Control | Status |
|---|---|---|
| T: a committed data file goes missing | DuckDB 1.5.6 deletes the files of a commit whose answer it lost (see [DuckDB](../README.md#duckdb)). The loader waits up to 10 minutes for the catalog; a writer that hits a missing file, or whose commit succeeded only on a retry, heals its table at once: it copies the file back from its previous version (the lake keeps them 3 days) or rolls the table back to its newest whole snapshot and reloads the affected batches; every maintenance run checks too (`test_slow_commit.py`, `test_integrity.py`, the real failure end to end) | tested |
| D: DuckDB's Iceberg extension aborts the process on a failed metadata read | Refused connections retried; storage file limits raised; fixed in DuckDB 2.0 | partial |
| T: a crash between steps loses or duplicates rows | Every step idempotent by batch; a crash at any of four points is finished by the next run with every row once (`test_crash_recovery.py`) | tested |
| T: a crashed or stalled lock holder | Heartbeat leases (120 s, renewed every 30 s); a holder that lost its lease stops and its work is redone (`test_locks.py`) | tested |
| T: concurrent runs of one source | Locks serialize writes; batch IDs make repeats idempotent (`test_concurrency.py`) | tested |
| T: Silver semantics drift from their definition | Property tests compare the generated SQL with a reference model over random batch sequences (`test_silver_properties.py`) | tested |

## Cryptography

| Piece | Construction |
|---|---|
| Identifier values | AES-256-GCM under the subject's data key; a fresh 96-bit nonce from `os.urandom` per value; associated data `subject_id \0 column`, so a value moved to another subject or column does not decrypt. Layout: version (1 byte), nonce (12), ciphertext and tag |
| Subject data keys | 256-bit, one per (source, subject), wrapped by the source's KEK with AES-256-GCM and associated data `subject-key \0 kek_id \0 subject_id` |
| Key-encryption keys | One per source, wrapped by its partner's master key: KMS on AWS (the encryption context names partner and KEK), AES-256-GCM locally (associated data names partner and KEK), so a KEK cannot be moved to another partner. KEKs from before partner keys stay readable under the shared master key until `rewrap-keks` moves them |
| Partner master keys | One per partner. Disabled by a freeze (reversible); deleted only by an approved delete, on AWS only by the partner-deletion role |
| Erasure | Crypto-shredding: delete the subject's wrapped key after writing the journal entry. Every copy of their identifiers, in every layer, snapshot and backup, becomes unreadable at once |
| Blind index | HMAC-SHA256 with a secret pepper over the source name and the normalized key fields. The field separator is whitespace to the normalizer, so fields can never run together |
| Re-identification | Role, purpose and subject cap from policy; every call audited, write-once, before anything is decrypted |

**Nonce bounds.** With random 96-bit nonces NIST SP 800-38D allows at most 2^32 encryptions per
key. At a mid-size payer (5M members, 20 sources, 7 years) a subject key encrypts about 10,000
values (a repeat chance near 10^-21) and a KEK wraps at most about 20M subject keys (about 10^-15).
`os.urandom` is fork-safe, so worker processes do not repeat nonces. Tests cover volume, forks,
associated-data binding and version checks (`test_crypto.py`, `test_crypto_review.py`).

**What a stolen piece gives an attacker.** Lake files alone: ciphertext for identifiers and
plaintext for other columns, which bucket encryption, the bucket guardrail and per-table
credentials protect. The vault table alone: wrapped keys, useless without the KEKs. A KEK (in
process memory for at most 15 minutes after use): with the vault table, every subject key of one
source. The pepper: membership tests for guessed identifiers, not reversal.

**Not built yet.** KEK rotation: a new KEK version for new keys and a background re-wrap of
existing ones (the associated data already binds the KEK ID).

## Scanning

`make scan` runs locally and on every pull request:

| Tool | Checks |
|---|---|
| pip-audit | Locked Python dependencies against known vulnerabilities |
| gitleaks | Secrets anywhere in the git history |
| semgrep | Static analysis (Python, security audit and secrets rule packs) |
| trivy | Terraform, Dockerfiles and compose; the built images for fixable HIGH and CRITICAL vulnerabilities and for secrets |

Accepted findings carry their reason where they are accepted (`.trivyignore.yaml`, `#trivy:ignore`,
`nosemgrep`, `.gitleaksignore`).

## Accepted risks

- **Contracts are trusted code.** They name SQL identifiers and fetch targets, and change only by
  reviewed pull request. Validation (names, types, fetch kinds) limits mistakes, not malice.
- **The loader sees plaintext.** Encryption happens in the loader, so its memory holds one batch's
  identifiers. Mitigated by no swap, no core dumps and short-lived tasks.
- **Deleted lake objects are kept 3 days**, so a lost file can be restored. Erasure does not depend
  on deleting lake files (identifiers are crypto-shredded), and the raw zone, which holds plaintext,
  is not versioned and expires after 30 days.
- **A deleted partner's Gold models.** dbt models that read a deleted partner's tables fail on the
  next Gold run until they are changed (a reviewed pull request); the report lists them, and
  `--gold keep` keeps their de-identified aggregates if leadership and legal decide so.
- **Polaris purges on AWS.** Polaris 1.8 puts the list of files to delete into an STS session tag,
  which breaks STS's limits once session tags are on (they name principal and table in CloudTrail),
  so a table dropped with purge keeps its files. Drop without purge and let maintenance's orphan
  cleanup remove the location until Polaris fixes it.
