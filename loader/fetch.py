"""Fetchers: get a source's data into its drop prefix, exactly as received. Everything after that
(landing, Bronze, Silver) is the same for every source.

- file_drop: nothing to do; the partner drops files themselves.
- http_api: request the contract's date window from the partner API, page by page, and land all
  pages as one file (a JSON array of the response bodies, byte for byte), so one fetch is one batch:
  a window restated across pages is replaced as a whole in Silver.
- email: read raw messages from raw/inbox/<mailbox>/, keep only allowed senders, drop each matching
  attachment, and delete the message (bodies are never kept). Other messages are quarantined.
- staged: move the files a capture tool wrote to its staging bucket into the drop prefix.

Errors carry status codes and our own words only: response bodies can hold PHI and the request
carries the partner's token, so neither reaches an exception, a log or the raw zone's key names.
"""
import email
import email.policy
import email.utils
import fnmatch
import hashlib
import ipaddress
import json
import logging
import os
import random
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

import botocore.exceptions

from loader.contracts import Contract, Email, HttpApi, Staged
from loader.raw import RawZone, drop_prefix
from loader.settings import Settings

log = logging.getLogger(__name__)
RETRYABLE = {429, 500, 502, 503, 504}


def _gone(err: botocore.exceptions.ClientError) -> bool:
    """The object was removed between listing and reading: another run (scheduled and manual, or
    two workers) took it first."""
    return err.response["Error"]["Code"] in ("NoSuchKey", "404")


class FetchFailed(RuntimeError):
    """A fetch could not complete. The message is safe to log (no bodies, no tokens)."""


def fetch(s: Settings, contract: Contract) -> int:
    """Fetch new data for a source into its drop prefix. Returns the number of files dropped."""
    if isinstance(contract.fetch, HttpApi):
        return fetch_http_api(s, contract, contract.fetch)
    if isinstance(contract.fetch, Email):
        return fetch_email(s, contract, contract.fetch)
    if isinstance(contract.fetch, Staged):
        return fetch_staged(s, contract, contract.fetch)
    return 0


def fetch_staged(s: Settings, contract: Contract, staged: Staged) -> int:
    """Move staged files into the drop prefix, oldest first (copy, then delete). Their names (DMS:
    schema/table/timestamp) are kept, flattened. Returns the number of files moved."""
    zone, bucket = RawZone(s), s.bucket(staged.bucket)
    objects = [o for page in zone.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=staged.prefix)
               for o in page.get("Contents", []) if not o["Key"].endswith("/")]
    for obj in sorted(objects, key=lambda o: (o["LastModified"], o["Key"])):
        name = "-".join(part for part in obj["Key"].removeprefix(staged.prefix).split("/") if part)
        try:
            zone.s3.copy_object(Bucket=zone.bucket, Key=f"{drop_prefix(contract)}{name}",
                                CopySource={"Bucket": bucket, "Key": obj["Key"]})
        except botocore.exceptions.ClientError as err:
            if not _gone(err):
                raise
            continue  # another run moved it
        zone.s3.delete_object(Bucket=bucket, Key=obj["Key"])
    if objects:
        log.info("moved %d staged files into the drop prefix", len(objects))
    return len(objects)


def inbox_prefix(mailbox: str) -> str:
    return f"inbox/{mailbox}/"


def fetch_email(s: Settings, contract: Contract, mail: Email) -> int:
    """Turn each inbound message into dropped attachments. Returns the number of files dropped.

    Nothing about a message is logged beyond counts: senders, subjects and file names can all name
    a patient. Attachment names are replaced by the message hash and an index.
    """
    zone, dropped, quarantined = RawZone(s), 0, 0
    for key in zone.keys(inbox_prefix(mail.mailbox)):
        try:
            raw_message = zone.read(key)
        except botocore.exceptions.ClientError as err:
            if not _gone(err):
                raise
            continue  # another run took this message

        message = email.message_from_bytes(raw_message, policy=email.policy.default)
        sender = email.utils.parseaddr(message.get("From", ""))[1].lower()
        attachments = [part for part in message.iter_attachments()
                       if fnmatch.fnmatch((part.get_filename() or "").lower(), mail.attachment_glob.lower())]
        if sender not in mail.allowed_senders or not attachments:
            reason = "sender not allowed" if sender not in mail.allowed_senders else "no matching attachment"
            target = f"quarantine/inbox/{mail.mailbox}/{key.rsplit('/', 1)[1]}"
            zone.put(target, raw_message)
            zone.delete(key)
            quarantined += 1
            log.warning("inbound message quarantined: %s", reason)
            continue
        digest = hashlib.sha256(raw_message).hexdigest()[:16]
        ext = contract.format.type if contract.format.type != "json_pages" else "json"
        for index, part in enumerate(attachments):
            content = part.get_payload(decode=True) or b""
            if len(content) > mail.max_attachment_bytes:
                raise FetchFailed(f"attachment {index} exceeds {mail.max_attachment_bytes} bytes")
            zone.put(f"{drop_prefix(contract)}mail-{digest}-{index}.{ext}", content)
            dropped += 1
        zone.delete(key)  # the attachments are in the drop prefix; the body is not kept
    if dropped or quarantined:
        log.info("inbound mail: %d attachments dropped, %d messages quarantined", dropped, quarantined)
    return dropped


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """A redirect could take the request (and its bearer token) to an address check_destination
    never saw, such as the cloud metadata endpoint. Partners get the exact URL they publish."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirects are not followed", headers, fp)


_OPENER = urllib.request.build_opener(_NoRedirects)


def check_destination(url: str) -> None:
    """Refuse addresses a partner API never has: loopback, link-local (the ECS and EC2 metadata
    endpoints live there), multicast; and private networks unless the platform allows them
    (LOADER_FETCH_ALLOW_PRIVATE=1: the local stack's and the sandbox's mock partner)."""
    host = urllib.parse.urlsplit(url).hostname or ""
    allow_private = os.environ.get("LOADER_FETCH_ALLOW_PRIVATE") == "1"
    try:
        addresses = {info[4][0] for info in socket.getaddrinfo(host, None)}
    except socket.gaierror:
        raise FetchFailed("partner API host does not resolve") from None
    for address in addresses:
        ip = ipaddress.ip_address(address.split("%", 1)[0])
        if ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_unspecified or ip.is_reserved:
            raise FetchFailed("partner API address is not allowed")
        if ip.is_private and not allow_private:
            raise FetchFailed("partner API address is private (set LOADER_FETCH_ALLOW_PRIVATE=1 to allow)")


def _get(url: str, token: str, api: HttpApi) -> bytes:
    check_destination(url)
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
    for attempt in range(api.max_retries + 1):
        try:
            with _OPENER.open(request, timeout=api.timeout_seconds) as response:
                return response.read()
        except urllib.error.HTTPError as err:
            err.read()  # drain, never surface
            if err.code not in RETRYABLE or attempt == api.max_retries:
                raise FetchFailed(f"partner API answered HTTP {err.code}") from None
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt == api.max_retries:
                raise FetchFailed("partner API unreachable") from None
        delay = min(30.0, 0.5 * 2 ** attempt) * random.uniform(0.75, 1.25)
        log.warning("partner API request failed; retrying in %.1f s (attempt %d)", delay, attempt + 1)
        time.sleep(delay)
    raise AssertionError("unreachable")


def fetch_http_api(s: Settings, contract: Contract, api: HttpApi, today: date | None = None) -> int:
    token = s.source_secret(api.token_secret)
    today = today or datetime.now(timezone.utc).date()
    start = today - timedelta(days=api.days_back - 1)
    pages, records, page = [], 0, 1
    while page is not None:
        if len(pages) >= api.pagination.max_pages:
            raise FetchFailed(f"more than {api.pagination.max_pages} pages; raise max_pages in the contract")
        query = urllib.parse.urlencode({api.window_from_param: start.isoformat(), api.window_to_param: today.isoformat(),
                                        api.pagination.page_param: page, api.pagination.size_param: api.pagination.page_size})
        body = _get(f"{api.url}?{query}", token, api)
        try:
            parsed = json.loads(body)
        except ValueError:
            raise FetchFailed(f"page {page} is not JSON") from None
        rows = parsed.get(contract.format.records_path) if isinstance(parsed, dict) else None
        if not isinstance(rows, list):
            raise FetchFailed(f"page {page} has no '{contract.format.records_path}' list")
        pages.append(body)
        records += len(rows)
        page = parsed.get(api.pagination.next_field)
    if records == 0:
        log.info("partner API returned no records for %s to %s; nothing landed", start, today)
        return 0
    key = f"{drop_prefix(contract)}api-{start}-{today}-{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}.json"
    RawZone(s).put(key, b"[" + b",".join(pages) + b"]")
    log.info("fetched %d records in %d pages for %s to %s", records, len(pages), start, today)
    return 1
