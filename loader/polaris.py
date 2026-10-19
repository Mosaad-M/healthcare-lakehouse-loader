"""A small client for the Polaris management, Iceberg REST and policy APIs."""
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from urllib.parse import quote

from loader.settings import Settings

VENDED = {"X-Iceberg-Access-Delegation": "vended-credentials"}


class PolarisError(RuntimeError):
    def __init__(self, status: int, body):
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body


def http(method: str, url: str, body=None, token: str | None = None, form: dict | None = None,
         headers: dict | None = None):
    """Returns (status, parsed body). Never raises on HTTP errors; callers decide."""
    hdrs = dict(headers or {})
    data = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        hdrs["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        hdrs["Content-Type"] = "application/json"
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:  # nosemgrep: dynamic-urllib-use-detected (configured service URL, not input)
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as err:
        raw = err.read()
        try:
            return err.code, json.loads(raw)
        except ValueError:
            return err.code, raw.decode(errors="replace")


def expect(result, *ok: int):
    status, body = result
    if status not in ok:
        raise PolarisError(status, body)
    return body


@dataclass
class Polaris:
    """Calls Polaris as one principal. scope 'PRINCIPAL_ROLE:ALL' activates all its roles."""

    settings: Settings
    principal: str = "root"
    scope: str = "PRINCIPAL_ROLE:ALL"
    credentials: tuple[str, str] | None = None  # (client_id, secret); default: from the bootstrap
    _token: str | None = field(default=None, repr=False)

    @property
    def token(self) -> str:
        if self._token is None:
            client_id, secret = self.credentials or self.settings.principal(self.principal)
            self._token = expect(http("POST", self.settings.token_uri, form={
                "grant_type": "client_credentials", "client_id": client_id,
                "client_secret": secret, "scope": self.scope}), 200)["access_token"]
        return self._token

    # -- management API
    def mgmt(self, method: str, path: str, body=None):
        return http(method, f"{self.settings.polaris_management_uri}{path}", body=body, token=self.token)

    # -- Iceberg REST API (prefix = catalog name in Polaris)
    def table_url(self, catalog: str, namespace: str, table: str | None = None) -> str:
        url = f"{self.settings.polaris_uri}/v1/{quote(catalog, safe='')}/namespaces/{quote(namespace, safe='')}"
        return f"{url}/tables/{quote(table, safe='')}" if table else url

    def rest(self, method: str, url: str, body=None, vended: bool = False):
        return http(method, url, body=body, token=self.token, headers=VENDED if vended else None)

    def load_table(self, catalog: str, namespace: str, table: str, vended: bool = False) -> dict:
        return expect(self.rest("GET", self.table_url(catalog, namespace, table), vended=vended), 200)

    def update_table(self, catalog: str, namespace: str, table: str, updates: list[dict],
                     requirements: list[dict] | None = None) -> dict:
        """Commit raw Iceberg metadata updates (properties, sort orders, snapshot references); DuckDB
        has no SQL for these."""
        return expect(self.rest("POST", self.table_url(catalog, namespace, table),
                                {"requirements": requirements or [], "updates": updates}), 200)

    # -- policy API
    def policy_url(self, catalog: str, path: str = "") -> str:
        return f"{self.settings.polaris_uri}/polaris/v1/{quote(catalog, safe='')}{path}"


def vended_s3(load_result: dict) -> dict:
    """Storage credentials from a loadTable/createTable response (both spec shapes)."""
    for cred in load_result.get("storage-credentials") or []:
        if "s3.access-key-id" in cred.get("config", {}):
            return cred["config"]
    return load_result.get("config", {})
