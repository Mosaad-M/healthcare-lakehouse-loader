"""Administrative helpers used by the bootstrap and the contract deploy step (root credentials)."""
import json
import secrets
import urllib.error
import urllib.request

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from loader.polaris import Polaris, expect
from loader.settings import Settings


def _state(s: Settings, name: str) -> dict:
    path = s.state_dir / name
    return json.loads(path.read_text()) if path.exists() else {}


def _save_state(s: Settings, name: str, data: dict) -> None:
    s.state_dir.mkdir(parents=True, exist_ok=True)
    (s.state_dir / name).write_text(json.dumps(data, indent=2))


def ensure_principal(root: Polaris, s: Settings, name: str, catalog_roles: dict[str, list[str]]) -> bool:
    """Create principal `name` with principal role `<name>_role`, granted the given catalog roles
    ({catalog: [role, ...]}). Secrets are kept in the state volume (Secrets Manager on AWS); a
    principal whose secret is not on file is reset. Returns True when credentials were (re)issued."""
    status, body = root.mgmt("POST", "/principals", {"principal": {"name": name}})
    issued = None
    if status == 201:
        issued = body["credentials"]
    elif status == 409 and s.principal_secret(name) is None:
        issued = expect(root.mgmt("POST", f"/principals/{name}/reset", {}), 200)["credentials"]
    elif status != 409:
        raise RuntimeError(f"create principal {name}: HTTP {status} {body}")
    if issued:
        s.save_principal(name, issued)
    prole = f"{name}_role"
    root.mgmt("POST", "/principal-roles", {"principalRole": {"name": prole}})
    expect(root.mgmt("PUT", f"/principals/{name}/principal-roles", {"principalRole": {"name": prole}}), 200, 201)
    for catalog, roles in catalog_roles.items():
        for role in roles:
            expect(root.mgmt("PUT", f"/principal-roles/{prole}/catalog-roles/{catalog}",
                             {"catalogRole": {"name": role}}), 200, 201)
    return issued is not None


def ensure_catalog_role(root: Polaris, catalog: str, role: str, grants: list[dict]) -> None:
    root.mgmt("POST", f"/catalogs/{catalog}/catalog-roles", {"catalogRole": {"name": role}})
    for grant in grants:
        expect(root.mgmt("PUT", f"/catalogs/{catalog}/catalog-roles/{role}/grants", {"grant": grant}), 200, 201)


# ---- RustFS (MinIO-compatible admin API, SigV4-signed JSON)

def rustfs_admin(s: Settings, method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = AWSRequest(method=method, url=f"{s.s3_endpoint}/rustfs/admin/v3{path}", data=data,
                     headers={"Content-Type": "application/json"} if data else {})
    S3SigV4Auth(Credentials(s.s3_access_key, s.s3_secret_key), "s3", "us-east-1").add_auth(req)
    http_req = urllib.request.Request(req.url, data=data, headers=dict(req.headers), method=method)
    try:
        with urllib.request.urlopen(http_req, timeout=30) as resp:  # nosemgrep: dynamic-urllib-use-detected (configured service URL, not input)
            return resp.status, resp.read()
    except urllib.error.HTTPError as err:
        return err.code, err.read()


def restore_statements(buckets: list[str]) -> list[dict]:
    """lake-restore's rights (infra/sandbox/iam.tf on AWS): list and read earlier versions, and write
    only by copying an object of the same bucket. It cannot write new content or delete anything."""
    return [
        {"Effect": "Allow", "Action": ["s3:ListBucketVersions", "s3:ListBucket"], "Resource": [f"arn:aws:s3:::{b}" for b in buckets]},
        {"Effect": "Allow", "Action": ["s3:GetObjectVersion"], "Resource": [f"arn:aws:s3:::{b}/*" for b in buckets]},
        *({"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": [f"arn:aws:s3:::{b}/*"],
           "Condition": {"StringLike": {"s3:x-amz-copy-source": [f"{b}/*", f"/{b}/*"]}}} for b in buckets)]


def ensure_storage_user(s: Settings, user: str, buckets: list[str], actions: list[str] | None = None,
                        prefix: str = "", statements: list[dict] | None = None) -> None:
    """A RustFS user limited to `buckets` and `actions` (default: everything on those buckets), and
    to objects under `prefix` when one is given, or with exactly `statements`. On AWS this is an IAM
    role with the same scope. The secret is kept in the state volume."""
    users = _state(s, "storage.json")
    if user not in users:
        users[user] = {"accessKey": user, "secretKey": secrets.token_urlsafe(24)}
    status, body = rustfs_admin(s, "PUT", f"/add-user?accessKey={user}",
                                {"secretKey": users[user]["secretKey"], "status": "enabled"})
    if status != 200:
        raise RuntimeError(f"rustfs add-user {user}: HTTP {status} {body[:200]!r}")
    resources = ([f"arn:aws:s3:::{b}/{prefix}*" for b in buckets] if prefix else
                 [r for b in buckets for r in (f"arn:aws:s3:::{b}", f"arn:aws:s3:::{b}/*")])
    policy = f"{user}-policy"
    status, body = rustfs_admin(s, "PUT", f"/add-canned-policy?name={policy}", {
        "Version": "2012-10-17",
        "Statement": statements or [{"Effect": "Allow", "Action": actions or ["s3:*"], "Resource": resources}]})
    if status != 200:
        raise RuntimeError(f"rustfs add-canned-policy {policy}: HTTP {status} {body[:200]!r}")
    status, body = rustfs_admin(s, "POST", "/idp/builtin/policy/attach", {"policies": [policy], "user": user})
    if status != 200 and b"already" not in body.lower():
        raise RuntimeError(f"rustfs attach {policy}: HTTP {status} {body[:200]!r}")
    _save_state(s, "storage.json", users)
