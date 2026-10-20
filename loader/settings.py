"""Settings read from the environment, and the one place that knows local from AWS.

LOADER_PLATFORM=local (default): RustFS, DynamoDB Local, static storage users, secrets in the state
volume. LOADER_PLATFORM=aws: S3, DynamoDB and KMS through the task's IAM role, secrets in Secrets
Manager, buckets named with LOADER_BUCKET_PREFIX. Callers ask for clients and DuckDB secrets here
and never branch on the platform themselves.
"""
import json
import os
from dataclasses import dataclass, field
from pathlib import Path

SECRET_PREFIX = "lakehouse/"
_scoped_cache: dict[str, tuple[dict, float]] = {}  # scoped_storage: scope -> (credentials, expiry)
# Storage identities that are roles of their own on AWS, assumed from the task's role (env: role ARN).
# lake-restore copies a lost lake file back from its previous version (loader/integrity.py).
ASSUMED_ROLES = {"lake-restore": "LOADER_LAKE_RESTORE_ROLE_ARN"}


@dataclass(frozen=True)
class Settings:
    polaris_uri: str  # Iceberg REST base, e.g. http://polaris:8181/api/catalog
    polaris_management_uri: str
    s3_endpoint: str  # empty on AWS
    dynamodb_endpoint: str  # empty on AWS
    spark_remote: str
    # Secrets are kept out of repr() so test failures and logs never print them.
    s3_access_key: str = field(repr=False)  # RustFS admin key (local only): bootstrap and operator checks
    s3_secret_key: str = field(repr=False)
    polaris_root_secret: str = field(repr=False)
    state_dir: Path = Path("stack/.state")
    duckdb_extension_dir: str | None = None
    platform: str = "local"
    region: str = "us-east-1"
    bucket_prefix: str = ""

    @property
    def is_aws(self) -> bool:
        return self.platform == "aws"

    @property
    def token_uri(self) -> str:
        return f"{self.polaris_uri}/v1/oauth/tokens"

    def bucket(self, name: str) -> str:
        """Physical bucket for a logical one (raw, lake-phi, lake-deid, shred-journal)."""
        return f"{self.bucket_prefix}{name}"

    # ---------------------------------------------------------------- AWS service clients

    def dynamodb(self):
        import boto3

        if self.is_aws:
            return boto3.client("dynamodb", region_name=self.region)
        return boto3.client("dynamodb", endpoint_url=self.dynamodb_endpoint, region_name=self.region,
                            aws_access_key_id="local", aws_secret_access_key="local")

    def s3(self, who: str | dict = "admin"):
        """An S3 client as `who`: a storage user (`loader-raw`, `shred-journal`), `admin` (local
        operator key), or a vended credential dict from Polaris. On AWS the storage users and admin
        are the task's own IAM role."""
        import boto3

        endpoint = self.s3_endpoint or None
        if isinstance(who, dict):
            return boto3.client("s3", endpoint_url=who.get("s3.endpoint") or endpoint, region_name=self.region,
                                aws_access_key_id=who["s3.access-key-id"], aws_secret_access_key=who["s3.secret-access-key"],
                                aws_session_token=who.get("s3.session-token"))
        if self.is_aws and who in ASSUMED_ROLES:
            creds = boto3.client("sts", region_name=self.region).assume_role(
                RoleArn=os.environ[ASSUMED_ROLES[who]], RoleSessionName=who)["Credentials"]
            return boto3.client("s3", region_name=self.region, aws_access_key_id=creds["AccessKeyId"],
                                aws_secret_access_key=creds["SecretAccessKey"], aws_session_token=creds["SessionToken"])
        if self.is_aws:
            return boto3.client("s3", region_name=self.region)
        key, secret = (self.s3_access_key, self.s3_secret_key) if who == "admin" else self.storage_user(who)
        return boto3.client("s3", endpoint_url=endpoint, region_name=self.region,
                            aws_access_key_id=key, aws_secret_access_key=secret)

    def scoped_storage(self, buckets: dict[str, list[str]], session: str) -> dict:
        """Short-lived storage credentials admitting only `buckets` (logical bucket -> prefixes), in
        the shape Polaris vends (usable with s3() and duckdb_s3_secret()). STS AssumeRole with a
        session policy: the raw-access role on AWS (LOADER_RAW_ACCESS_ROLE_ARN), RustFS locally as the
        loader-raw storage user. Cached per scope for most of their hour."""
        import time

        import boto3

        from loader import partners

        physical = {self.bucket(b): sorted(p) for b, p in buckets.items()}
        cache_key = json.dumps(physical, sort_keys=True)
        cached = _scoped_cache.get(cache_key)
        if cached and cached[1] > time.time():
            return cached[0]
        if self.is_aws:
            sts, role = boto3.client("sts", region_name=self.region), os.environ["LOADER_RAW_ACCESS_ROLE_ARN"]
        else:
            key, secret = self.storage_user("loader-raw")
            sts = boto3.client("sts", endpoint_url=self.s3_endpoint, region_name=self.region,
                               aws_access_key_id=key, aws_secret_access_key=secret)
            role = "arn:aws:iam::000000000000:role/raw-access"  # RustFS scopes by the session policy alone
        creds = sts.assume_role(RoleArn=role, RoleSessionName=f"raw-{session}"[:64], DurationSeconds=3600,
                                Policy=partners.session_policy(physical, kms=self.is_aws))["Credentials"]
        vended = {"s3.access-key-id": creds["AccessKeyId"], "s3.secret-access-key": creds["SecretAccessKey"],
                  "s3.session-token": creds["SessionToken"]}
        _scoped_cache[cache_key] = (vended, time.time() + 2700)
        return vended

    def duckdb_s3_secret(self, name: str, scope: str, who: str | dict = "admin") -> str:
        """CREATE SECRET for DuckDB with the same identity rules as s3()."""
        def q(v: str) -> str:
            return "'" + v.replace("'", "''") + "'"

        parts = [f"TYPE s3", f"SCOPE {q(scope)}", f"REGION {q(self.region)}"]
        if isinstance(who, dict):
            parts += [f"KEY_ID {q(who['s3.access-key-id'])}", f"SECRET {q(who['s3.secret-access-key'])}"]
            if who.get("s3.session-token"):
                parts.append(f"SESSION_TOKEN {q(who['s3.session-token'])}")
        elif self.is_aws:
            parts.append("PROVIDER credential_chain")
        else:
            key, secret = (self.s3_access_key, self.s3_secret_key) if who == "admin" else self.storage_user(who)
            parts += [f"KEY_ID {q(key)}", f"SECRET {q(secret)}"]
        if self.s3_endpoint:
            parts += [f"ENDPOINT {q(self.s3_endpoint.split('://', 1)[1])}", "URL_STYLE 'path'",
                      f"USE_SSL {str(self.s3_endpoint.startswith('https')).lower()}"]
        return f"CREATE OR REPLACE SECRET {name} ({', '.join(parts)})"

    # ---------------------------------------------------------------- secrets

    def storage_user(self, name: str) -> tuple[str, str]:
        """(access key, secret key) of a local storage user created by the bootstrap."""
        users = json.loads((self.state_dir / "storage.json").read_text())
        return users[name]["accessKey"], users[name]["secretKey"]

    def principal(self, name: str) -> tuple[str, str]:
        """(client_id, client_secret) for a Polaris principal created by the bootstrap."""
        if name == "root":
            return "root", self.polaris_root_secret
        creds = self.principal_secret(name)
        if creds is None:
            raise KeyError(f"no credentials on file for principal {name}")
        return creds["clientId"], creds["clientSecret"]

    # One secret per principal (lakehouse/principals/<name> on AWS): writes never depend on reading
    # another principal's entry, which Secrets Manager's eventual consistency would make unsafe.
    def principal_secret(self, name: str) -> dict | None:
        if self.is_aws:
            sm = self._secretsmanager()
            try:
                return json.loads(sm.get_secret_value(SecretId=f"{SECRET_PREFIX}principals/{name}")["SecretString"])
            except sm.exceptions.ResourceNotFoundException:
                return None
        path = self.state_dir / "principals.json"
        return json.loads(path.read_text()).get(name) if path.exists() else None

    def save_principal(self, name: str, creds: dict) -> None:
        if self.is_aws:
            import time

            sm, secret_id = self._secretsmanager(), f"{SECRET_PREFIX}principals/{name}"
            # A principal deleted just before (a source removed and deployed again) leaves its secret
            # "marked for deletion" for a few seconds even when forced (AWS benchmark): wait
            # until it is gone.
            for attempt in range(30):
                try:
                    sm.put_secret_value(SecretId=secret_id, SecretString=json.dumps(creds))
                    return
                except sm.exceptions.ResourceNotFoundException:
                    try:
                        sm.create_secret(Name=secret_id, SecretString=json.dumps(creds),
                                         Tags=[{"Key": "Project", "Value": "lakehouse-loader"}])
                        return
                    except sm.exceptions.InvalidRequestException:
                        pass  # still being deleted
                except sm.exceptions.InvalidRequestException:
                    pass  # marked for deletion
                time.sleep(2)
            raise RuntimeError(f"secret {secret_id} is still being deleted; try again")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        path = self.state_dir / "principals.json"
        all_creds = json.loads(path.read_text()) if path.exists() else {}
        all_creds[name] = creds
        path.write_text(json.dumps(all_creds, indent=2))

    def delete_principal(self, name: str) -> None:
        if self.is_aws:
            sm = self._secretsmanager()
            try:
                sm.delete_secret(SecretId=f"{SECRET_PREFIX}principals/{name}", ForceDeleteWithoutRecovery=True)
            except sm.exceptions.ResourceNotFoundException:
                pass
            return
        path = self.state_dir / "principals.json"
        if path.exists():
            all_creds = json.loads(path.read_text())
            all_creds.pop(name, None)
            path.write_text(json.dumps(all_creds, indent=2))

    def source_secret(self, name: str) -> str:
        """A source's credential (e.g. a partner API token): LOADER_SOURCE_SECRET_<NAME> when set
        (generated into .env locally; an ECS task may have it injected), otherwise Secrets Manager
        lakehouse/sources/<name> on AWS."""
        value = os.environ.get(f"LOADER_SOURCE_SECRET_{name.upper()}")
        if value:
            return value
        if self.is_aws:
            return self._secretsmanager().get_secret_value(SecretId=f"{SECRET_PREFIX}sources/{name}")["SecretString"]
        raise KeyError(f"source secret {name} is not configured")

    def _secretsmanager(self):
        import boto3

        return boto3.client("secretsmanager", region_name=self.region)

    def partner_keys(self):
        """Each partner's master key (loader/partner_keys.py): KMS on AWS, the state volume locally."""
        from loader import partner_keys

        prefix = os.environ.get("LOADER_PARTNER_KEY_ALIAS_PREFIX")
        return partner_keys.KmsPartnerKeys(self.region, prefix) if prefix else partner_keys.LocalPartnerKeys(self.state_dir)

    def key_provider(self):
        """KMS on AWS (LOADER_MASTER_KMS_KEY), the local key from .env otherwise."""
        from loader import crypto

        key_id = os.environ.get("LOADER_MASTER_KMS_KEY")
        return crypto.KmsKeyProvider(key_id, self.region) if key_id else crypto.LocalKeyProvider()


def load() -> Settings:
    env = os.environ
    platform = env.get("LOADER_PLATFORM", "local")
    aws = platform == "aws"
    return Settings(
        polaris_uri=env.get("POLARIS_URI", "http://polaris:8181/api/catalog"),
        polaris_management_uri=env.get("POLARIS_MANAGEMENT_URI", "http://polaris:8181/api/management/v1"),
        s3_endpoint=env.get("S3_ENDPOINT", "" if aws else "http://rustfs:9000"),
        dynamodb_endpoint=env.get("DYNAMODB_ENDPOINT", "" if aws else "http://dynamodb:8000"),
        spark_remote=env.get("SPARK_REMOTE", "sc://spark:15002"),
        s3_access_key=env.get("RUSTFS_ACCESS_KEY", ""),
        s3_secret_key=env.get("RUSTFS_SECRET_KEY", ""),
        polaris_root_secret=env.get("POLARIS_ROOT_SECRET", ""),
        state_dir=Path(env.get("LOADER_STATE_DIR", "stack/.state")),
        duckdb_extension_dir=env.get("LOADER_DUCKDB_EXTENSION_DIR"),
        platform=platform,
        region=env.get("AWS_REGION", "us-east-1"),
        bucket_prefix=env.get("LOADER_BUCKET_PREFIX", ""),
    )
