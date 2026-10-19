"""On AWS: a partner's master key can be frozen by operators but deleted only by the partner-deletion
role, and each partner's drop role reaches only its own drop prefixes."""
import os
import uuid

import boto3
import botocore.exceptions
import pytest

from loader import contracts, raw

pytestmark = pytest.mark.aws


def denied(fn) -> bool:
    try:
        fn()
        return False
    except botocore.exceptions.ClientError as err:
        return err.response["Error"]["Code"] in ("AccessDenied", "AccessDeniedException", "403")


def test_no_task_role_can_schedule_deletion_of_a_partner_key(s):
    kms = boto3.client("kms", region_name=s.region)  # the admin task: operators' tooling, not the deletion role
    alias = f"{os.environ['LOADER_PARTNER_KEY_ALIAS_PREFIX']}acme"
    key = kms.describe_key(KeyId=alias)["KeyMetadata"]["KeyId"]
    assert denied(lambda: kms.schedule_key_deletion(KeyId=key, PendingWindowInDays=7))
    assert kms.describe_key(KeyId=key)["KeyMetadata"]["KeyState"] == "Enabled"


def test_a_partner_drop_role_reaches_only_its_own_drop_prefixes(s):
    by_source = contracts.load_all()
    acme, lab = by_source["acme_claims"], by_source["lab_results"]
    creds = boto3.client("sts", region_name=s.region).assume_role(
        RoleArn=f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/"
                f"{os.environ['LOADER_PARTNER_ROLE_PREFIX']}acme", RoleSessionName="partner-test")["Credentials"]
    s3 = boto3.client("s3", region_name=s.region, aws_access_key_id=creds["AccessKeyId"],
                      aws_secret_access_key=creds["SecretAccessKey"], aws_session_token=creds["SessionToken"])
    bucket, name = s.bucket(raw.LOGICAL), f"partner-test-{uuid.uuid4().hex[:8]}.csv"
    s3.put_object(Bucket=bucket, Key=f"{raw.drop_prefix(acme)}{name}", Body=b"x")
    assert denied(lambda: s3.put_object(Bucket=bucket, Key=f"{raw.drop_prefix(lab)}{name}", Body=b"x"))
    assert denied(lambda: s3.get_object(Bucket=bucket, Key=f"{raw.drop_prefix(acme)}{name}"))
    raw.RawZone(s).delete(f"{raw.drop_prefix(acme)}{name}")
