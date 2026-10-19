"""On real S3, the lake buckets refuse every identity but their Polaris vending role, even one
whose IAM policy grants full S3 access; vended credentials reach exactly one table."""
import os

import boto3
import botocore.exceptions
import pytest

from loader.polaris import Polaris, vended_s3

pytestmark = [pytest.mark.aws]


def denied(fn) -> bool:
    try:
        fn()
        return False
    except botocore.exceptions.ClientError as err:
        return err.response["Error"]["Code"] in ("AccessDenied", "403")


@pytest.fixture(scope="module")
def probe(s):
    creds = boto3.client("sts", region_name=s.region).assume_role(
        RoleArn=os.environ["LOADER_GUARDRAIL_PROBE_ROLE"], RoleSessionName="guardrail-test")["Credentials"]
    return boto3.client("s3", region_name=s.region, aws_access_key_id=creds["AccessKeyId"],
                        aws_secret_access_key=creds["SecretAccessKey"], aws_session_token=creds["SessionToken"])


@pytest.mark.parametrize("bucket", ["lake-phi", "lake-deid"])
def test_full_s3_rights_in_iam_do_not_open_the_lake(s, probe, bucket):
    name = s.bucket(bucket)
    assert denied(lambda: probe.list_objects_v2(Bucket=name))
    assert denied(lambda: probe.put_object(Bucket=name, Key="probe/x", Body=b"x"))
    assert denied(lambda: probe.get_object(Bucket=name, Key="silver/anything"))  # gitleaks:allow (an object key)


def test_the_loader_task_role_cannot_reach_the_lake(s):
    task_role = s.s3("admin")  # on AWS: the toolbox task role
    for bucket in ("lake-phi", "lake-deid"):
        assert denied(lambda b=bucket: task_role.list_objects_v2(Bucket=s.bucket(b)))


def test_vended_credentials_reach_one_table_on_real_s3(s):
    """The C2 check against S3 and STS themselves: session policies scoped to the table location."""
    polaris = Polaris(s, "auditor")
    status, body = polaris.rest("GET", polaris.table_url("phi", "silver") + "/tables")
    names = [i["name"] for i in body["identifiers"]]
    assert len(names) >= 2
    one, other = (polaris.load_table("phi", "silver", n, vended=True) for n in names[:2])
    s3 = s.s3(vended_s3(one))
    bucket = s.bucket("lake-phi")
    loc_one = one["metadata"]["location"].removeprefix(f"s3://{bucket}/")
    loc_other = other["metadata"]["location"].removeprefix(f"s3://{bucket}/")
    assert not denied(lambda: s3.list_objects_v2(Bucket=bucket, Prefix=loc_one + "/"))
    assert denied(lambda: s3.list_objects_v2(Bucket=bucket, Prefix=loc_other + "/"))
    assert denied(lambda: s3.put_object(Bucket=bucket, Key=loc_one + "/data/x", Body=b"x"))  # read-only principal
