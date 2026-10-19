"""Every stack service is up and reachable from the loader's network."""
import botocore.exceptions
import pytest


def test_dynamodb_lock_is_exclusive(s, run_id):
    """Table locks rely on conditional puts: the second claim of the same lock must fail."""
    ddb = s.dynamodb()
    item = {"lock_name": {"S": f"smoke_{run_id}"}, "owner": {"S": "first"}}
    ddb.put_item(TableName="loader_locks", Item=item, ConditionExpression="attribute_not_exists(lock_name)")
    with pytest.raises(botocore.exceptions.ClientError, match="ConditionalCheckFailed"):
        ddb.put_item(TableName="loader_locks", Item={**item, "owner": {"S": "second"}},
                     ConditionExpression="attribute_not_exists(lock_name)")
    ddb.delete_item(TableName="loader_locks", Key={"lock_name": item["lock_name"]})


def test_prefect_runs_a_flow_in_a_toolbox_container():
    """The local-docker worker starts a flow-run container that reaches Polaris."""
    from prefect.deployments import run_deployment

    from flows.deploy import deploy
    from flows.smoke import smoke

    deploy(smoke, "smoke")
    run = run_deployment("smoke/smoke", timeout=240)
    assert run.state.is_completed(), f"flow run ended {run.state.type}: {run.state.message}"


@pytest.mark.local
def test_spark_compacts_a_duckdb_table_through_polaris(s, ns, table):
    """Spark, as `maintenance` with vended credentials, rewrites files DuckDB wrote; data is unchanged."""
    from pyspark.sql import SparkSession

    from loader import duck

    name, t = table
    con = duck.connect(s, "phi")
    con.execute(f"CREATE TABLE {t} (id INT, v VARCHAR)")
    for i in range(6):  # six commits, six small files
        con.execute(f"INSERT INTO {t} SELECT x + {i * 100}, 'v' || x FROM range(100) r(x)")
    before = con.execute(f"SELECT count(*), sum(id) FROM {t}").fetchone()

    spark = SparkSession.builder.remote(s.spark_remote).getOrCreate()
    assert spark.sql(f"SELECT count(*), sum(id) FROM phi.{ns}.{name}").first() == tuple(before)
    result = spark.sql(f"CALL phi.system.rewrite_data_files(table => '{ns}.{name}', "
                       "options => map('min-input-files', '2'))").first()
    assert result["rewritten_data_files_count"] == 6 and result["added_data_files_count"] == 1

    after = duck.connect(s, "phi").execute(f"SELECT count(*), sum(id) FROM {t}").fetchone()
    assert after == before
