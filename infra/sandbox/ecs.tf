# ECS Fargate (ARM64): Polaris, the Prefect server and its ECS worker run as services, found by
# name in a private DNS namespace (the compose stack's service names, under lakehouse.internal).
# One-off tasks: database setup, the Polaris realm bootstrap, and the toolbox: `toolbox` for flow runs
# (started by the Prefect worker, no admin credentials), `toolbox_admin` for the bootstrap and tests.

resource "aws_ecs_cluster" "main" {
  name = var.name
}

resource "aws_cloudwatch_log_group" "main" {
  name              = "/${var.name}/sandbox"
  retention_in_days = 7
}

resource "aws_service_discovery_private_dns_namespace" "main" {
  name = local.dns_namespace
  vpc  = aws_vpc.main.id
}

resource "aws_service_discovery_service" "svc" {
  for_each = toset(["polaris", "prefect"])
  name     = each.key
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      type = "A"
      ttl  = 10
    }
    routing_policy = "MULTIVALUE"
  }
  force_destroy = true
}

locals {
  jdbc_url = "jdbc:postgresql://${aws_db_instance.pg.address}:5432/polaris"
  logs = { for name in ["polaris", "polaris-admin", "db-init", "prefect", "prefect-worker", "toolbox", "partner-api"] : name => {
    logDriver = "awslogs"
    options = { "awslogs-group" = aws_cloudwatch_log_group.main.name, "awslogs-region" = var.region,
    "awslogs-stream-prefix" = name }
  } }
  secret = { for k, s in aws_secretsmanager_secret.s : k => s.arn }
  polaris_db_env = [
    { name = "POLARIS_PERSISTENCE_TYPE", value = "relational-jdbc" },
    { name = "POLARIS_PERSISTENCE_RELATIONAL_JDBC_DATABASE_TYPE", value = "postgresql" },
    { name = "QUARKUS_DATASOURCE_JDBC_URL", value = local.jdbc_url },
    { name = "QUARKUS_DATASOURCE_USERNAME", value = "lakehouse" },
  ]
  # Everything the toolbox needs to run the loader on AWS (loader/settings.py).
  toolbox_env = [
    { name = "LOADER_PLATFORM", value = "aws" },
    { name = "AWS_REGION", value = var.region },
    { name = "LOADER_BUCKET_PREFIX", value = local.bucket_prefix },
    { name = "POLARIS_URI", value = "http://polaris.${local.dns_namespace}:8181/api/catalog" },
    { name = "POLARIS_MANAGEMENT_URI", value = "http://polaris.${local.dns_namespace}:8181/api/management/v1" },
    { name = "PREFECT_API_URL", value = "http://prefect.${local.dns_namespace}:4200/api" },
    { name = "DORIS_HOST", value = "doris.${local.dns_namespace}" },
    { name = "LOADER_VENDING_ROLE_PHI", value = aws_iam_role.vending["phi"].arn },
    { name = "LOADER_VENDING_ROLE_DEID", value = aws_iam_role.vending["deid"].arn },
    { name = "LOADER_KMS_PHI", value = aws_kms_key.key["phi"].arn },
    { name = "LOADER_KMS_DEID", value = aws_kms_key.key["deid"].arn },
    { name = "LOADER_MASTER_KMS_KEY", value = aws_kms_key.key["master"].arn },
    { name = "LOADER_MAINTENANCE_EXECUTOR", value = "emr" },
    { name = "LOADER_EMR_APPLICATION_ID", value = aws_emrserverless_application.spark.id },
    { name = "LOADER_EMR_JOB_ROLE", value = aws_iam_role.emr_job.arn },
    { name = "LOADER_ARTIFACTS_BUCKET", value = aws_s3_bucket.b["artifacts"].id },
    { name = "LOADER_TRAIL_BUCKET", value = aws_s3_bucket.b["trail"].id },
    { name = "LOADER_GUARDRAIL_PROBE_ROLE", value = aws_iam_role.guardrail_probe.arn },
    { name = "LOADER_VENDING_ROLES", value = join(",", [for r in aws_iam_role.vending : r.name]) },
    { name = "LOADER_PAUSE_SCHEDULES", value = "1" },
    # Half the task's 8 GB for DuckDB (it would take 80%), the rest for the Python side.
    { name = "LOADER_DUCKDB_MEMORY_LIMIT", value = "4GB" },
    # Integrity repair copies a lost lake file back from its previous version (loader/integrity.py).
    { name = "LOADER_LAKE_RESTORE_ROLE_ARN", value = aws_iam_role.lake_restore.arn },
    # Where flow runs go: toolbox tasks started by the Prefect ECS worker (flows/deploy.py).
    { name = "LOADER_ECS_CLUSTER", value = var.name },
    { name = "LOADER_TOOLBOX_IMAGE", value = "${aws_ecr_repository.r["toolbox"].repository_url}:${var.image_tag}" },
    { name = "LOADER_TOOLBOX_TASK_FAMILY", value = "${var.name}-toolbox" },
    { name = "LOADER_SUBNETS", value = join(",", aws_subnet.private[*].id) },
    { name = "LOADER_VPC_ID", value = aws_vpc.main.id },
    { name = "LOADER_SECURITY_GROUP", value = aws_security_group.internal.id },
    # Sources with their own AWS services (sources.tf).
    { name = "PARTNER_API_URL", value = "http://partner-api.${local.dns_namespace}:8080" },
    { name = "LOADER_FETCH_ALLOW_PRIVATE", value = "1" }, # the mock partner is in the VPC; production leaves it unset
    { name = "CLINIC_PG_HOST", value = aws_db_instance.pg.address },
    { name = "CLINIC_PG_USER", value = "lakehouse" },
    { name = "LOADER_FIREHOSE_ROLE", value = aws_iam_role.firehose.arn },
    { name = "LOADER_KMS_RAW", value = aws_kms_key.key["raw"].arn },
    { name = "PREFECT_LOGGING_EXTRA_LOGGERS", value = "loader" },
    { name = "PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", value = "ignore" },
  ]
  # Flow runs (the Prefect worker's ECS tasks) get no administrator credentials; the bootstrap and
  # the test runs use the admin task definition (least privilege).
  flow_secrets = [
    { name = "PREFECT_API_AUTH_STRING", valueFrom = local.secret["prefect-auth"] },
    { name = "LOADER_IDENTITY_PEPPER", valueFrom = local.secret["identity-pepper"] },
    { name = "DORIS_REFRESHER_PASSWORD", valueFrom = local.secret["doris-refresher"] },
  ]
  admin_secrets = concat(local.flow_secrets, [
    { name = "POLARIS_ROOT_SECRET", valueFrom = local.secret["polaris-root"] },
    { name = "DORIS_ROOT_PASSWORD", valueFrom = local.secret["doris-root"] },
    { name = "DORIS_ANALYST_PASSWORD", valueFrom = local.secret["doris-analyst"] },
    { name = "DORIS_ANALYST_MA_PASSWORD", valueFrom = local.secret["doris-analyst-ma"] },
    { name = "DORIS_CARE_TEAM_PASSWORD", valueFrom = local.secret["doris-care-team"] },
    # The CDC tests write to the clinic database (sandbox: the RDS master user).
    { name = "POSTGRES_PASSWORD", valueFrom = local.secret["postgres"] },
  ])
}

# ---------------------------------------------------------------- Polaris
resource "aws_ecs_task_definition" "polaris" {
  family                   = "${var.name}-polaris"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.polaris_task.arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([{
    name         = "polaris"
    image        = "apache/polaris:1.8.0"
    essential    = true
    portMappings = [{ containerPort = 8181 }, { containerPort = 8182 }]
    environment = concat(local.polaris_db_env, [
      { name = "POLARIS_REALM_CONTEXT_REALMS", value = "POLARIS" },
      { name = "QUARKUS_OTEL_SDK_DISABLED", value = "true" },
      { name = "AWS_REGION", value = var.region },
      # Name the principal and table in every vended STS session, and tag it: CloudTrail then
      # attributes every S3 access to both.
      { name = "polaris.features.\"SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL\"", value = "[\"principal\",\"table\"]" },
      { name = "polaris.features.\"SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL\"", value = "[\"principal\",\"catalog\",\"namespace\",\"table\"]" },
      # Few purge tasks at once, a larger pool: commits must not stall behind purges (loader/duck.py).
      { name = "POLARIS_TASKS_MAX_CONCURRENT_TASKS", value = "2" },
      { name = "QUARKUS_DATASOURCE_JDBC_MAX_SIZE", value = "40" },
    ])
    secrets          = [{ name = "QUARKUS_DATASOURCE_PASSWORD", valueFrom = local.secret["postgres"] }]
    healthCheck      = { command = ["CMD-SHELL", "curl --fail --silent http://localhost:8182/q/health || exit 1"], interval = 10, retries = 10, startPeriod = 60, timeout = 5 }
    logConfiguration = local.logs["polaris"]
  }])
}

resource "aws_ecs_service" "polaris" {
  name            = "polaris"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.polaris.arn
  desired_count   = 0 # scaled to 1 by `make sandbox-apply` after the database is initialized
  launch_type     = "FARGATE"
  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.internal.id]
  }
  service_registries { registry_arn = aws_service_discovery_service.svc["polaris"].arn }
  lifecycle { ignore_changes = [desired_count] }
}

resource "aws_ecs_task_definition" "polaris_admin" {
  family                   = "${var.name}-polaris-admin"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.execution.arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([{
    name        = "polaris-admin"
    image       = "apache/polaris-admin-tool:1.8.0"
    essential   = true
    entryPoint  = ["sh", "-c"]
    command     = ["java -jar /deployments/polaris-admin-tool.jar bootstrap --realm=POLARIS --credential=POLARIS,root,$POLARIS_ROOT_SECRET || echo 'realm already bootstrapped'"]
    environment = local.polaris_db_env
    secrets = [
      { name = "QUARKUS_DATASOURCE_PASSWORD", valueFrom = local.secret["postgres"] },
      { name = "POLARIS_ROOT_SECRET", valueFrom = local.secret["polaris-root"] },
    ]
    logConfiguration = local.logs["polaris-admin"]
  }])
}

# Polaris does not create its schema; Prefect needs its own database; the CDC source needs the
# clinic database and its table (the local stack's stack/source_pg/init.sql).
resource "aws_ecs_task_definition" "db_init" {
  family                   = "${var.name}-db-init"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.execution.arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([{
    name       = "db-init"
    image      = "postgres:17"
    essential  = true
    entryPoint = ["sh", "-c"]
    command = [join(" ", [
      "psql \"host=${aws_db_instance.pg.address} user=lakehouse dbname=polaris sslmode=require\" -c 'CREATE SCHEMA IF NOT EXISTS polaris_schema' &&",
      "(psql \"host=${aws_db_instance.pg.address} user=lakehouse dbname=polaris sslmode=require\" -c 'CREATE DATABASE prefect' || echo 'prefect database exists') &&",
      "(psql \"host=${aws_db_instance.pg.address} user=lakehouse dbname=polaris sslmode=require\" -c 'CREATE DATABASE clinic' || echo 'clinic database exists') &&",
      "(psql \"host=${aws_db_instance.pg.address} user=lakehouse dbname=clinic sslmode=require\" -c \"$CLINIC_DDL\" || echo 'clinic table exists')",
    ])]
    environment      = [{ name = "CLINIC_DDL", value = file("${path.module}/../../stack/source_pg/init.sql") }]
    secrets          = [{ name = "PGPASSWORD", valueFrom = local.secret["postgres"] }]
    logConfiguration = local.logs["db-init"]
  }])
}

# ---------------------------------------------------------------- Prefect
resource "aws_ecs_task_definition" "prefect" {
  family                   = "${var.name}-prefect"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.prefect_server_task.arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([{
    name         = "prefect"
    image        = "prefecthq/prefect:3.8.8-python3.12"
    essential    = true
    command      = ["prefect", "server", "start", "--host", "0.0.0.0"]
    portMappings = [{ containerPort = 4200 }]
    environment  = [{ name = "PREFECT_SERVER_ANALYTICS_ENABLED", value = "false" }]
    secrets = [
      { name = "PREFECT_API_DATABASE_CONNECTION_URL", valueFrom = local.secret["prefect-db-url"] },
      { name = "PREFECT_SERVER_API_AUTH_STRING", valueFrom = local.secret["prefect-auth"] },
    ]
    healthCheck      = { command = ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://127.0.0.1:4200/api/health')\""], interval = 10, retries = 10, startPeriod = 60, timeout = 5 }
    logConfiguration = local.logs["prefect"]
  }])
}

resource "aws_ecs_service" "prefect" {
  name            = "prefect"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.prefect.arn
  desired_count   = 0 # scaled to 1 after the database is initialized
  launch_type     = "FARGATE"
  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.internal.id]
  }
  service_registries { registry_arn = aws_service_discovery_service.svc["prefect"].arn }
  lifecycle { ignore_changes = [desired_count] }
}

resource "aws_ecs_task_definition" "prefect_worker" {
  family                   = "${var.name}-prefect-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.prefect_worker_task.arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([{
    name             = "prefect-worker"
    image            = "${aws_ecr_repository.r["prefect-worker"].repository_url}:${var.image_tag}"
    essential        = true
    command          = ["prefect", "worker", "start", "--pool", "aws-ecs", "--type", "ecs"]
    environment      = [{ name = "PREFECT_API_URL", value = "http://prefect.${local.dns_namespace}:4200/api" }, { name = "AWS_REGION", value = var.region }]
    secrets          = [{ name = "PREFECT_API_AUTH_STRING", valueFrom = local.secret["prefect-auth"] }]
    logConfiguration = local.logs["prefect-worker"]
  }])
}

resource "aws_ecs_service" "prefect_worker" {
  name            = "prefect-worker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.prefect_worker.arn
  desired_count   = 0 # scaled to 1 once Prefect is up
  launch_type     = "FARGATE"
  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.internal.id]
  }
  lifecycle { ignore_changes = [desired_count] }
}

# ---------------------------------------------------------------- toolbox: flow runs (no admin credentials)
resource "aws_ecs_task_definition" "toolbox" {
  family                   = "${var.name}-toolbox"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 2048
  memory                   = 8192
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.toolbox_task.arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([{
    name             = "toolbox"
    image            = "${aws_ecr_repository.r["toolbox"].repository_url}:${var.image_tag}"
    essential        = true
    command          = ["python", "-c", "print('flow runs override this command')"]
    environment      = local.toolbox_env
    secrets          = local.flow_secrets
    logConfiguration = local.logs["toolbox"]
  }])
}

# ---------------------------------------------------------------- toolbox admin: bootstrap, Doris setup, tests
resource "aws_ecs_task_definition" "toolbox_admin" {
  family                   = "${var.name}-toolbox-admin"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 2048
  memory                   = 8192
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.toolbox_admin_task.arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([{
    name             = "toolbox"
    image            = "${aws_ecr_repository.r["toolbox"].repository_url}:${var.image_tag}"
    essential        = true
    command          = ["python", "-m", "stack.bootstrap"]
    environment      = local.toolbox_env
    secrets          = local.admin_secrets
    logConfiguration = local.logs["toolbox"]
  }])
}
