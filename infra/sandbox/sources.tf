# Sources that need AWS services of their own. Each one only gets data into raw/drops/<source>/; the loader takes it from there
# exactly as it does locally.
#
# - Partner API: the mock partner as an ECS service (a real partner is just a URL), its token in
#   Secrets Manager lakehouse/sources/acme_partner_api (data.tf).
# - CDC: AWS DMS from a `clinic` database on the stack's RDS (logical replication on) to Parquet
#   change files in its own bucket, cdc-staging; the loader's `staged` fetch moves them into
#   raw/drops/. DMS has no access to the raw zone: its bucket check (HeadBucket) needs list rights
#   on the whole bucket, which would show it every raw key name. `scripts/sandbox.py` starts the
#   task once the table exists and stops it before teardown.
# - Streaming: Firehose Direct PUT streams, one per stream source, created by the bootstrap and the
#   tests (stack/stream_sink/streams.py). Terraform owns only the role they deliver as.
# - Email: no SES receipt rule (it needs a domain whose MX points at SES); messages are stored where
#   SES's S3 action would put them, raw/inbox/<mailbox>/.

# ---------------------------------------------------------------- partner API (mock)
resource "aws_service_discovery_service" "partner_api" {
  name = "partner-api"
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

resource "aws_ecs_task_definition" "partner_api" {
  family                   = "${var.name}-partner-api"
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
    name             = "partner-api"
    image            = "${aws_ecr_repository.r["toolbox"].repository_url}:${var.image_tag}"
    essential        = true
    command          = ["python", "-m", "stack.mock_partner"]
    portMappings     = [{ containerPort = 8080 }]
    secrets          = [{ name = "LOADER_SOURCE_SECRET_ACME_PARTNER_API", valueFrom = local.secret["sources/acme_partner_api"] }]
    logConfiguration = local.logs["partner-api"]
  }])
}

resource "aws_ecs_service" "partner_api" {
  name            = "partner-api"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.partner_api.arn
  desired_count   = 0 # scaled to 1 by `make sandbox-apply` once the image is pushed
  launch_type     = "FARGATE"
  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.internal.id]
  }
  service_registries { registry_arn = aws_service_discovery_service.partner_api.arn }
  lifecycle { ignore_changes = [desired_count] }
}

# ---------------------------------------------------------------- CDC: DMS
# DMS needs this role, under exactly this name, to place instances in the VPC.
resource "aws_iam_role" "dms_vpc" {
  name = "dms-vpc-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "dms.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "dms_vpc" {
  role       = aws_iam_role.dms_vpc.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
}

# Task logs in CloudWatch (DMS also wants this exact name).
resource "aws_iam_role" "dms_logs" {
  name               = "dms-cloudwatch-logs-role"
  assume_role_policy = aws_iam_role.dms_vpc.assume_role_policy
}

resource "aws_iam_role_policy_attachment" "dms_logs" {
  role       = aws_iam_role.dms_logs.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSCloudWatchLogsRole"
}

# New IAM roles take a few seconds to be usable by DMS.
resource "time_sleep" "dms_roles" {
  depends_on      = [aws_iam_role_policy_attachment.dms_vpc, aws_iam_role_policy_attachment.dms_logs]
  create_duration = "20s"
}

resource "aws_dms_replication_subnet_group" "main" {
  replication_subnet_group_id          = "${var.name}-dms"
  replication_subnet_group_description = "Lakehouse sandbox private subnets"
  subnet_ids                           = aws_subnet.private[*].id
  depends_on                           = [time_sleep.dms_roles]
}

resource "aws_dms_replication_instance" "main" {
  replication_instance_id     = "${var.name}-dms"
  replication_instance_class  = "dms.t3.small" # the smallest class DMS still offers
  allocated_storage           = 20
  engine_version              = "3.6.1"
  multi_az                    = false
  publicly_accessible         = false
  kms_key_arn                 = aws_kms_key.key["master"].arn
  replication_subnet_group_id = aws_dms_replication_subnet_group.main.id
  vpc_security_group_ids      = [aws_security_group.internal.id]
  apply_immediately           = true
}

resource "aws_dms_endpoint" "clinic" {
  endpoint_id   = "${var.name}-clinic"
  endpoint_type = "source"
  engine_name   = "postgres"
  server_name   = aws_db_instance.pg.address
  port          = 5432
  database_name = "clinic"
  username      = "lakehouse"
  password      = random_password.p["postgres"].result
  ssl_mode      = "require"
}

# DMS writes as this role: its own staging bucket only.
resource "aws_iam_role" "dms_s3" {
  name               = "${var.name}-dms-s3"
  assume_role_policy = aws_iam_role.dms_vpc.assume_role_policy
}

resource "aws_iam_role_policy" "dms_s3" {
  role = aws_iam_role.dms_s3.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:PutObject", "s3:DeleteObject", "s3:PutObjectTagging"],
      Resource = "${aws_s3_bucket.b["cdc-staging"].arn}/*" },
      { Effect = "Allow", Action = "s3:ListBucket", Resource = aws_s3_bucket.b["cdc-staging"].arn },
      { Effect = "Allow", Action = ["kms:GenerateDataKey", "kms:Decrypt"], Resource = aws_kms_key.key["raw"].arn },
    ]
  })
}

resource "aws_dms_s3_endpoint" "drops" {
  endpoint_id                       = "${var.name}-clinic-drops"
  endpoint_type                     = "target"
  bucket_name                       = aws_s3_bucket.b["cdc-staging"].id
  bucket_folder                     = "acme_clinic_patients" # the contract's fetch prefix
  service_access_role_arn           = aws_iam_role.dms_s3.arn
  data_format                       = "parquet"
  parquet_version                   = "parquet-2-0"
  include_op_for_full_load          = true
  cdc_max_batch_interval            = 10 # seconds: a change file at least this often
  encryption_mode                   = "SSE_KMS"
  server_side_encryption_kms_key_id = aws_kms_key.key["raw"].arn
  depends_on                        = [aws_iam_role_policy.dms_s3]
}

resource "aws_dms_replication_task" "clinic" {
  replication_task_id      = "${var.name}-clinic-patients"
  migration_type           = "full-load-and-cdc"
  replication_instance_arn = aws_dms_replication_instance.main.replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.clinic.endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.drops.endpoint_arn
  start_replication_task   = false # started by scripts/sandbox.py once db-init has created the table
  # The patients table, plus the change sequence that orders changes (the contract's cdc_seq).
  table_mappings = jsonencode({ rules = [
    { "rule-type" = "selection", "rule-id" = "1", "rule-name" = "patients", "rule-action" = "include",
    "object-locator" = { "schema-name" = "public", "table-name" = "patients" } },
    { "rule-type"   = "transformation", "rule-id" = "2", "rule-name" = "change-seq", "rule-target" = "column",
      "rule-action" = "add-column", "value" = "change_seq", "expression" = "$AR_H_CHANGE_SEQ",
      "data-type"   = { type = "string", length = 50 },
    "object-locator" = { "schema-name" = "public", "table-name" = "patients" } },
  ] })
  replication_task_settings = jsonencode({
    Logging = { EnableLogging = true }
  })
  lifecycle { ignore_changes = [replication_task_settings] } # DMS fills in defaults
}

# ---------------------------------------------------------------- streaming: the Firehose delivery role
resource "aws_iam_role" "firehose" {
  name = "${var.name}-firehose"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "firehose.amazonaws.com" },
    Condition = { StringEquals = { "sts:ExternalId" = var.account_id } } }]
  })
}

# Put into drop prefixes only (Firehose's documented minimum, limited to drops/): it cannot read what
# the loader landed, delete anything, or write anywhere else.
resource "aws_iam_role_policy" "firehose" {
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload"],
      Resource = "${aws_s3_bucket.b["raw"].arn}/drops/*" },
      { Effect = "Allow", Action = ["s3:GetBucketLocation", "s3:ListBucketMultipartUploads"], Resource = aws_s3_bucket.b["raw"].arn },
      { Effect = "Allow", Action = ["kms:GenerateDataKey", "kms:Decrypt"], Resource = aws_kms_key.key["raw"].arn },
    ]
  })
}
