# Who may do what. The lake buckets are reachable only through the Polaris vending roles (and
# their bucket policies say so too); every other identity works through Polaris.

data "aws_iam_policy_document" "ecs_tasks" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# ---------------------------------------------------------------- vending roles (one per catalog)
data "aws_iam_policy_document" "vending_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.polaris_task.arn]
    }
  }
}

locals {
  catalogs = { phi = "lake-phi", deid = "lake-deid" }
}

resource "aws_iam_role" "vending" {
  for_each           = local.catalogs
  name               = "${var.name}-vending-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.vending_trust.json
}

data "aws_iam_policy_document" "vending" {
  for_each = local.catalogs
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:GetObjectVersion", "s3:DeleteObjectVersion"]
    resources = ["${aws_s3_bucket.b[each.value].arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.b[each.value].arn]
  }
  statement {
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [aws_kms_key.key[each.key].arn]
  }
}

resource "aws_iam_role_policy" "vending" {
  for_each = local.catalogs
  role     = aws_iam_role.vending[each.key].id
  policy   = data.aws_iam_policy_document.vending[each.key].json
}

# ---------------------------------------------------------------- ECS execution (pull images, read secrets)
resource "aws_iam_role" "execution" {
  name               = "${var.name}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secrets" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [for s in aws_secretsmanager_secret.s : s.arn]
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secrets.json
}

# ---------------------------------------------------------------- Polaris: may only assume the vending roles
resource "aws_iam_role" "polaris_task" {
  name               = "${var.name}-polaris-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks.json
}

resource "aws_iam_role_policy" "polaris_task" {
  role = aws_iam_role.polaris_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sts:AssumeRole", "sts:TagSession"]
      Resource = [for r in aws_iam_role.vending : r.arn]
    }]
  })
}

# ---------------------------------------------------------------- Prefect server (nothing) and worker (starts flow runs)
resource "aws_iam_role" "prefect_server_task" {
  name               = "${var.name}-prefect-server-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks.json
}

resource "aws_iam_role" "prefect_worker_task" {
  name               = "${var.name}-prefect-worker-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks.json
}

data "aws_iam_policy_document" "prefect_worker" {
  statement {
    actions = ["ecs:RunTask", "ecs:StopTask", "ecs:DescribeTasks", "ecs:RegisterTaskDefinition",
      "ecs:DeregisterTaskDefinition", "ecs:DescribeTaskDefinition", "ecs:ListTaskDefinitions", "ecs:TagResource",
    "ecs:DescribeClusters", "ec2:DescribeSubnets", "ec2:DescribeVpcs", "ec2:DescribeSecurityGroups"]
    resources = ["*"]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:GetLogEvents", "logs:DescribeLogStreams"]
    resources = ["*"]
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.toolbox_task.arn, aws_iam_role.execution.arn]
  }
}

resource "aws_iam_role_policy" "prefect_worker" {
  role   = aws_iam_role.prefect_worker_task.id
  policy = data.aws_iam_policy_document.prefect_worker.json
}

# ---------------------------------------------------------------- toolbox: flow runs and admin tasks
# Shared by both: raw zone, shred journal (write and read, never delete), control tables, master key,
# EMR job submission, assuming lake-restore. No lake access of its own: the bucket policies would
# refuse it anyway.
# Flow runs (`toolbox_task`) read only the secrets a flow needs; the bootstrap and tests
# (`toolbox_admin_task`) also read the administrator secrets and manage principals and streams.
resource "aws_iam_role" "toolbox_task" {
  name               = "${var.name}-toolbox-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks.json
}

resource "aws_iam_role" "toolbox_admin_task" {
  name               = "${var.name}-toolbox-admin-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks.json
}

data "aws_iam_policy_document" "toolbox" {
  statement {
    sid     = "RawZone" # and DMS's staging bucket, whose files the loader moves into raw/drops/
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.b["raw"].arn, "${aws_s3_bucket.b["raw"].arn}/*",
    aws_s3_bucket.b["cdc-staging"].arn, "${aws_s3_bucket.b["cdc-staging"].arn}/*"]
  }
  statement {
    sid       = "ShredJournalWriteOnce"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:ListBucketVersions", "s3:GetObjectVersion"]
    resources = [aws_s3_bucket.b["shred-journal"].arn, "${aws_s3_bucket.b["shred-journal"].arn}/*"]
  }
  statement {
    sid       = "Artifacts"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.b["artifacts"].arn, "${aws_s3_bucket.b["artifacts"].arn}/*"]
  }
  statement {
    sid       = "AuditTrailRead"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.b["trail"].arn, "${aws_s3_bucket.b["trail"].arn}/*"]
  }
  statement {
    sid       = "Keys"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [aws_kms_key.key["raw"].arn, aws_kms_key.key["journal"].arn, aws_kms_key.key["master"].arn]
  }
  statement {
    sid = "ControlTables"
    actions = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem",
    "dynamodb:Scan", "dynamodb:Query", "dynamodb:BatchGetItem", "dynamodb:BatchWriteItem", "dynamodb:DescribeTable"]
    resources = [for name, t in aws_dynamodb_table.t : t.arn if name != "decrypt_audit"]
  }
  statement {
    # The decrypt audit table is written and read, never edited or deleted. The write-once copy
    # of every event is in the journal bucket (loader/journal.py).
    sid       = "DecryptAuditAppend"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Scan", "dynamodb:Query", "dynamodb:DescribeTable"]
    resources = [aws_dynamodb_table.t["decrypt_audit"].arn]
  }
  statement {
    actions   = ["dynamodb:ListTables"]
    resources = ["*"]
  }
  statement {
    sid = "Maintenance"
    actions = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun", "emr-serverless:CancelJobRun",
    "emr-serverless:GetApplication", "emr-serverless:StartApplication"]
    resources = [aws_emrserverless_application.spark.arn, "${aws_emrserverless_application.spark.arn}/jobruns/*"]
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_job.arn]
  }
  statement {
    sid       = "LakeRestore"
    actions   = ["sts:AssumeRole"]
    resources = [aws_iam_role.lake_restore.arn]
  }
}

resource "aws_iam_role_policy" "toolbox" {
  role   = aws_iam_role.toolbox_task.id
  policy = data.aws_iam_policy_document.toolbox.json
}

data "aws_iam_policy_document" "toolbox_flow" {
  statement {
    sid     = "FlowSecrets" # principal credentials, source tokens, the pepper and the Doris refresher
    actions = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.s["identity-pepper"].arn, aws_secretsmanager_secret.s["doris-refresher"].arn,
      aws_secretsmanager_secret.s["prefect-auth"].arn,
    "arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:lakehouse/principals/*", "arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:lakehouse/sources/*"]
  }
}

data "aws_iam_policy_document" "toolbox_admin" {
  statement {
    sid       = "AllSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = concat([for s in aws_secretsmanager_secret.s : s.arn], ["arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:lakehouse/sources/*"])
  }
  statement {
    sid = "PrincipalSecrets"
    actions = ["secretsmanager:GetSecretValue", "secretsmanager:PutSecretValue", "secretsmanager:CreateSecret",
    "secretsmanager:DeleteSecret", "secretsmanager:TagResource"]
    resources = ["arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:lakehouse/principals/*"]
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.firehose.arn]
  }
  statement {
    sid = "StreamEntryPoints"
    actions = ["firehose:CreateDeliveryStream", "firehose:DeleteDeliveryStream", "firehose:DescribeDeliveryStream",
    "firehose:PutRecord", "firehose:PutRecordBatch", "firehose:TagDeliveryStream"]
    resources = ["arn:aws:firehose:${var.region}:${var.account_id}:deliverystream/lakehouse-*"]
  }
  statement {
    actions   = ["firehose:ListDeliveryStreams"]
    resources = ["*"]
  }
  statement {
    sid       = "CheckTheSinkRole" # the streaming test asks IAM what Firehose's role may do
    actions   = ["iam:SimulatePrincipalPolicy"]
    resources = [aws_iam_role.firehose.arn]
  }
}

resource "aws_iam_role_policy" "toolbox_flow" {
  role   = aws_iam_role.toolbox_task.id
  policy = data.aws_iam_policy_document.toolbox_flow.json
}

resource "aws_iam_role_policy" "toolbox_admin_shared" {
  role   = aws_iam_role.toolbox_admin_task.id
  policy = data.aws_iam_policy_document.toolbox.json
}

resource "aws_iam_role_policy" "toolbox_admin" {
  role   = aws_iam_role.toolbox_admin_task.id
  policy = data.aws_iam_policy_document.toolbox_admin.json
}

# ---------------------------------------------------------------- EMR Serverless maintenance jobs
data "aws_iam_policy_document" "emr_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_job" {
  name               = "${var.name}-emr-job"
  assume_role_policy = data.aws_iam_policy_document.emr_trust.json
}

data "aws_iam_policy_document" "emr_job" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.b["artifacts"].arn, "${aws_s3_bucket.b["artifacts"].arn}/*"]
  }
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:lakehouse/principals/maintenance-*"]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "emr_job" {
  role   = aws_iam_role.emr_job.id
  policy = data.aws_iam_policy_document.emr_job.json
}

# ---------------------------------------------------------------- Doris host: SSM access only
data "aws_iam_policy_document" "ec2_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "doris" {
  name               = "${var.name}-doris"
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

resource "aws_iam_role_policy_attachment" "doris_ssm" {
  role       = aws_iam_role.doris.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "doris" {
  name = "${var.name}-doris"
  role = aws_iam_role.doris.name
}

# ---------------------------------------------------------------- lake-restore (integrity repair)
# Copies a lost lake file back from its previous version (loader/integrity.py). It may list and read
# earlier versions and write only by copying within the same bucket; the lake bucket policies hold it
# to that as well (s3.tf). Assumed by the toolbox: writers heal their own table, maintenance too.
resource "aws_iam_role" "lake_restore" {
  name = "${var.name}-lake-restore"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole",
    Principal = { AWS = [aws_iam_role.toolbox_task.arn, aws_iam_role.toolbox_admin_task.arn] } }]
  })
}

data "aws_iam_policy_document" "lake_restore" {
  statement {
    actions   = ["s3:ListBucketVersions", "s3:ListBucket"]
    resources = [for b in ["lake-phi", "lake-deid"] : aws_s3_bucket.b[b].arn]
  }
  statement {
    actions   = ["s3:GetObjectVersion"]
    resources = [for b in ["lake-phi", "lake-deid"] : "${aws_s3_bucket.b[b].arn}/*"]
  }
  dynamic "statement" {
    for_each = toset(["lake-phi", "lake-deid"])
    content {
      actions   = ["s3:PutObject"]
      resources = ["${aws_s3_bucket.b[statement.value].arn}/*"]
      condition {
        test     = "StringLike"
        variable = "s3:x-amz-copy-source"
        values   = ["${aws_s3_bucket.b[statement.value].id}/*", "/${aws_s3_bucket.b[statement.value].id}/*"]
      }
    }
  }
  statement { # the copy decrypts the old version and encrypts the new one under the bucket's key
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [aws_kms_key.key["phi"].arn, aws_kms_key.key["deid"].arn]
  }
}

resource "aws_iam_role_policy" "lake_restore" {
  role   = aws_iam_role.lake_restore.id
  policy = data.aws_iam_policy_document.lake_restore.json
}

# ---------------------------------------------------------------- guardrail probe (tests only)
# Full S3 rights on the lake through IAM, assumable by the toolbox: the AWS-only test proves the
# bucket policy still refuses it, so the guardrail does not depend on IAM hygiene.
resource "aws_iam_role" "guardrail_probe" {
  name = "${var.name}-guardrail-probe"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { AWS = aws_iam_role.toolbox_admin_task.arn } }]
  })
}

#trivy:ignore:AVD-AWS-0345 full S3 rights on purpose: the test proves the bucket policy refuses them
resource "aws_iam_role_policy" "guardrail_probe" {
  role = aws_iam_role.guardrail_probe.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = "s3:*", Resource = [for b in ["lake-phi", "lake-deid"] : aws_s3_bucket.b[b].arn],
      },
      { Effect = "Allow", Action = "s3:*", Resource = [for b in ["lake-phi", "lake-deid"] : "${aws_s3_bucket.b[b].arn}/*"] },
      { Effect = "Allow", Action = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"], Resource = [aws_kms_key.key["phi"].arn, aws_kms_key.key["deid"].arn] },
    ]
  })
}

resource "aws_iam_role_policy" "toolbox_probe" {
  role = aws_iam_role.toolbox_admin_task.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Resource = aws_iam_role.guardrail_probe.arn }]
  })
}
