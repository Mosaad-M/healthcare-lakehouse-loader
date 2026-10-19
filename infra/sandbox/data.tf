# Control-plane tables: the same list the bootstrap uses (stack/bootstrap/model.json).
resource "aws_dynamodb_table" "t" {
  for_each     = local.model.dynamodb_tables
  name         = each.key
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = each.value
  attribute {
    name = each.value
    type = "S"
  }
  # Entries that are only useful for a while expire (model.json dynamodb_ttl).
  dynamic "ttl" {
    for_each = contains(keys(local.model.dynamodb_ttl), each.key) ? [local.model.dynamodb_ttl[each.key]] : []
    content {
      attribute_name = ttl.value
      enabled        = true
    }
  }
  # The key vault and its KEKs keep a recovery window (35 days of point-in-time recovery):
  # losing a KEK would lose every subject key it wraps.
  point_in_time_recovery {
    enabled = contains(["key_vault", "key_encryption_keys"], each.key)
  }
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.key["master"].arn
  }
}

# Secrets. Sandbox: deleted immediately on teardown (no recovery window).
resource "random_password" "p" {
  for_each = toset(["polaris_root", "postgres", "doris_root", "doris_analyst", "doris_analyst_ma", "doris_care_team", "doris_refresher", "partner_api", "prefect_auth"])
  length   = 32
  special  = false
}

resource "random_bytes" "pepper" {
  length = 32
}

locals {
  secrets = {
    "polaris-root"     = random_password.p["polaris_root"].result
    "postgres"         = random_password.p["postgres"].result
    "identity-pepper"  = random_bytes.pepper.base64
    "doris-root"       = random_password.p["doris_root"].result
    "doris-analyst"    = random_password.p["doris_analyst"].result
    "doris-analyst-ma" = random_password.p["doris_analyst_ma"].result
    "doris-care-team"  = random_password.p["doris_care_team"].result
    "doris-refresher"  = random_password.p["doris_refresher"].result
    # Prefect API basic auth (user:password): the server requires it, every client sends it.
    "prefect-auth" = "lakehouse:${random_password.p["prefect_auth"].result}"
    # The mock partner's API token: a source secret, read by the loader at fetch time.
    "sources/acme_partner_api" = random_password.p["partner_api"].result
    # Prefect's database URL, so its task needs one secret, not two values.
    "prefect-db-url" = "postgresql+asyncpg://lakehouse:${random_password.p["postgres"].result}@${aws_db_instance.pg.address}:5432/prefect"
  }
}

resource "aws_secretsmanager_secret" "s" {
  for_each                = local.secrets
  name                    = "lakehouse/${each.key}"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "s" {
  for_each      = local.secrets
  secret_id     = aws_secretsmanager_secret.s[each.key].id
  secret_string = each.value
}

# Principal credentials are written by the bootstrap, one secret each (lakehouse/principals/<name>);
# scripts/sandbox.py deletes them before `terraform destroy`.

# Our two images; everything else comes from public registries through the NAT gateway.
resource "aws_ecr_repository" "r" {
  for_each     = toset(["toolbox", "prefect-worker"])
  name         = "${var.name}/${each.key}"
  force_delete = true
  # The sandbox re-pushes one tag (var.image_tag); production pushes immutable git-SHA tags.
  image_tag_mutability = "MUTABLE" #trivy:ignore:AVD-AWS-0031
  image_scanning_configuration { scan_on_push = true }
}

resource "aws_db_subnet_group" "pg" {
  name       = "${var.name}-pg"
  subnet_ids = aws_subnet.private[*].id
}

# Logical replication on, for DMS to capture the `clinic` database.
resource "aws_db_parameter_group" "pg" {
  name   = "${var.name}-pg17"
  family = "postgres17"
  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }
}

# One small PostgreSQL for Polaris (database polaris, schema polaris_schema), Prefect (database
# prefect) and the CDC source (database clinic). Production: Multi-AZ, larger, separate
# instances, and the clinic's own database.
resource "aws_db_instance" "pg" {
  identifier             = "${var.name}-pg"
  engine                 = "postgres"
  engine_version         = "17"
  instance_class         = "db.t4g.micro"
  allocated_storage      = 20
  storage_encrypted      = true
  kms_key_id             = aws_kms_key.key["master"].arn
  db_name                = "polaris"
  username               = "lakehouse"
  password               = random_password.p["postgres"].result
  db_subnet_group_name   = aws_db_subnet_group.pg.name
  vpc_security_group_ids = [aws_security_group.internal.id]
  parameter_group_name   = aws_db_parameter_group.pg.name
  skip_final_snapshot    = true
  deletion_protection    = false
  apply_immediately      = true
}
