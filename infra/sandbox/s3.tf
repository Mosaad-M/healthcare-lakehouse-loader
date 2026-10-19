resource "aws_s3_bucket" "b" {
  for_each            = local.buckets
  bucket              = each.value
  force_destroy       = true # sandbox: synthetic data only
  object_lock_enabled = each.key == "shred-journal"
}

resource "aws_s3_bucket_public_access_block" "b" {
  for_each                = aws_s3_bucket.b
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

locals {
  bucket_key = { raw = "raw", "lake-phi" = "phi", "lake-deid" = "deid", "shred-journal" = "journal", "cdc-staging" = "raw" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "b" {
  for_each = local.bucket_key
  bucket   = aws_s3_bucket.b[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.key[each.value].arn
    }
    bucket_key_enabled = true
  }
}

# The two buckets without PHI (audit trail, build artifacts) use S3-managed keys, set explicitly.
#trivy:ignore:AVD-AWS-0132
resource "aws_s3_bucket_server_side_encryption_configuration" "s3_managed" {
  for_each = toset(["trail", "artifacts"])
  bucket   = aws_s3_bucket.b[each.key].id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_versioning" "journal" {
  bucket = aws_s3_bucket.b["shred-journal"].id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_object_lock_configuration" "journal" {
  bucket = aws_s3_bucket.b["shred-journal"].id
  rule {
    default_retention {
      mode = "GOVERNANCE"
      days = var.journal_retention_days
    }
  }
  depends_on = [aws_s3_bucket_versioning.journal]
}

# The raw zone holds plaintext and cannot be shredded per subject, so it expires.
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.b["raw"].id
  dynamic "rule" {
    for_each = ["drops/", "landed/", "quarantine/", "inbox/"]
    content {
      id     = "expire-${trimsuffix(rule.value, "/")}"
      status = "Enabled"
      filter { prefix = rule.value }
      expiration { days = local.model.raw_expiry_days }
    }
  }
}

# DMS's change files wait here until the loader moves them into raw/drops/ (plaintext: same expiry).
resource "aws_s3_bucket_lifecycle_configuration" "cdc_staging" {
  bucket = aws_s3_bucket.b["cdc-staging"].id
  rule {
    id     = "expire-all"
    status = "Enabled"
    filter {}
    expiration { days = local.model.raw_expiry_days }
  }
}

# The lake keeps a deleted object's previous version for a few days: DuckDB 1.5.6 can delete the files
# of a commit Polaris applied (README, DuckDB), and the integrity repair copies them back
# (loader/integrity.py). Expired delete markers go too. Crypto-shredding does not depend on deleting
# lake files, so keeping earlier versions does not delay an erasure.
resource "aws_s3_bucket_versioning" "lake" {
  for_each = toset(["lake-phi", "lake-deid"])
  bucket   = aws_s3_bucket.b[each.key].id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  for_each = aws_s3_bucket_versioning.lake
  bucket   = each.value.bucket
  rule {
    id     = "expire-earlier-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration { noncurrent_days = local.model.lake_version_days }
    expiration { expired_object_delete_marker = true }
  }
}

# The lake buckets admit only their Polaris vending role and the operator (break-glass), and
# lake-restore for the one thing it does: copy an object back from an earlier version of itself.
# Every engine reaches them through credentials Polaris vends; nothing else gets in, whatever its
# IAM policies say.
data "aws_iam_policy_document" "lake" {
  for_each = { "lake-phi" = "phi", "lake-deid" = "deid" }
  statement {
    sid       = "OnlyThroughPolaris"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.b[each.key].arn, "${aws_s3_bucket.b[each.key].arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "ArnNotEquals"
      variable = "aws:PrincipalArn"
      values   = [aws_iam_role.vending[each.value].arn, local.operator_role_arn, aws_iam_role.lake_restore.arn]
    }
  }
  statement {
    sid         = "RestoreOnlyListsReadsVersionsAndCopies"
    effect      = "Deny"
    not_actions = ["s3:ListBucketVersions", "s3:ListBucket", "s3:GetBucketLocation", "s3:GetObjectVersion", "s3:PutObject"]
    resources   = [aws_s3_bucket.b[each.key].arn, "${aws_s3_bucket.b[each.key].arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:PrincipalArn"
      values   = [aws_iam_role.lake_restore.arn]
    }
  }
  statement {
    sid       = "RestoreWritesOnlyCopiesFromThisBucket"
    effect    = "Deny"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.b[each.key].arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:PrincipalArn"
      values   = [aws_iam_role.lake_restore.arn]
    }
    condition { # also denies a plain upload: the header is then absent
      test     = "StringNotLike"
      variable = "s3:x-amz-copy-source"
      values   = ["${aws_s3_bucket.b[each.key].id}/*", "/${aws_s3_bucket.b[each.key].id}/*"]
    }
  }
  statement {
    sid       = "TLSOnly"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.b[each.key].arn, "${aws_s3_bucket.b[each.key].arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  for_each = data.aws_iam_policy_document.lake
  bucket   = aws_s3_bucket.b[each.key].id
  policy   = each.value.json
}

# The journal: only the operator may bypass governance retention (sandbox teardown).
data "aws_iam_policy_document" "journal" {
  statement {
    sid       = "NoBypassButOperator"
    effect    = "Deny"
    actions   = ["s3:BypassGovernanceRetention", "s3:PutObjectRetention", "s3:PutBucketObjectLockConfiguration"]
    resources = [aws_s3_bucket.b["shred-journal"].arn, "${aws_s3_bucket.b["shred-journal"].arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "ArnNotEquals"
      variable = "aws:PrincipalArn"
      values   = [local.operator_role_arn]
    }
  }
}

resource "aws_s3_bucket_policy" "journal" {
  bucket = aws_s3_bucket.b["shred-journal"].id
  policy = data.aws_iam_policy_document.journal.json
}
