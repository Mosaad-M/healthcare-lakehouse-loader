# Audit: S3 data events on the lake buckets. Each record names the vended session, whose name
# carries the Polaris principal and table (SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL).
data "aws_iam_policy_document" "trail" {
  statement {
    actions   = ["s3:GetBucketAcl"]
    resources = [aws_s3_bucket.b["trail"].arn]
    principals {
      type        = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }
  }
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.b["trail"].arn}/AWSLogs/${var.account_id}/*"]
    principals {
      type        = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "s3:x-amz-acl"
      values   = ["bucket-owner-full-control"]
    }
  }
}

resource "aws_s3_bucket_policy" "trail" {
  bucket = aws_s3_bucket.b["trail"].id
  policy = data.aws_iam_policy_document.trail.json
}

# Encrypted with S3-managed keys: the records name sessions, principals and tables, never row data.
#trivy:ignore:AVD-AWS-0015
resource "aws_cloudtrail" "lake" {
  name           = "${var.name}-lake-data-events"
  s3_bucket_name = aws_s3_bucket.b["trail"].id
  # Digest files make deleted or altered log files detectable (trivy).
  enable_log_file_validation = true
  advanced_event_selector {
    name = "Lake object access"
    field_selector {
      field  = "eventCategory"
      equals = ["Data"]
    }
    field_selector {
      field  = "resources.type"
      equals = ["AWS::S3::Object"]
    }
    field_selector {
      field       = "resources.ARN"
      starts_with = ["${aws_s3_bucket.b["lake-phi"].arn}/", "${aws_s3_bucket.b["lake-deid"].arn}/"]
    }
  }
  depends_on = [aws_s3_bucket_policy.trail]
}
