data "aws_caller_identity" "current" {}
data "aws_iam_session_context" "operator" {
  arn = data.aws_caller_identity.current.arn
}
data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  model         = jsondecode(file("${path.module}/../../stack/bootstrap/model.json"))
  bucket_prefix = "${var.name}-${var.account_id}-"
  buckets       = { for b in ["raw", "lake-phi", "lake-deid", "shred-journal", "artifacts", "trail", "cdc-staging"] : b => "${local.bucket_prefix}${b}" }
  azs           = slice(data.aws_availability_zones.available.names, 0, 2)
  # The operator's own role (SSO): the break-glass identity allowed past the lake guardrail.
  operator_role_arn = data.aws_iam_session_context.operator.issuer_arn
  dns_namespace     = "lakehouse.internal"
}
