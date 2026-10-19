# One key per sensitivity, one for the shred journal, and the master key that wraps the
# per-subject data keys. IAM policies decide who may use each; the key policy delegates
# to IAM. Sandbox keys are deleted after the minimum 7-day window on teardown.
resource "aws_kms_key" "key" {
  for_each                = toset(["raw", "phi", "deid", "journal", "master"])
  description             = "${var.name} ${each.key}"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

resource "aws_kms_alias" "key" {
  for_each      = aws_kms_key.key
  name          = "alias/${var.name}-${each.key}"
  target_key_id = each.value.key_id
}
