# Partners (loader/partners.py, loader/partner_keys.py, loader/offboarding.py), read from the
# contracts: a master key and a drop identity per partner.
locals {
  contracts       = [for f in fileset("${path.module}/../../contracts", "*.yaml") : yamldecode(file("${path.module}/../../contracts/${f}"))]
  partners        = toset([for c in local.contracts : c.partner])
  partner_sources = { for p in local.partners : p => [for c in local.contracts : c.source if c.partner == p] }
}

# ---------------------------------------------------------------- partner-deletion role
# The only identity that may schedule (or cancel) deletion of a partner's master key, after an
# approved delete. Assumable by the operator's own SSO role only: no task, flow or test can delete a
# partner's key. `scripts/sandbox.py schedule-key-deletion <partner>` uses it.
resource "aws_iam_role" "partner_deletion" {
  name = "${var.name}-partner-deletion"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { AWS = local.operator_role_arn } }]
  })
}

resource "aws_iam_role_policy" "partner_deletion" {
  role = aws_iam_role.partner_deletion.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["kms:ScheduleKeyDeletion", "kms:CancelKeyDeletion", "kms:DescribeKey"],
    Resource = [for k in aws_kms_key.partner : k.arn] }]
  })
}

# ---------------------------------------------------------------- master key per partner
data "aws_iam_policy_document" "partner_key" {
  statement {
    sid       = "AccountManagesTheKeyThroughIAM"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.account_id}:root"]
    }
  }
  statement {
    sid       = "OnlyThePartnerDeletionRoleDeletes"
    effect    = "Deny"
    actions   = ["kms:ScheduleKeyDeletion", "kms:CancelKeyDeletion"]
    resources = ["*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "ArnNotEquals"
      variable = "aws:PrincipalArn"
      values   = [aws_iam_role.partner_deletion.arn]
    }
  }
}

resource "aws_kms_key" "partner" {
  for_each                = local.partners
  description             = "${var.name} master key of partner ${each.key}"
  deletion_window_in_days = 30 # the last chance to cancel an approved delete
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.partner_key.json
}

resource "aws_kms_alias" "partner" {
  for_each      = aws_kms_key.partner
  name          = "alias/${var.name}-partner-${each.key}"
  target_key_id = each.value.key_id
}

# ---------------------------------------------------------------- drop identity per partner
# What the partner itself uses: put into its sources' drop prefixes, nothing else. A freeze adds a
# deny-all inline policy ("frozen"). Here the admin task (tests) stands in for the partner's account.
resource "aws_iam_role" "partner" {
  for_each = local.partners
  name     = "${var.name}-partner-${each.key}"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { AWS = aws_iam_role.toolbox_admin_task.arn } }]
  })
}

resource "aws_iam_role_policy" "partner" {
  for_each = local.partners
  role     = aws_iam_role.partner[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:PutObject"],
      Resource = [for src in local.partner_sources[each.key] : "${aws_s3_bucket.b["raw"].arn}/drops/${src}/*"] },
      { Effect = "Allow", Action = ["kms:GenerateDataKey", "kms:Encrypt"], Resource = [aws_kms_key.key["raw"].arn] },
    ]
  })
}

# ---------------------------------------------------------------- what the loader may do with them
# Flow runs wrap and unwrap KEKs; the admin task (operators) also freezes and unfreezes.
resource "aws_iam_role_policy" "toolbox_partner_keys" {
  role = aws_iam_role.toolbox_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["kms:Encrypt", "kms:Decrypt", "kms:DescribeKey"],
    Resource = [for k in aws_kms_key.partner : k.arn] }]
  })
}

resource "aws_iam_role_policy" "toolbox_admin_partners" {
  role = aws_iam_role.toolbox_admin_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["kms:Encrypt", "kms:Decrypt", "kms:DescribeKey", "kms:DisableKey", "kms:EnableKey"],
      Resource = [for k in aws_kms_key.partner : k.arn] },
      { Effect = "Allow", Action = ["iam:PutRolePolicy", "iam:DeleteRolePolicy"], Resource = [for r in aws_iam_role.partner : r.arn] },
      { Effect = "Allow", Action = ["sts:AssumeRole"], Resource = [for r in aws_iam_role.partner : r.arn] },
    ]
  })
}
