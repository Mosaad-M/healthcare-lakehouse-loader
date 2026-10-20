variable "account_id" {
  description = "The sandbox account (terraform.tfvars). Terraform refuses to touch any other."
  type        = string
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "profile" {
  description = "AWS CLI profile (SSO) for the sandbox account (terraform.tfvars)."
  type        = string
}

variable "name" {
  description = "Prefix for every resource name."
  type        = string
  default     = "lh"
}

variable "image_tag" {
  description = "Tag of the toolbox and prefect-worker images pushed to ECR."
  type        = string
  default     = "sandbox"
}

variable "journal_retention_days" {
  description = "Object Lock retention of shred-journal entries. GOVERNANCE mode in the sandbox so teardown can bypass it; production uses COMPLIANCE and years."
  type        = number
  default     = 1
}

variable "alert_emails" {
  description = "Who gets budget and cost-anomaly alerts (terraform.tfvars). Empty: no alerts are created."
  type        = list(string)
  default     = []
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget: alerts at 80% forecast and 100% actual."
  type        = number
  default     = 50
}

variable "anomaly_alert_usd" {
  description = "Alert on a cost anomaly whose total impact reaches this many dollars."
  type        = number
  default     = 10
}

variable "create_anomaly_monitor" {
  description = "An account allows one per-service anomaly monitor; false if it already has one."
  type        = bool
  default     = true
}
