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
