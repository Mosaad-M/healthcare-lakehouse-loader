# The AWS sandbox: the production shape at the smallest sizes, in one
# sandbox account. Synthetic data only. `make sandbox-plan`, `make sandbox-apply`,
# `make sandbox-test`, `make sandbox-destroy` (see the README, Running on AWS).
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 6.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
    time   = { source = "hashicorp/time", version = "~> 0.13" }
  }
}

provider "aws" {
  region  = var.region
  profile = var.profile
  # Never act on any account but the sandbox: no real PHI may ever be near this stack.
  allowed_account_ids = [var.account_id]
  default_tags {
    tags = { Project = "lakehouse-loader", Environment = "sandbox", ManagedBy = "terraform" }
  }
}
