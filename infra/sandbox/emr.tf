# EMR Serverless runs table maintenance: one job per maintenance batch, inside the VPC so
# it reaches Polaris, as the `maintenance` principal (credential read from Secrets Manager).
variable "emr_release" {
  type    = string
  default = "emr-7.14.0"
}

resource "aws_emrserverless_application" "spark" {
  name          = "${var.name}-maintenance"
  release_label = var.emr_release
  type          = "spark"
  architecture  = "ARM64"
  maximum_capacity {
    cpu    = "8 vCPU"
    memory = "32 GB"
  }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 10
  }
  network_configuration {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.internal.id]
  }
  lifecycle { ignore_changes = [scheduler_configuration] } # AWS fills in its defaults
}
