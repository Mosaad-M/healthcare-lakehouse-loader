# What `scripts/sandbox.py` needs to push images, run tasks and read logs.
output "region" { value = var.region }
output "cluster" { value = aws_ecs_cluster.main.name }
output "subnets" { value = aws_subnet.private[*].id }
output "security_group" { value = aws_security_group.internal.id }
output "log_group" { value = aws_cloudwatch_log_group.main.name }
output "ecr" { value = { for k, r in aws_ecr_repository.r : k => r.repository_url } }
output "task_definitions" {
  value = {
    db_init       = aws_ecs_task_definition.db_init.arn
    polaris_admin = aws_ecs_task_definition.polaris_admin.arn
    toolbox       = aws_ecs_task_definition.toolbox.arn
    toolbox_admin = aws_ecs_task_definition.toolbox_admin.arn
  }
}
output "services" { value = ["polaris", "prefect", "prefect-worker", "partner-api"] }
output "dms_task" { value = aws_dms_replication_task.clinic.replication_task_arn }
output "toolbox_task_role" { value = aws_iam_role.toolbox_task.arn }
output "toolbox_admin_task_role" { value = aws_iam_role.toolbox_admin_task.arn }
output "execution_role" { value = aws_iam_role.execution.arn }
output "artifacts_bucket" { value = aws_s3_bucket.b["artifacts"].id }
output "bucket_prefix" { value = local.bucket_prefix }
output "doris_instance" { value = aws_instance.doris.id }
output "emr_application" { value = aws_emrserverless_application.spark.id }
