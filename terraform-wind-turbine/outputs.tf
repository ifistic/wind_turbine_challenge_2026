output "data_lake_bucket" {
  description = "S3 bucket holding raw/bronze/silver/gold data"
  value       = aws_s3_bucket.data_lake.bucket
}

output "glue_scripts_bucket" {
  description = "S3 bucket to upload bronze.py/silver.py/gold.py into"
  value       = aws_s3_bucket.glue_scripts.bucket
}

output "rds_endpoint" {
  description = "RDS PostgreSQL connection endpoint"
  value       = aws_db_instance.main.endpoint
}

output "db_credentials_secret_name" {
  description = "Secrets Manager secret name holding DB credentials"
  value       = aws_secretsmanager_secret.db_credentials.name
}

output "glue_workflow_name" {
  description = "Name of the Glue workflow — run with: aws glue start-workflow-run --name <this>"
  value       = aws_glue_workflow.pipeline.name
}

output "sns_alerts_topic_arn" {
  description = "SNS topic ARN — subscribe your email to this for job failure alerts"
  value       = aws_sns_topic.pipeline_alerts.arn
}
