# Replaces the local Postgres.app/Homebrew Postgres instance. Holds the same
# three tables as before: Processed_data, gold_summary_statistics, gold_anomalies.

resource "aws_db_instance" "main" {
  identifier     = "${var.project_name}-${var.environment}"
  engine         = "postgres"
  engine_version = "18.1"

  instance_class        = var.db_instance_class
  allocated_storage     = var.db_allocated_storage_gb
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = var.db_name
  username = var.db_username
  # Password is generated in secrets.tf; referenced here to avoid a
  # circular data flow through Secrets Manager on first apply.
  password = random_password.db_password.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.data_pipeline.id]

  publicly_accessible = false # only reachable from inside the VPC (i.e. by Glue)

  backup_retention_period = 7
  skip_final_snapshot     = var.environment != "prod" # dev/staging: skip snapshot for easy teardown
  deletion_protection     = var.environment == "prod"

  tags = var.tags
}
