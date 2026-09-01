# Generates and stores the RDS master password so it never appears in
# .tfvars, code, or command history. Glue jobs read it at runtime via the
# AWS SDK instead of an .env file.

resource "random_password" "db_password" {
  length  = 24
  special = false # avoids characters that need extra escaping in JDBC connection strings
}

resource "aws_secretsmanager_secret" "db_credentials" {
  name        = "${var.project_name}/${var.environment}/db-credentials"
  description = "RDS PostgreSQL credentials for the wind turbine pipeline"

  tags = var.tags
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id

  secret_string = jsonencode({
    username = var.db_username
    password = random_password.db_password.result
    host     = aws_db_instance.main.address
    port     = aws_db_instance.main.port
    dbname   = var.db_name
  })
}
