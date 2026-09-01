# Lets Glue jobs run inside the VPC so they can reach RDS on its private
# subnet. Without this, Glue jobs run outside your VPC and can only reach
# public endpoints (S3 via the internet works, but RDS with
# publicly_accessible = false would not be reachable).

resource "aws_glue_connection" "rds_postgres" {
  name            = "${var.project_name}-rds-connection"
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:postgresql://${aws_db_instance.main.endpoint}/${var.db_name}"
    USERNAME             = var.db_username
    PASSWORD             = random_password.db_password.result
  }

  physical_connection_requirements {
    availability_zone      = aws_subnet.private[0].availability_zone
    security_group_id_list = [aws_security_group.data_pipeline.id]
    subnet_id               = aws_subnet.private[0].id
  }

  tags = var.tags
}
