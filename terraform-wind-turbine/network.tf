# Minimal networking: one VPC, two private subnets (RDS requires at least two
# AZs for a DB subnet group even in single-AZ deployments), one security group
# shared by RDS and the Glue ENIs so they can talk to each other.

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(var.tags, { Name = "${var.project_name}-vpc" })
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = merge(var.tags, { Name = "${var.project_name}-private-${count.index}" })
}

resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet-group"
  subnet_ids = aws_subnet.private[*].id

  tags = var.tags
}

# Security group used by both RDS and Glue's VPC connection (self-referencing
# rule lets Glue's Spark executors reach Postgres on 5432).
resource "aws_security_group" "data_pipeline" {
  name        = "${var.project_name}-sg"
  description = "Allows Glue jobs to reach RDS PostgreSQL"
  vpc_id      = aws_vpc.main.id

  tags = var.tags
}

resource "aws_security_group_rule" "postgres_self_ingress" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.data_pipeline.id
  source_security_group_id = aws_security_group.data_pipeline.id
}

resource "aws_security_group_rule" "all_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.data_pipeline.id
}

# Glue needs a NAT or VPC endpoints to reach S3/Secrets Manager from inside
# the VPC. Gateway endpoint for S3 is free and avoids a NAT gateway entirely
# for this use case.
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.main.id
  service_name = "com.amazonaws.${var.aws_region}.s3"

  route_table_ids = [aws_route_table.private.id]

  tags = var.tags
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  tags   = merge(var.tags, { Name = "${var.project_name}-private-rt" })
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}
