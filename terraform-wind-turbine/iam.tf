# Least-privilege IAM role for the Glue jobs: scoped to only this project's
# S3 bucket, this project's secret, and standard Glue service permissions.
# No S3FullAccess, no wildcard secret access.

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "glue_job" {
  name = "${var.project_name}-glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

# AWS-managed baseline (CloudWatch Logs, Glue catalog access, etc.)
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-glue-s3-access"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DataLakeReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Sid    = "ScriptsReadOnly"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.glue_scripts.arn,
          "${aws_s3_bucket.glue_scripts.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_secrets_access" {
  name = "${var.project_name}-glue-secrets-access"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadDbCredentials"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = aws_secretsmanager_secret.db_credentials.arn
    }]
  })
}

# Allows Glue to create/manage the elastic network interfaces it needs to
# run inside your VPC (required for jobs that connect to RDS).
resource "aws_iam_role_policy" "glue_vpc_access" {
  name = "${var.project_name}-glue-vpc-access"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "GlueVpcEni"
      Effect = "Allow"
      Action = [
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcAttribute"
      ]
      Resource = "*" # these Describe/Create/Delete ENI actions don't support resource-level restriction
    }]
  })
}
