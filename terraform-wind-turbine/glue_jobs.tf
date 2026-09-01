# Three Glue jobs mirroring src/pipelines/bronze.py, silver.py, gold.py,
# chained into a workflow so a single trigger runs all three in order —
# the cloud equivalent of `python main.py`.
#
# NOTE: this expects bronze.py, silver.py, gold.py to already exist at
# these S3 paths (upload them after converting local-path I/O to S3 paths —
# see the accompanying README for what needs to change in each script).

locals {
  script_bucket = aws_s3_bucket.glue_scripts.bucket
  data_bucket   = aws_s3_bucket.data_lake.bucket
}

resource "aws_glue_job" "bronze" {
  name              = "${var.project_name}-bronze"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 30 # minutes

  command {
    name            = "glueetl"
    script_location = "s3://${local.script_bucket}/bronze.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--data_bucket"                     = local.data_bucket
    "--secret_name"                     = aws_secretsmanager_secret.db_credentials.name
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  connections = [aws_glue_connection.rds_postgres.name]

  tags = var.tags
}

resource "aws_glue_job" "silver" {
  name              = "${var.project_name}-silver"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${local.script_bucket}/silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--data_bucket"                     = local.data_bucket
    "--secret_name"                     = aws_secretsmanager_secret.db_credentials.name
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  connections = [aws_glue_connection.rds_postgres.name]

  tags = var.tags
}

resource "aws_glue_job" "gold" {
  name              = "${var.project_name}-gold"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${local.script_bucket}/gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--data_bucket"                     = local.data_bucket
    "--secret_name"                     = aws_secretsmanager_secret.db_credentials.name
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  connections = [aws_glue_connection.rds_postgres.name]

  tags = var.tags
}

# Workflow ties the three jobs together with triggers so they run in
# sequence: bronze -> silver -> gold, same order as main.py.
resource "aws_glue_workflow" "pipeline" {
  name = "${var.project_name}-workflow"
  tags = var.tags
}

resource "aws_glue_trigger" "start_bronze" {
  name          = "${var.project_name}-start-bronze"
  type          = "ON_DEMAND" # trigger manually or via EventBridge schedule (see glue_trigger.scheduled below)
  workflow_name = aws_glue_workflow.pipeline.name

  actions {
    job_name = aws_glue_job.bronze.name
  }
}

resource "aws_glue_trigger" "bronze_to_silver" {
  name          = "${var.project_name}-bronze-to-silver"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.bronze.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.silver.name
  }
}

resource "aws_glue_trigger" "silver_to_gold" {
  name          = "${var.project_name}-silver-to-gold"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.silver.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.gold.name
  }
}

# Optional: uncomment to run the whole workflow daily at 2am UTC instead of
# triggering it manually.
# resource "aws_glue_trigger" "scheduled" {
#   name          = "${var.project_name}-daily-schedule"
#   type          = "SCHEDULED"
#   schedule      = "cron(0 2 * * ? *)"
#   workflow_name = aws_glue_workflow.pipeline.name
#
#   actions {
#     job_name = aws_glue_job.bronze.name
#   }
# }
