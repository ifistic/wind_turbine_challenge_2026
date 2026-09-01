# wind_turbine_challenge_2026 — AWS Terraform

Infrastructure for migrating the local PySpark/PostgreSQL pipeline to AWS:
S3 (data lake) + Glue (Spark processing) + RDS PostgreSQL + Secrets Manager
+ CloudWatch alarms, chained via a Glue Workflow.

## Prerequisites

- Terraform >= 1.5.0
- AWS CLI configured with credentials (`aws configure`)
- An AWS account with permission to create VPC, RDS, Glue, IAM, S3, SNS resources

## Usage

```bash
terraform init
terraform plan
terraform apply
```

Review the plan carefully before applying — this creates billable resources
(RDS instance, VPC endpoint, Glue jobs when run). RDS on `db.t4g.micro` and a
Glue job running only when triggered keep this cheap for portfolio/dev use,
but it is not free-tier-guaranteed depending on your account's usage history.

## After `apply`

1. **Convert the pipeline scripts to Glue jobs.** `src/pipelines/bronze.py`,
   `silver.py`, `gold.py` currently read/write local paths
   (`data/bronze/`, `data/silver/`, etc.) and connect to `localhost:5432`.
   For each script:
   - Replace local file paths with `s3://<data_lake_bucket>/bronze/`,
     `.../silver/`, `.../gold/` (from `terraform output data_lake_bucket`)
   - Replace the `.env`-based DB connection with a call to
     `boto3.client('secretsmanager').get_secret_value(SecretId=...)` using
     the secret name from `terraform output db_credentials_secret_name`
   - Wrap the script body to accept Glue job arguments via
     `awsglue.utils.getResolvedOptions`

2. **Upload the converted scripts:**
   ```bash
   aws s3 cp bronze.py s3://$(terraform output -raw glue_scripts_bucket)/bronze.py
   aws s3 cp silver.py s3://$(terraform output -raw glue_scripts_bucket)/silver.py
   aws s3 cp gold.py   s3://$(terraform output -raw glue_scripts_bucket)/gold.py
   ```

3. **Upload your source data:**
   ```bash
   aws s3 cp data.zip s3://$(terraform output -raw data_lake_bucket)/raw/data.zip
   ```

4. **Subscribe to failure alerts:**
   ```bash
   aws sns subscribe \
     --topic-arn $(terraform output -raw sns_alerts_topic_arn) \
     --protocol email \
     --notification-endpoint you@example.com
   ```
   (Confirm the subscription via the email AWS sends you.)

5. **Run the pipeline:**
   ```bash
   aws glue start-workflow-run --name $(terraform output -raw glue_workflow_name)
   ```

6. **Check results** — connect to RDS from a bastion/VPN or temporarily set
   `publicly_accessible = true` in `rds.tf` for local testing (not
   recommended to leave on), or query via a Glue Studio notebook inside the VPC.

## Teardown

```bash
terraform destroy
```

`skip_final_snapshot` is `true` for non-prod environments, so this fully
removes the RDS instance without leaving a snapshot behind — check
`var.environment` before running this against anything you want to keep.

## What's intentionally left out

- **No CI/CD pipeline yet** — a GitHub Actions workflow to run
  `terraform plan` on PRs and `terraform apply` on merge to main is a
  natural next step once this is stable.
- **No Athena/Glue Data Catalog crawler** — if you want to query the gold
  Parquet files directly from S3 with SQL (in addition to RDS), add an
  `aws_glue_crawler` pointed at the gold prefix and query via Athena.
- **State is local** — the `backend "s3"` block in `versions.tf` is
  commented out; uncomment and point it at a state bucket once you're
  applying this repeatedly, so you're not tracking `.tfstate` in git.
