# SNS topic + CloudWatch alarms so a failed Glue job actually notifies you,
# rather than silently sitting in the console. Mirrors the alerting pattern
# you already have on bitcoin-market-pipeline (Slack/email on failure).

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_name}-alerts"
  tags = var.tags
}

# Subscribe your email after apply with:
#   aws sns subscribe --topic-arn <arn from output> --protocol email --notification-endpoint you@example.com
# (left out of Terraform so the address isn't committed to the repo)

resource "aws_cloudwatch_metric_alarm" "bronze_failed" {
  alarm_name          = "${var.project_name}-bronze-job-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods   = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Triggers when the bronze Glue job has any failed tasks"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.bronze.name
  }
}

resource "aws_cloudwatch_metric_alarm" "silver_failed" {
  alarm_name          = "${var.project_name}-silver-job-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods   = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Triggers when the silver Glue job has any failed tasks"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.silver.name
  }
}

resource "aws_cloudwatch_metric_alarm" "gold_failed" {
  alarm_name          = "${var.project_name}-gold-job-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods   = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Triggers when the gold Glue job has any failed tasks"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.gold.name
  }
}
