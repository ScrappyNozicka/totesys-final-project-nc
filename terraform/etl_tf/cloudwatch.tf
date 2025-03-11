resource "aws_sns_topic" "terraform_alerts" {
  name = "my-terraform-alerts"
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.terraform_alerts.arn
  protocol  = "email"
  endpoint  = "kettslough@gmail.com"
}

resource "aws_cloudwatch_log_metric_filter" "extract_error_filter" {
  name           = "ExtractErrorFilter"
  log_group_name = "/aws/lambda/${var.extract_lambda_name}"
  pattern        = "ERROR"
  metric_transformation {
    name      = "ExtractErrorCount"
    namespace = "TerraformErrors"
    value     = "1"
  }
  depends_on = [aws_lambda_function.extract_lambda]
}

resource "aws_cloudwatch_metric_alarm" "extract_error_alarm" {
  alarm_name          = "ExtractErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ExtractErrorCount"
  namespace           = "TerraformErrors"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm when Extract Lambda function logs an ERROR"
  alarm_actions       = [aws_sns_topic.terraform_alerts.arn]
}

resource "aws_cloudwatch_log_metric_filter" "transform_error_filter" {
  name           = "TransformErrorFilter"
  log_group_name = "/aws/lambda/${var.transform_lambda_name}"
  pattern        = "ERROR"
  metric_transformation {
    name      = "TransformErrorCount"
    namespace = "TerraformErrors"
    value     = "1"
  }
  depends_on = [aws_lambda_function.transform_lambda]
}

resource "aws_cloudwatch_metric_alarm" "transform_error_alarm" {
  alarm_name          = "TransformErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "TransformErrorCount"
  namespace           = "TerraformErrors"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm when Transform Lambda function logs an ERROR"
  alarm_actions       = [aws_sns_topic.terraform_alerts.arn]
}

resource "aws_cloudwatch_log_metric_filter" "load_error_filter" {
  name           = "LoadErrorFilter"
  log_group_name = "/aws/lambda/${var.load_lambda_name}"
  pattern        = "ERROR"
  metric_transformation {
    name      = "LoadErrorCount"
    namespace = "TerraformErrors"
    value     = "1"
  }
  depends_on = [aws_lambda_function.load_lambda]
}

resource "aws_cloudwatch_metric_alarm" "load_error_alarm" {
  alarm_name          = "LoadErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "LoadErrorCount"
  namespace           = "TerraformErrors"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm when Load Lambda function logs an ERROR"
  alarm_actions       = [aws_sns_topic.terraform_alerts.arn]
}
