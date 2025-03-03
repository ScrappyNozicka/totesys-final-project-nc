
resource "aws_sns_topic" "terraform_alerts" {
 name = "my-terraform-alerts"
}
resource "aws_sns_topic_subscription" "email_subscription" {
 topic_arn = aws_sns_topic.terraform_alerts.arn
 protocol  = "email"
 endpoint  = "kettslough@gmail.com"
}
resource "aws_cloudwatch_log_metric_filter" "error_filter" {
 name = "ErrorFilter"
 log_group_name = aws_cloudwatch_log_group.lambda_log_group.name
 pattern = "ERROR"
 metric_transformation {
    name = "TerraformErrorCount"
    namespace = "TerraformErrors"
    value = "1"
 }
}
resource "aws_cloudwatch_metric_alarm" "terraform_error_alarm" {
  alarm_name          = "TerraformErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "TerraformErrorCount"
  namespace           = "TerraformErrors"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm when Lambda function logs an ERROR"
  alarm_actions       = [aws_sns_topic.terraform_alerts.arn]
}