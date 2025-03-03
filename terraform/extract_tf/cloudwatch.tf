
resource "aws_sns_topic" "lambda_alerts" {
 name = "my-lambda-alerts"
}
resource "aws_sns_topic_subscription" "email_subscription" {
 topic_arn = aws_sns_topic.lambda_alerts.arn
 protocol  = "email"
 endpoint  = "kettslough@gmail.com"
}
resource "aws_cloudwatch_log_metric_filter" "error_filter" {
 name = "ErrorFilter"
 log_group_name = aws_cloudwatch_log_group.lambda_log_group.name
 pattern = "{ $.level = \"ERROR\" }"
 metric_transformation {
    name = "LambdaErrorCount"
    namespace = "LambdaErrors"
    value = "1"
 }
}
resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm" {
  alarm_name          = "LambdaErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "LambdaErrorCount"
  namespace           = "LambdaErrors"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm when Lambda function logs an ERROR"
  alarm_actions       = [aws_sns_topic.lambda_alerts.arn]
}