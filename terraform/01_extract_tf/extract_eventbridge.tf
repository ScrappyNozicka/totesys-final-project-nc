resource "aws_cloudwatch_event_rule" "lambda_schedule" {
    name = "lambda-schedule-rule"
    description = "Triggers Lambda every 30 minutes"
    schedule_expression = "rate(30 minutes)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.lambda_schedule.name
  target_id = "lambda"
  arn       = aws_lambda_function.extract_lambda.arn 
}
