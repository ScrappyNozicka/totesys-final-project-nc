resource "aws_scheduler_schedule" "state_machine_scheduler" {
  name_prefix         = var.scheduler_name
  schedule_expression = "rate(30 minutes)"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    role_arn = aws_iam_role.scheduler_role.arn
    arn      = aws_sfn_state_machine.sfn_state_machine.arn
  }
  depends_on = [aws_sfn_state_machine.sfn_state_machine]
}

