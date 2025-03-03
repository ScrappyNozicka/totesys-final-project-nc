resource "aws_sfn_state_machine" "sfn_state_machine" {
  name_prefix = var.state_machine_name
  definition = templatefile("${path.module}/pipeline.json",
    { aws_region      = data.aws_region.current.name,
      aws_account_num = data.aws_caller_identity.current.account_id,
      function_name_1 = var.extract_lambda_name,
      function_name_2 = var.transform_lambda_name,
      function_name_3 = var.load_lambda_name
  })
  role_arn = aws_iam_role.step_function_role.arn
}