resource "aws_iam_role" "step_function_role" {
  name_prefix        = "role-step-function-"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "states.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "step_function_role_policy_document" {
  statement {
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.extract_lambda_name}:*",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.transform_lambda_name}:*",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.load_lambda_name}:*"
    ]
  }

}

resource "aws_iam_policy" "step_function_role_policy" {
  name_prefix = var.state_machine_name
  policy      = data.aws_iam_policy_document.step_function_role_policy_document.json
}

resource "aws_iam_role_policy_attachment" "step_function_role_policy_attachment" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_function_role_policy.arn
}
