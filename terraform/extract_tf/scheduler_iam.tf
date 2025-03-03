resource "aws_iam_role" "scheduler_role" {
  name_prefix        = "role-scheduler-"
  assume_role_policy = <<EOF
    {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "scheduler.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
    }
    EOF
}

data "aws_iam_policy_document" "scheduer_role_policy_document" {

  statement {
    effect = "Allow"

    actions = [
      "states:StartExecution"
    ]
    resources = [
      "arn:aws:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${resource.aws_sfn_state_machine.sfn_state_machine.name}"
    ]
  }
}

resource "aws_iam_policy" "scheduler_role_policy" {
  name_prefix = var.scheduler_name
  policy      = data.aws_iam_policy_document.scheduer_role_policy_document.json
}

resource "aws_iam_role_policy_attachment" "scheduler_role_policy_attachment" {
  role       = aws_iam_role.scheduler_role.name
  policy_arn = aws_iam_policy.scheduler_role_policy.arn
}