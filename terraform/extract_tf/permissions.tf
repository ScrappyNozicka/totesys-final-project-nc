#Assume role


data "aws_iam_policy_document" "assume_role" {
   statement {
       effect = "Allow"


   principals {
       type = "Service"
       identifiers = ["lambda.amazonaws.com"]
   }


   actions = ["sts:AssumeRole"]
   }
}


resource "aws_iam_role" "extract_iam" {
   name  = "extract-iam"
   assume_role_policy = data.aws_iam_policy_document.assume_role.json
}


#Read to S3


data "aws_iam_policy_document" "s3policy-doc" {
   statement {
       effect = "Allow"
       actions = [
           "s3:PutObject",
           "s3:GetObject",
           "s3:AbortMultipartUpload",
           "s3:ListBucket",
           "s3:DeleteObject",
           "s3:GetObjectVersion",
           "s3:ListMultipartUploadParts"
       ]
        resources = ["arn:aws:s3:::${var.ingestion_bucket}", "arn:aws:s3:::${var.ingestion_bucket}/*"
        ]
   }
}
resource "aws_iam_policy" "s3policy" {
 name        = "s3-full-access-policy"
 description = "Allow full access to s3"
 policy      = data.aws_iam_policy_document.s3policy-doc.json
}
resource "aws_iam_policy_attachment" "s3-fullaccess-attach" {
 name       = "s3-fullaccess-attachment"
 roles      = [aws_iam_role.extract_iam.name]
 policy_arn = aws_iam_policy.s3policy.arn
}





resource "aws_lambda_permission" "allow_eventbridge" {
statement_id  = "AllowExecutionFromEventBridge"
action        = "lambda:InvokeFunction"
function_name = aws_lambda_function.extract_lambda.function_name
principal     = "events.amazonaws.com"
source_arn    = aws_cloudwatch_event_rule.lambda_schedule.arn
}


resource "aws_cloudwatch_log_group" "lambda_log_group" {
 name              = "/aws/lambda/extract_lambda"
 retention_in_days = 14
}


data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:*:*"
    ]
  }

}

resource "aws_iam_policy" "lambda_logging_cloudwatch" {
name        = "lambda_logging_policy"
description = "allows lambda function to write to cloudwatch"
policy = jsonencode({
  Version = "2012-10-17"
  Statement = [
    {
      Action   = "logs:*"
      Resource = "*"
      Effect   = "Allow"
    }
  ]
})
}
resource "aws_iam_role_policy_attachment" "lambda_logging_attach" {
policy_arn = aws_iam_policy.lambda_logging_cloudwatch.arn
role      = aws_iam_role.extract_iam.name
}



data "aws_iam_policy_document" "sns_topic_policy" {
   policy_id = "__default_policy_ID"


   statement {
       sid     = "__default_statement_ID"
       actions = [
           "SNS:Subscribe",
           "SNS:SetTopicAttributes",
           "SNS:RemovePermission",
           "SNS:Receive",
           "SNS:Publish",
           "SNS:ListSubscriptionsByTopic",
           "SNS:GetTopicAttributes",
           "SNS:DeleteTopic",
           "SNS:AddPermission"
       ]
       effect    = "Allow"
       resources = [aws_sns_topic.terraform_alerts.arn]
       principals {
           type        = "AWS"
           identifiers = ["*"]
       }
       condition {
           test     = "StringEquals"
           variable = "AWS:SourceOwner"
           values = [
               "arn:aws:sns:eu-west-2:205930621103:my-terraform-alerts:*",
           ]
       }
   }


   statement {
       sid       = "Allow_Publish_Alarms"
       actions   = ["SNS:Publish"]
       resources = [aws_sns_topic.terraform_alerts.arn]
       principals {
           type        = "Service"
           identifiers = ["cloudwatch.amazonaws.com"]
       }
   }
}
resource "aws_iam_policy" "lambda_logging_sns" {
 name        = "lambda_logging_sns_policy"
 description = "Allows Lambda to publish logs and send SNS alerts"
 policy = jsonencode({
   Version = "2012-10-17"
   Statement = [
     {
       Effect   = "Allow"
       Action   = [
         "logs:CreateLogGroup",
         "logs:CreateLogStream",
         "logs:PutLogEvents",
         "sns:Publish"
       ]
       Resource = "*"
     }
   ]
 })
}


resource "aws_iam_role_policy_attachment" "lambda_logging_sns_attach" {
 policy_arn = aws_iam_policy.lambda_logging_sns.arn
 role       = aws_iam_role.extract_iam.name
}



resource "aws_iam_policy" "secretsmanager_policy" {
 name        = "LambdaSecretsManagerAccess"
 description = "Allow Lambda to get secrets from AWS Secrets Manager"
  policy = jsonencode({
   Version = "2012-10-17"
   Statement = [
     {
       Effect   = "Allow"
       Action   = "secretsmanager:GetSecretValue"
       Resource = "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:ketts-lough-secrets-*"
     }
   ]
 })
}


resource "aws_iam_role_policy_attachment" "attach_secrets_policy" {
 policy_arn = aws_iam_policy.secretsmanager_policy.arn
 role       = aws_iam_role.extract_iam.name
}


resource "aws_iam_role" "transform_iam" {
  name               = "transform-iam"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}


resource "aws_iam_role" "load_iam" {
  name               = "load-iam"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

resource "aws_iam_role_policy_attachment" "transform_lambda_logging_attach" {
  policy_arn = aws_iam_policy.lambda_logging_cloudwatch.arn
  role       = aws_iam_role.transform_iam.name
}

resource "aws_iam_role_policy_attachment" "load_lambda_logging_attach" {
  policy_arn = aws_iam_policy.lambda_logging_cloudwatch.arn
  role       = aws_iam_role.load_iam.name
}

