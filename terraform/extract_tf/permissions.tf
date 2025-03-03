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
        resources = ["arn:aws:s3:::ingestion-bucket-ketts-lough", "arn:aws:s3:::ingestion-bucket-ketts-lough/*"
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


resource "aws_iam_policy" "lambda_logging_cloudwatch" {
 name        = "lambda_logging_policy"
 description = "allows lambda function to write to cloudwatch"
 policy = jsonencode({
   Version = "2012-10-17"
   Statement = [
     {
       Action   =  ["logs:CreateLogStream",
          "logs:PutLogEvents"
       ]
       Resource = "arn:aws:logs:eu-west-2:205930621103:log-group:/aws/lambda/extract_lambda:*"
       Effect   = "Allow"
     }
   ]
 })
}
resource "aws_iam_role_policy_attachment" "lambda_logging_attach" {
 policy_arn = aws_iam_policy.lambda_logging_cloudwatch.arn
 role      = aws_iam_role.extract_iam.name
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
        Resource = "arn:aws:secretsmanager:eu-west-2:205930621103:secret:ketts-lough-secrets-*"
      }
    ]
  })
}


resource "aws_iam_role" "extract_iam" {
  name = "LambdaExecutionRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Effect    = "Allow"
        Sid       = ""
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "attach_secrets_policy" {
  policy_arn = aws_iam_policy.secretsmanager_policy.arn
  role       = aws_iam_role.extract_iam.name
}


resource "aws_lambda_function" "extract_lambda" {
  function_name = "ExtractDataLambda"
  role          = aws_iam_role.extract_iam.arn
  handler       = "extract_main_script.extract_main_script"
  runtime       = "python3.13"
  filename      = "path/to/your/deployment/package.zip"
  
  environment {
    variables = {
      SECRET_NAME = "ketts-lough-secrets"
    }
  }
}
