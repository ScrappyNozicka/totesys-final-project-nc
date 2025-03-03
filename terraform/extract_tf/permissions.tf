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
  name = "lambda_logging_cloudwatch"
  policy      = data.aws_iam_policy_document.cw_document.json
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

