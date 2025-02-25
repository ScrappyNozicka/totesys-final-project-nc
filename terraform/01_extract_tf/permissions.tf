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
         resources = ["arn:aws:s3:::${BUCKET_NAME}", "arn:aws:s3:::${BUCKET_NAME}/*" 
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