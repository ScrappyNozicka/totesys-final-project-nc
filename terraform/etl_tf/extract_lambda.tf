resource "aws_lambda_function" "extract_lambda" {
  filename         = data.archive_file.extract_lambda_package.output_path
  source_code_hash = data.archive_file.extract_lambda_package.output_base64sha256
  function_name    = var.extract_lambda_name
  runtime          = var.python_runtime
  role             = aws_iam_role.extract_iam.arn
  handler          = "extract_main_script.extract_main_script"
  layers           = [aws_lambda_layer_version.extract_python_layer.arn]
  timeout          = 900
  environment {
    variables = {
      SECRET_NAME = "ketts-lough-secrets"
      S3_BUCKET_NAME = var.ingestion_bucket
    }
  }
}

