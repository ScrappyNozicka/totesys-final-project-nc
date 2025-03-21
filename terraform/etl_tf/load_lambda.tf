resource "aws_lambda_function" "load_lambda" {
  filename         = data.archive_file.load_lambda_package.output_path
  source_code_hash = data.archive_file.load_lambda_package.output_base64sha256
  function_name    = var.load_lambda_name
  runtime          = var.python_runtime
  role             = aws_iam_role.load_iam.arn
  handler          = "load_main_script.load_main_script"
  layers           = [aws_lambda_layer_version.extract_python_layer.arn,
                      aws_lambda_layer_version.sqlalchemy_python_layer.arn,
                      aws_lambda_layer_version.fastparquet_python_layer.arn,
                      aws_lambda_layer_version.pandas_python_layer.arn]
  timeout          = 900
  environment {
    variables = {
      PROCESSED_S3_BUCKET_NAME = var.processed_bucket
      SECRET_NAME = "ketts-lough-secrets"
    }
  }

}

resource "aws_cloudwatch_log_group" "load_log_group" {
  name = "/aws/lambda/${var.load_lambda_name}"
}