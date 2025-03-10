resource "aws_lambda_function" "transform_lambda" {
  filename         = data.archive_file.transform_lambda_package.output_path
  source_code_hash = data.archive_file.transform_lambda_package.output_base64sha256
  function_name    = var.transform_lambda_name
  runtime          = var.python_runtime
  role             = aws_iam_role.transform_iam.arn
  handler          = "transform_main_script.transform_main_script"
  layers = [aws_lambda_layer_version.extract_python_layer.arn,
            aws_lambda_layer_version.fastparquet_python_layer.arn,
            aws_lambda_layer_version.pandas_python_layer.arn]
  timeout     = 900
  memory_size = 448

  environment {
    variables = {
      S3_BUCKET_NAME           = var.ingestion_bucket
      PROCESSED_S3_BUCKET_NAME = var.processed_bucket
    }
  }
}


resource "aws_cloudwatch_log_group" "transform_log_group" {
  name = "/aws/lambda/${var.transform_lambda_name}"
}
