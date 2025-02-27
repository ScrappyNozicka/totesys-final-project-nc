data "archive_file" "lambda_package" {
 type        = "zip"
 source_dir  = "${path.module}/lambda_code"
 output_path = "${path.module}/extract_lambda.zip"
}
