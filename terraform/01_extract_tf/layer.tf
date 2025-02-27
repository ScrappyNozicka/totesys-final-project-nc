resource "aws_lambda_layer_version" "shared_layer" {
  layer_name          = "SharedLambdaLayer"
  compatible_runtimes = ["python3.12"]
  filename   = "shared_layer.zip"
  description = "shared dependencies for different Lambda functions"
}