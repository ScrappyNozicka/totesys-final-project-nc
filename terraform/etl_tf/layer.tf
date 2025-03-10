data "archive_file" "extract_lambda_package" {
 type        = "zip"
 source_dir  = "${path.module}/../../src/extract"
 output_path = "${path.module}/../extract_lambda.zip"
}

resource "aws_lambda_layer_version" "extract_python_layer" {
    filename = "${path.module}/../extract_layer.zip"
    layer_name = "extract_python_layer"
    compatible_runtimes=[var.python_runtime]
    
}


data "archive_file" "transform_lambda_package" {
  type        = "zip"
  source_dir  = "${path.module}/../../src/transform"
  output_path = "${path.module}/../transform_lambda.zip"
}

data "archive_file" "load_lambda_package" {
  type        = "zip"
  source_dir  = "${path.module}/../../src/load"
  output_path = "${path.module}/../load_lambda.zip"
}

resource "aws_lambda_layer_version" "pandas_python_layer" {
    filename = "${path.module}/../pandas_layer.zip"
    layer_name = "pandas_python_layer"
    compatible_runtimes=[var.python_runtime]
    
}

resource "aws_lambda_layer_version" "fastparquet_python_layer" {
    filename = "${path.module}/../fastparquet_layer.zip"
    layer_name = "fastparquet_python_layer"
    compatible_runtimes=[var.python_runtime]
    
}

resource "aws_lambda_layer_version" "sqlalchemy_python_layer" {
    filename = "${path.module}/../sqlalchemy_layer.zip"
    layer_name = "sqlalchemy_python_layer"
    compatible_runtimes=[var.python_runtime]
    
}
