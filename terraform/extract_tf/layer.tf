data "archive_file" "extract_lambda_package" {
 type        = "zip"
 source_dir  = "${path.module}/../../src/extract"
 output_path = "${path.module}/../extract_lambda.zip"
}


data "archive_file" "layer" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "${path.module}/layers/extract"
  output_path = "${path.module}/../extract_layer.zip"
  
}

resource "aws_lambda_layer_version" "extract_python_layer" {
    filename = data.archive_file.layer.output_path
    layer_name = "extract_python_layer"
    compatible_runtimes=["python3.13"]
    
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



data "archive_file" "layer_pandas" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "${path.module}/layers/pandas"
  output_path = "${path.module}/../pandas_layer.zip"

}

resource "aws_lambda_layer_version" "pandas_python_layer" {
    filename = data.archive_file.layer_pandas.output_path
    layer_name = "pandas_python_layer"
    compatible_runtimes=["python3.12"]
    
}


data "archive_file" "layer_fastparquet" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "${path.module}/layers/fastparquet"
  output_path = "${path.module}/../fastparquet_layer.zip"

}

resource "aws_lambda_layer_version" "fastparquet_python_layer" {
    filename = data.archive_file.layer_fastparquet.output_path
    layer_name = "fastparquet_python_layer"
    compatible_runtimes=["python3.12"]
    
}