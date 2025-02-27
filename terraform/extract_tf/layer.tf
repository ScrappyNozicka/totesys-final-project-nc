data "archive_file" "lambda_package" {
 type        = "zip"
 source_file  = "${path.module}/extract_lambda.tf"
 output_path = "${path.module}/extract_lambda.zip"
}


resource "aws_lambda_layer_version" "extract_layer" {
    filename = data.archive_file.lambda_package.output_path
    layer_name = "extract-layer"
    compatible_runtimes=["python3.13"]
    
}

