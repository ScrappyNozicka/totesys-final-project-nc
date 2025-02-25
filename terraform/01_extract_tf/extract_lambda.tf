
data "archive_file" "lambda_package" {
  type        = "zip"
  source_dir  = "${path.module}/lambda_code"  # Folder where Lambda code is stored
  output_path = "${path.module}/lambda_function.zip"
}


resource "aws_lambda_function" "extract_lambda" {
    filename = data.archive_file.lambda_package.output_path
    source_code_hash = data.archive_file.lambda_package.output_base64sha256
    function_name = var.lambda_name
    runtime = "python3.13"
    role = aws_iam_role.extract_iam.arn      
    handler = "extract_lambda_handler.lambda_handler" 


    
    layers = [
        aws_lambda_layer_version.lambda_layer.arn
    ]
    depends_on = [
        aws_lambda_layer_version.lambda_layer
    ]
}

