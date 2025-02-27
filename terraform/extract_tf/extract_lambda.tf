resource "aws_lambda_function" "extract_lambda" {
   filename = data.archive_file.lambda_package.output_path
   source_code_hash = data.archive_file.lambda_package.output_base64sha256
   function_name = var.lambda_name
   runtime = "python3.13"
   role = aws_iam_role.extract_iam.arn     
   handler = "extract_main_script.extract_main_script" 
   layers = [aws_lambda_layer_version.extract_python_layer.arn]
   timeout = 120
   environment {
    variables = {
        DB_USER="project_team_8"
        DB_PASSWORD="p7fkmmUYIEK0sMf"
        DB_HOST="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
        DB_NAME="totesys"
        DB_PORT="5432"
        S3_BUCKET_NAME="ingestionbucketkettslough"
    }
   }
}

