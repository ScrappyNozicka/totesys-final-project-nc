# test "module_plan" {
#     description = "Test the Terraform plan"

#     run "init" {
#         command = "terraform init"
#     }

#     run "plan" {
#         command = "terraform plan"
#         expect_stdout = "No changes. Infrastructure is up-to-date"
#     }
# }

# mock_provider "aws" {
#   mock_resource "aws_lambda_function" {
#     defaults = {
#       function_name = "mock-lambda"
#       runtime       = "python3.12"
#       handler       = "index.handler"
#     }
#   }
# }

# test {
#     check "mock_lambda_name" {
#         condition     = aws_extract_lambda.test_lambda.function_name == "mock-lambda"
#         error_message = "Lambda function name mismatch!"
#     }
# }