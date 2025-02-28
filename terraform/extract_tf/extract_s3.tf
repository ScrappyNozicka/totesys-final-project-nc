resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "ingestion-bucket-ketts-lough"

  tags = {
    Name        = "Our bucket"
    Environment = "Dev"
  }
}

# resource "aws_s3_bucket_versioning" "ingestion_bucket_versioning" {
#     bucket = aws_s3_bucket.ingestion_bucket.id 

#     versioning_configuration {
#         status = "Enabled"
#     }
# }