resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "ingestion-bucket-ketts-lough"

  tags = {
    Name        = "Our bucket"
    Environment = "Dev"
  }

  force_destroy = true
}
