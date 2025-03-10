resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = var.ingestion_bucket

  tags = {
    Name        = "Our bucket"
    Environment = "Dev"
  }

  force_destroy = true
}
