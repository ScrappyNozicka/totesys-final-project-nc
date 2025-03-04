resource "aws_s3_bucket" "processed_bucket" {
  bucket = var.processed_bucket

  tags = {
    Name        = "Processed bucket"
    Environment = "Dev"
  }

  force_destroy = true
}
