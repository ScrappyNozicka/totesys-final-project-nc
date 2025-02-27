resource "aws_sns_topic" "terraform_alerts" {
  name = "my-terraform-alerts"
}
resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.terraform_alerts.arn
  protocol  = "email"
  endpoint  = "kettslough@gmail.com"
}