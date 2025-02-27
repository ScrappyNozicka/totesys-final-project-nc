terraform {
  backend "s3" {
    bucket         = "kettslough-tfstatebucket"      
    key            = "terraform/01_extract_tf/terraform.tfstate"  
    region         = "eu-west-2"                       
    encrypt        = true                             
    dynamodb_table = "terraform-lock-for-tfstate"                 
    acl            = "private"                         
}
}
