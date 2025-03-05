variable "extract_lambda_name" {
  type    = string
  default = "extractlambda"
}

variable "transform_lambda_name" {
  type    = string
  default = "transformlambda"
}

variable "load_lambda_name" {
  type    = string
  default = "loadlambda"
}

variable "ingestion_bucket" {
  type    = string
  default = "ingestion-bucket-ketts-lough-1"
}

variable "state_machine_name" {
  type    = string
  default = "ketts-lough-state-machine"
}

variable "scheduler_name" {
  type    = string
  default = "ketts-lough-scheduler"
}

variable "processed_bucket" {
  type    = string
  default = "processed-bucket-ketts-lough-1"
}

