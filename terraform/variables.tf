locals {
  data_lake_bucket = "data_lake"
}

variable "project" {
  description = "The project ID of the project"
  type        = string
}

variable "region" {
  description = "The region of the project"
  type        = string
  default     = "europe-west1"

}

variable "storage_class" {
  description = "The storage class of the bucket"
  default     = "STANDARD"
  type        = string
}

variable "bigquery_dataset" {
  description = "bigquery dataset name"
  type        = string
  default     = "my_dataset"
}

variable "credentials" {
  description = "The path to the credential file"
  type        = string
  default     = "~/.google/credentials/google_credentials.json"
}
