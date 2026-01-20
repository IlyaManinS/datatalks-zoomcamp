variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my_creds.json"
}

variable "project" {
  description = "Project Name"
  default     = "project-6e52c6e7-52bb-421e-a6c"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "region" {
  description = "Project Region"
  default     = "us-central1"
}

variable "bq_daataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_bigquery"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "project-6e52c6e7-52bb-421e-a6c-terra-bucket"
}


variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

