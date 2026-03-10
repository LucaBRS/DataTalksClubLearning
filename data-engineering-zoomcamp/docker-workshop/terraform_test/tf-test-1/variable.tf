variable "project" {
  default = "test-data-eng-course"
}
variable "region" {
  default = "europe-west8"

}

variable "location" {
  description = "project locatin"
  default     = "EU"

}

variable "bq_dataset_name" {
  description = "bq dataset name"
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  default = "STANDARD"
}

variable "gcs_bucket_name" {
  default     = "test-data-eng-course-terra-bucket"
  description = "storage buvket name"
}
