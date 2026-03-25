variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "test-data-eng-course"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "EU"
}

variable "bq_dataset" {
  description = "BigQuery Dataset name"
  type        = string
  default     = "eurostat_data"
}
