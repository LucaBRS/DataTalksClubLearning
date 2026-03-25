output "bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.data_lake.name
}

output "bigquery_dataset" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.dataset.dataset_id
}
