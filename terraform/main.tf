terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "load" {
  dataset_id = "load"
  location   = var.region
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = "staging"
  location   = var.region
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics"
  location   = var.region
}

# --- Staging tables ---

resource "google_bigquery_table" "staging_marriage_rate" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "marriage_rate"
  deletion_protection = false

  schema = jsonencode([
    { name = "country",       type = "STRING",  mode = "NULLABLE" },
    { name = "year",          type = "INTEGER", mode = "NULLABLE" },
    { name = "marriage_rate", type = "FLOAT",   mode = "NULLABLE" },
  ])

  lifecycle { ignore_changes = [schema] }
}

resource "google_bigquery_table" "staging_divorce_rate" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "divorce_rate"
  deletion_protection = false

  schema = jsonencode([
    { name = "country",      type = "STRING",  mode = "NULLABLE" },
    { name = "year",         type = "INTEGER", mode = "NULLABLE" },
    { name = "divorce_rate", type = "FLOAT",   mode = "NULLABLE" },
  ])

  lifecycle { ignore_changes = [schema] }
}

resource "google_bigquery_table" "staging_age_at_marriage" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "age_at_marriage"
  deletion_protection = false

  schema = jsonencode([
    { name = "country",           type = "STRING",  mode = "NULLABLE" },
    { name = "year",              type = "INTEGER", mode = "NULLABLE" },
    { name = "age_at_marriage_f", type = "FLOAT",   mode = "NULLABLE" },
    { name = "age_at_marriage_m", type = "FLOAT",   mode = "NULLABLE" },
  ])

  lifecycle { ignore_changes = [schema] }
}

resource "google_bigquery_table" "staging_income_quintile" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "income_quintile"
  deletion_protection = false

  schema = jsonencode([
    { name = "country",           type = "STRING",  mode = "NULLABLE" },
    { name = "year",              type = "INTEGER", mode = "NULLABLE" },
    { name = "income_quintile_f", type = "FLOAT",   mode = "NULLABLE" },
    { name = "income_quintile_m", type = "FLOAT",   mode = "NULLABLE" },
  ])

  lifecycle { ignore_changes = [schema] }
}

# --- Analytics table ---

resource "google_bigquery_table" "analytics_relationships" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "relationships"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "year_date"
  }

  clustering = ["country"]

  schema = jsonencode([
    { name = "year_date",         type = "DATE",    mode = "NULLABLE" },
    { name = "country",           type = "STRING",  mode = "NULLABLE" },
    { name = "year",              type = "INTEGER", mode = "NULLABLE" },
    { name = "marriage_rate",     type = "FLOAT",   mode = "NULLABLE" },
    { name = "divorce_rate",      type = "FLOAT",   mode = "NULLABLE" },
    { name = "age_at_marriage_f", type = "FLOAT",   mode = "NULLABLE" },
    { name = "age_at_marriage_m", type = "FLOAT",   mode = "NULLABLE" },
    { name = "income_quintile_f", type = "FLOAT",   mode = "NULLABLE" },
    { name = "income_quintile_m", type = "FLOAT",   mode = "NULLABLE" },
  ])

  lifecycle { ignore_changes = [schema] }
}
