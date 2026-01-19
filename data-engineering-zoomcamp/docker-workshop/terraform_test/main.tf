terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  # config option
  credentials =
  project = "test-data-eng-course"
  region  = "europe-west8"
}