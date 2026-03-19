terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "de_citibike_bucket" {
  name     = var.gcs_bucket_name
  location = var.location

  storage_class               = var.gcs_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90 // days
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "de_citibike_dataset" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.location
}
