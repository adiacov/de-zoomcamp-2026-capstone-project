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
  name     = local.gcs_bucket_name
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

  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_dataset" "de_citibike_raw_dataset" {
  dataset_id = local.citibike_raw_dataset_name
  project    = var.project
  location   = var.location

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_dataset" "de_citibike_staging_dataset" {
  dataset_id = local.citibike_staging_dataset_name
  project    = var.project
  location   = var.location

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_dataset" "de_citibike_marts_dataset" {
  dataset_id = local.citibike_marts_dataset_name
  project    = var.project
  location   = var.location

  lifecycle {
    prevent_destroy = true
  }
}
