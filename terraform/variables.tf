variable "credentials" {
  description = "Terraform runner service account keys"
  default     = "../dev/credentials.json"
}

variable "project" {
  description = "DE Course project ID"
  default     = "project-3d40ca58-5728-4a7f-899" # <- replace with your GCP project ID
}

variable "prefix" {
  description = "Unique prefix for all GCP resource names. Use your GCP project ID or a short identifier to avoid naming collisions."
  default     = "de" # <- replace with a unique value (e.g. your GCP project ID)
}

variable "region" {
  description = "GCP region"
  default     = "europe-central2"
}

variable "location" {
  description = "GCP location in the region"
  default     = "EU"
}

variable "gcs_class" {
  description = "GC storage class"
  default     = "STANDARD"
}

locals {
  # GCS bucket
  gcs_bucket_name = "${var.prefix}_citibike_bucket"

  # BigQuery datasets
  citibike_raw_dataset_name     = "${replace(var.prefix, "-", "_")}_citibike_raw"
  citibike_staging_dataset_name = "${replace(var.prefix, "-", "_")}_citibike_staging"
  citibike_marts_dataset_name   = "${replace(var.prefix, "-", "_")}_citibike_marts"
}
