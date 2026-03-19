variable "credentials" {
  description = "Terraform runner service account keys"
  default     = "../dev/gcp-capstone-project-keys.json"
}

variable "project" {
  description = "DE Course project ID"
  default     = "project-3d40ca58-5728-4a7f-899"
}

variable "region" {
  description = "GCP region"
  default     = "europe-central2"
}

variable "location" {
  description = "GCP location in the region"
  default     = "EU"
}

variable "gcs_bucket_name" {
  description = "DE capstone project storage name"
  default     = "de_citibike_bucket"
}

variable "gcs_class" {
  description = "GC storage class"
  default     = "STANDARD"
}

variable "bq_dataset_name" {
  description = "DE bigquery dataset name"
  default     = "de_citibike_dataset"
}
