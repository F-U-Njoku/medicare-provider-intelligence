terraform {
  required_version = ">= 1.0"
  backend "local" {} # You can change this to 'gcs' later for remote state
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. BRONZE LAYER: The Data Lake (GCS)
resource "google_storage_bucket" "bronze_bucket" {
  name          = var.bucket_name
  location      = var.region
  storage_class = var.storage_class
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 30 # Auto-delete/archive old raw data if needed
    }
    action {
      type = "Delete"
    }
  }
}

# 2. SILVER LAYER: Staging Dataset (BigQuery)
# This is where dbt will clean the raw data
resource "google_bigquery_dataset" "silver_medicare_stg" {
  dataset_id = "medicare_silver_stg"
  project    = var.project_id
  location   = var.region
}

# 3. GOLD LAYER: Production/Marts Dataset (BigQuery)
# This is what Looker Studio will connect to
resource "google_bigquery_dataset" "gold_medicare_marts" {
  dataset_id = "medicare_gold_marts"
  project    = var.project_id
  location   = var.region
}