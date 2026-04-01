output "bronze_bucket_url" {
  value = google_storage_bucket.bronze_bucket.url
}

output "silver_dataset_id" {
  value = google_bigquery_dataset.silver_medicare_stg.dataset_id
}

output "gold_dataset_id" {
  value = google_bigquery_dataset.gold_medicare_marts.dataset_id
}