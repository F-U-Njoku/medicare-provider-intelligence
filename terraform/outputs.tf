output "bronze_bucket_url" {
  value = google_storage_bucket.bronze_bucket.url
}

output "bronze_dataset_id" {
  value = google_bigquery_dataset.bronze_medicare.dataset_id
}

output "silver_dataset_id" {
  value = google_bigquery_dataset.silver_medicare.dataset_id
}

output "gold_dataset_id" {
  value = google_bigquery_dataset.gold_medicare.dataset_id
}