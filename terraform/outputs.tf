output "gcs_bucket_name" {
  description = "Name of the raw data lake GCS bucket"
  value       = google_storage_bucket.lending_club_raw.name
}

output "bq_raw_dataset" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "bq_staging_dataset" {
  value = google_bigquery_dataset.staging.dataset_id
}

output "bq_core_dataset" {
  value = google_bigquery_dataset.core.dataset_id
}

output "bq_mart_dataset" {
  value = google_bigquery_dataset.mart.dataset_id
}
