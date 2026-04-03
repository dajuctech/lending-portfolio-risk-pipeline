terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.16"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

resource "google_storage_bucket" "lending_club_raw" {
  name          = "${var.project_id}-lending-club-raw"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    action    { type = "Delete" }
    condition { age  = 90 }
  }

  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "lending_club_raw"
  location    = var.region
  description = "Raw ingested Lending Club loan data"
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "lending_club_staging"
  location    = var.region
  description = "dbt staging views"
}

resource "google_bigquery_dataset" "core" {
  dataset_id  = "lending_club_core"
  location    = var.region
  description = "dbt core fact tables"
}

resource "google_bigquery_dataset" "mart" {
  dataset_id  = "lending_club_mart"
  location    = var.region
  description = "dbt mart tables for dashboard"
}

resource "google_bigquery_table" "loans_raw" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "loans_raw"
  deletion_protection = false

  time_partitioning {
    type  = "MONTH"
    field = "issue_date"
  }

  clustering = ["grade", "loan_status"]

  schema = jsonencode([
    { name = "id",                     type = "STRING",    mode = "REQUIRED" },
    { name = "loan_amnt",              type = "FLOAT64",   mode = "NULLABLE" },
    { name = "funded_amnt",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "grade",                  type = "STRING",    mode = "NULLABLE" },
    { name = "sub_grade",              type = "STRING",    mode = "NULLABLE" },
    { name = "int_rate",               type = "FLOAT64",   mode = "NULLABLE" },
    { name = "loan_status",            type = "STRING",    mode = "NULLABLE" },
    { name = "issue_date",             type = "DATE",      mode = "NULLABLE" },
    { name = "purpose",                type = "STRING",    mode = "NULLABLE" },
    { name = "addr_state",             type = "STRING",    mode = "NULLABLE" },
    { name = "mths_since_last_delinq", type = "INT64",     mode = "NULLABLE" },
    { name = "total_pymnt",            type = "FLOAT64",   mode = "NULLABLE" },
    { name = "out_prncp",              type = "FLOAT64",   mode = "NULLABLE" },
    { name = "delinq_2yrs",            type = "INT64",     mode = "NULLABLE" },
    { name = "annual_inc",             type = "FLOAT64",   mode = "NULLABLE" },
    { name = "dti",                    type = "FLOAT64",   mode = "NULLABLE" },
    { name = "ingest_ts",              type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "row_hash",               type = "STRING",    mode = "NULLABLE" }
  ])
}
