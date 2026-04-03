variable "project_id" {
  description = "Your GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west2"
}

variable "credentials_file" {
  description = "Path to GCP service account JSON key"
  type        = string
  default     = "../credentials/gcp-service-account.json"
}
