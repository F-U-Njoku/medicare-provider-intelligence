variable "project_id" {
  description = "Your GCP Project ID"
  type        = string
}

variable "region" {
  description = "Region for GCP resources"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket"
  default     = "STANDARD"
}

variable "bucket_name" {
  description = "The name of the GCS bucket (must be unique)"
  type        = string
}

variable "bronze_dataset_name" {
  description = "The name of the bronze dataset"
  type        = string
}

variable "silver_dataset_name" {
  description = "The name of the silver dataset"
  type        = string
}

variable "gold_dataset_name" {
  description = "The name of the gold dataset"
  type        = string
}