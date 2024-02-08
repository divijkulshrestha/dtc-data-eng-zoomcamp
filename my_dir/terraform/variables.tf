variable "project" {
  description = "Project ID"
  default     = "divij-zoomcamp-2024"
}

variable "region" {
  description = "Region"
  default     = "asia-south1-c"
}

variable "location" {
  description = "Project Location"
  default     = "ASIA-SOUTH1"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset - Zoomcamp Practice"
  #Update the below to what you want your dataset to be called
  default     = "nyc_taxi_dataset"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket - Zoomcamp Practice"
  #Update the below to a unique bucket name
  default     = "divij-zoomcamp-2024-gcs-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}