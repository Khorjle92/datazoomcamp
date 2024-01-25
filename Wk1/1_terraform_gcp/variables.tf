variable "credentials" {
    description = "My Service Acc Cred"
    default ="./keys/my-creds.json"
  
}

variable "region" {
  description = "Region"
  default     = "us-central1"

}

variable "project" {
  description = "Project Name"
  default     = "second-core-412212"

}

variable "location" {
  description = "Project Location"
  default     = "US"

}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}


variable "gcs_bucket_name" {
  description = "Bucket Storage Name"
  default     = "demo-khorj-terra-bucket"
}
