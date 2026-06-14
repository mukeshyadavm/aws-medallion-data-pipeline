variable "aws_region" {
  description = "AWS region for the data platform resources."
  type        = string
  default     = "us-east-2"
}

variable "project_name" {
  description = "Prefix used for named AWS resources."
  type        = string
  default     = "nyc-taxi-de"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for Bronze, Silver, Gold, scripts, and Athena results."
  type        = string
}

variable "glue_database_name" {
  description = "Glue Data Catalog database name."
  type        = string
  default     = "nyc_taxi_db"
}

variable "glue_version" {
  description = "AWS Glue runtime version."
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers per job."
  type        = number
  default     = 4
}

variable "bronze_to_silver_script_key" {
  description = "S3 key for the uploaded Bronze to Silver Glue script."
  type        = string
  default     = "scripts/bronze_to_silver_glue.py"
}

variable "silver_to_gold_script_key" {
  description = "S3 key for the uploaded Silver to Gold aggregate Glue script."
  type        = string
  default     = "scripts/silver_to_gold_aggregates_glue.py"
}

variable "star_schema_script_key" {
  description = "S3 key for the uploaded Star Schema Glue script."
  type        = string
  default     = "scripts/nyc_taxi_star_schema_glue.py"
}

variable "silver_catalog_table_name" {
  description = "Glue Catalog table name used by the Star Schema Glue job."
  type        = string
  default     = "silver_silver"
}

variable "location_lookup_path" {
  description = "Optional S3 path to TLC taxi zone lookup CSV."
  type        = string
  default     = ""
}
