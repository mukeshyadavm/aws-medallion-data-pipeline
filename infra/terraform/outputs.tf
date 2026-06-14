output "bucket_name" {
  description = "Data lake S3 bucket name."
  value       = aws_s3_bucket.data_lake.bucket
}

output "glue_database_name" {
  description = "Glue Data Catalog database name."
  value       = aws_glue_catalog_database.nyc_taxi.name
}

output "glue_role_arn" {
  description = "IAM role ARN used by Glue jobs."
  value       = aws_iam_role.glue_service_role.arn
}

output "bronze_to_silver_job_name" {
  description = "Bronze to Silver Glue job name."
  value       = aws_glue_job.bronze_to_silver.name
}

output "silver_to_gold_aggregates_job_name" {
  description = "Silver to Gold aggregates Glue job name."
  value       = aws_glue_job.silver_to_gold_aggregates.name
}

output "star_schema_job_name" {
  description = "Star Schema Glue job name."
  value       = aws_glue_job.star_schema.name
}

output "athena_workgroup_name" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.nyc_taxi.name
}
