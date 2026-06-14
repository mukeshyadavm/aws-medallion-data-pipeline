# Terraform Infrastructure

This Terraform configuration creates the AWS resources required by the NYC Taxi data engineering pipeline:

- S3 data lake bucket
- Glue Data Catalog database
- IAM role and policy for Glue jobs
- Bronze to Silver Glue job
- Silver to Gold aggregate Glue job
- Star Schema Glue job
- Athena workgroup

## Usage

Upload Glue scripts before running the jobs:

```bash
aws s3 cp glue_jobs/bronze_to_silver_glue.py s3://<bucket>/scripts/bronze_to_silver_glue.py
aws s3 cp glue_jobs/silver_to_gold_aggregates_glue.py s3://<bucket>/scripts/silver_to_gold_aggregates_glue.py
aws s3 cp glue_jobs/nyc_taxi_star_schema_glue.py s3://<bucket>/scripts/nyc_taxi_star_schema_glue.py
```

Deploy:

```bash
cd infra/terraform
terraform init
terraform plan -var='bucket_name=<globally-unique-bucket-name>'
terraform apply -var='bucket_name=<globally-unique-bucket-name>'
```

The Terraform state file must not be committed.
