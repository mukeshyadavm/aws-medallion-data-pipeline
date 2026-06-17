locals {
  bronze_path       = "s3://${aws_s3_bucket.data_lake.bucket}/Bronze/yellow_tripdata/"
  silver_path       = "s3://${aws_s3_bucket.data_lake.bucket}/Silver/yellow_tripdata/"
  gold_path         = "s3://${aws_s3_bucket.data_lake.bucket}/Gold/"
  star_schema_path  = "s3://${aws_s3_bucket.data_lake.bucket}/StarSchema"
  glue_temp_path    = "s3://${aws_s3_bucket.data_lake.bucket}/temp/glue/"
  athena_results    = "s3://${aws_s3_bucket.data_lake.bucket}/athena-results/"
}

resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name

  tags = {
    Project = var.project_name
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_glue_catalog_database" "nyc_taxi" {
  name = var.glue_database_name
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_service_role" {
  name = "Glue-Access-S3"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

data "aws_iam_policy_document" "glue_permissions" {
  statement {
    sid = "S3DataLakeAccess"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.data_lake.arn,
      "${aws_s3_bucket.data_lake.arn}/*"
    ]
  }

  statement {
    sid = "GlueCatalogAccess"
    actions = [
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:BatchGetPartition",
      "glue:CreateDatabase",
      "glue:CreatePartition",
      "glue:CreateTable",
      "glue:DeletePartition",
      "glue:DeleteTable",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:GetTable",
      "glue:GetTables",
      "glue:UpdatePartition",
      "glue:UpdateTable"
    ]
    resources = ["*"]
  }

  statement {
    sid = "CloudWatchLogsAccess"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "glue_permissions" {
  name   = "${var.project_name}-glue-policy"
  policy = data.aws_iam_policy_document.glue_permissions.json
}

resource "aws_iam_role_policy_attachment" "glue_permissions" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_permissions.arn
}

resource "aws_glue_job" "bronze_to_silver" {
  name              = "${var.project_name}-bronze-to-silver"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${var.bronze_to_silver_script_key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--BRONZE_INPUT_PATH"                = local.bronze_path
    "--SILVER_OUTPUT_PATH"               = local.silver_path
    "--TempDir"                          = local.glue_temp_path
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  depends_on = [aws_iam_role_policy_attachment.glue_permissions]
}

resource "aws_glue_job" "silver_to_gold_aggregates" {
  name              = "${var.project_name}-silver-to-gold-aggregates"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${var.silver_to_gold_script_key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--SILVER_INPUT_PATH"                = local.silver_path
    "--GOLD_OUTPUT_PATH"                 = local.gold_path
    "--TempDir"                          = local.glue_temp_path
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  depends_on = [aws_iam_role_policy_attachment.glue_permissions]
}

resource "aws_glue_job" "star_schema" {
  name              = "${var.project_name}-star-schema"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${var.star_schema_script_key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--DATABASE_NAME"                    = var.glue_database_name
    "--SILVER_TABLE_NAME"                = var.silver_catalog_table_name
    "--OUTPUT_BASE"                      = local.star_schema_path
    "--LOCATION_LOOKUP_PATH"             = var.location_lookup_path
    "--TempDir"                          = local.glue_temp_path
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
  }

  depends_on = [
    aws_glue_catalog_database.nyc_taxi,
    aws_iam_role_policy_attachment.glue_permissions
  ]
}

resource "aws_athena_workgroup" "nyc_taxi" {
  name = "${var.project_name}-athena"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = local.athena_results

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
