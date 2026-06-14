# Bronze Layer

## Purpose

The Bronze layer stores raw NYC TLC Yellow Taxi Parquet files exactly as received from the source system. Files are not cleaned, deduplicated, repartitioned, or rewritten before landing.

## Source

- Dataset: NYC TLC Yellow Taxi trip records
- Format: Parquet
- Public source pattern: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`

## Target Layout

```text
s3://<bucket>/Bronze/yellow_tripdata/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet
```

## Ingestion Script

Script:

```text
src/bronze/ingest_tlc_taxi.py
```

Example:

```bash
python src/bronze/ingest_tlc_taxi.py \
  --bucket <bucket> \
  --year 2025 \
  --month 1
```

Local-file upload:

```bash
python src/bronze/ingest_tlc_taxi.py \
  --bucket <bucket> \
  --year 2025 \
  --month 1 \
  --source-file ./yellow_tripdata_2025-01.parquet
```

## Credential Handling

The script uses boto3 default credential resolution. Do not commit AWS keys. Use one of:

- AWS SSO
- AWS profile
- IAM role
- Environment variables managed outside git

## Data Contract

Bronze is append-only/raw. The only expected metadata is the S3 key layout. Data quality rules are applied in Silver, not Bronze.
