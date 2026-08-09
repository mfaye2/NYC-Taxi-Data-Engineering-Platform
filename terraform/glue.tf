resource "aws_glue_catalog_database" "nyc_taxi" {
  name = "${replace(var.project_name, "-", "_")}_${var.environment}"
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.data_lake.id

  key = "scripts/glue/transform_trips_job.py"

  source = "${path.module}/../glue_jobs/transform_trips_job.py"

  etag = filemd5(
    "${path.module}/../glue_jobs/transform_trips_job.py"
  )
}

resource "aws_glue_job" "transform_trips" {
  name = "${var.project_name}-${var.environment}-transform-trips"

  role_arn = aws_iam_role.glue_job_role.arn

  glue_version = "5.1"

  worker_type = "G.1X"

  number_of_workers = 2

  timeout = 10

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--SOURCE_TRIPS_PATH" = "s3://${aws_s3_bucket.data_lake.bucket}/raw/trips/"

    "--SOURCE_ZONES_PATH" = "s3://${aws_s3_bucket.data_lake.bucket}/raw/zones/taxi_zone_lookup.csv"

    "--CURATED_OUTPUT_PATH" = "s3://${aws_s3_bucket.data_lake.bucket}/curated/trips/"

    "--REJECTED_OUTPUT_PATH" = "s3://${aws_s3_bucket.data_lake.bucket}/rejected/trips/"

    "--enable-continuous-cloudwatch-log" = "true"

    "--START_DATE" = "2025-01-01"
    "--END_DATE"   = "2025-01-31"
  }

  depends_on = [
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy_attachment.glue_service_role,
  ]
}