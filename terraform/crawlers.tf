resource "aws_glue_crawler" "curated_trips" {
  name          = "${var.project_name}-${var.environment}-curated-trips-crawler"
  database_name = aws_glue_catalog_database.nyc_taxi.name
  role          = aws_iam_role.glue_job_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/curated/trips/"
  }

  table_prefix = "curated_"

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}


resource "aws_glue_crawler" "rejected_trips" {
  name          = "${var.project_name}-${var.environment}-rejected-trips-crawler"
  database_name = aws_glue_catalog_database.nyc_taxi.name
  role          = aws_iam_role.glue_job_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/rejected/trips/"
  }

  table_prefix = "rejected_"

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}