resource "aws_athena_workgroup" "analytics" {
  name = "${var.project_name}-${var.environment}-analytics"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.bucket}/athena-results/"
    }
  }

  state = "ENABLED"
}