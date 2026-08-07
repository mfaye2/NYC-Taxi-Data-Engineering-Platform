output "data_lake_bucket_name" {
  description = "Name of the S3 data-lake bucket."
  value       = aws_s3_bucket.data_lake.bucket
}

output "data_lake_bucket_arn" {
  description = "ARN of the S3 data-lake bucket."
  value       = aws_s3_bucket.data_lake.arn
}

output "aws_region" {
  description = "AWS region used by the project."
  value       = var.aws_region
}