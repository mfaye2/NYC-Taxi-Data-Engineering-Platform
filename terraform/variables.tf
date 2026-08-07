variable "aws_region" {
  description = "AWS region used by the project."
  type        = string
  default     = "eu-north-1"
}

variable "aws_profile" {
  description = "Local AWS CLI profile used by Terraform."
  type        = string
  default     = "nyc-taxi-dev"
}

variable "project_name" {
  description = "Project name used in resource names and tags."
  type        = string
  default     = "nyc-taxi-data-platform"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
}

variable "owner" {
  description = "Resource owner used in AWS tags."
  type        = string
  default     = "Mouhamadou"
}

variable "force_destroy_bucket" {
  description = "Allow Terraform to delete the bucket even if it contains objects."
  type        = bool
  default     = false
}