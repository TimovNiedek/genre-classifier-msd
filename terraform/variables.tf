variable "dev_ssh_public_key" {
  description = "Public SSH key to use for the Prefect instance"
  type        = string
}

variable "aws_access_key_id" {
  description = "AWS access key ID for authenticating from the prefect instance"
  type        = string
}

variable "aws_access_key_secret" {
  description = "AWS access key secret for authenticating from the prefect instance"
  type        = string
}
