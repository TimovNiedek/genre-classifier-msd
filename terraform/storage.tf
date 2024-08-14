resource "aws_s3_bucket" "million_songs_dataset" {
  bucket = "million-songs-dataset"

  force_destroy = true
}

resource "aws_s3_bucket" "million_songs_dataset_cicd" {
  bucket = "million-songs-dataset-cicd"

  force_destroy = true
}


resource "aws_s3_bucket" "mlflow-artifacts-tvn" {
  bucket = "mlflow-artifacts-tvn"

  force_destroy = true
}

resource "aws_s3_bucket" "evidently_static_dashboard_tvn" {
  bucket = "evidently-static-dashboard-tvn"

  force_destroy = true

}

resource "aws_s3_bucket_website_configuration" "evidently_dashboard_config" {
  bucket = aws_s3_bucket.evidently_static_dashboard_tvn.id
  index_document {
    suffix = "index.html"
  }
}


resource "aws_s3_bucket_public_access_block" "public_access_block" {
  bucket                  = aws_s3_bucket.evidently_static_dashboard_tvn.id
  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_policy" "allow_public_access_evidently" {
  bucket = aws_s3_bucket.evidently_static_dashboard_tvn.id
  policy = data.template_file.public_access_policy.rendered
}

data "template_file" "public_access_policy" {
  template = file("${path.module}/templates/public_access_policy.json")
  vars = {
    bucket_name = aws_s3_bucket.evidently_static_dashboard_tvn.id
  }
}
