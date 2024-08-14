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
  policy = data.aws_iam_policy_document.allow_public_access.json
}

data "aws_iam_policy_document" "allow_public_access" {
  statement {
    sid    = "PublicReadGetObject"
    effect = "Allow"
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::evidently-static-dashboard-tvn/*"]
  }
}
