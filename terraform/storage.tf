resource "aws_s3_bucket" "million_songs_dataset" {
  bucket = "million-songs-dataset"

  force_destroy = true
}

resource "aws_s3_bucket" "mlflow-artifacts-tvn" {
  bucket = "mlflow-artifacts-tvn"

  force_destroy = true
}
