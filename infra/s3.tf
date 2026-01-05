resource "aws_s3_bucket" "lake" {
  bucket        = "${var.project}-bucket"
  force_destroy = true
}
