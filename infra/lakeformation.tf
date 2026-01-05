resource "aws_glue_catalog_database" "db" {
  name = "nyc_taxi_lake"
}

resource "aws_lakeformation_resource" "lake" {
  arn       = "arn:aws:s3:::nyc-taxi-lakehouse-bucket"
  role_arn = aws_iam_role.glue_role.arn
}
