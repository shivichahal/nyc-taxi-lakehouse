resource "aws_glue_job" "etl" {
  name     = "${var.project}-etl"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://nyc-taxi-lakehouse-shivani-bucket/src/main_glue_job.py"
  }

  glue_version = "4.0"

  default_arguments = {
    "--datalake-formats" = "delta"
    "--enable-continuous-cloudwatch-log" = "true"
    "--extra-py-files" = "s3://nyc-taxi-lakehouse-shivani-bucket/src/dependencies.zip"
  }

  number_of_workers = 5
  worker_type       = "G.1X"
}
