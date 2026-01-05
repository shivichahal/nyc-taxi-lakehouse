output "s3_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "glue_job_name" {
  value = aws_glue_job.etl.name
}
