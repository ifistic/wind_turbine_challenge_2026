# One bucket, four prefixes — mirrors the local data/{raw,bronze,silver,gold}
# folder structure from the original project. A random suffix keeps the
# bucket name globally unique without you having to pick one by hand.

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-${random_id.bucket_suffix.hex}"

  tags = var.tags
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Separate bucket for Glue job scripts (bronze.py/silver.py/gold.py), kept
# apart from the data itself so a data-access policy doesn't also need to
# grant script read/write.
resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.project_name}-glue-scripts-${random_id.bucket_suffix.hex}"

  tags = var.tags
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle: move raw/bronze data to Infrequent Access after 30 days since
# it's an audit trail you'll rarely re-read, keep silver/gold in Standard
# since those get queried more often.
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "raw-bronze-to-ia"
    status = "Enabled"

    filter {
      and {
        prefix = "raw/"
      }
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "bronze-to-ia"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}
