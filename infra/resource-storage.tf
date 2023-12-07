# Data Lake Bucket
resource "random_id" "data_lake_bucket_prefix" {
  byte_length = 8
}

resource "google_storage_bucket" "data_lake_bucket" {
  name                        = "${random_id.data_lake_bucket_prefix.hex}-data-lake-bucket"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
}

# DataProc Buckets
resource "random_id" "dataproc_temp_bucket_prefix" {
  byte_length = 8
}

resource "google_storage_bucket" "dataproc_temp_bucket" {
  name                        = "${random_id.dataproc_temp_bucket_prefix.hex}-dataproc-temp-bucket"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
}

resource "random_id" "dataproc_stage_bucket_prefix" {
  byte_length = 8
}

resource "google_storage_bucket" "dataproc_stage_bucket" {
  name                        = "${random_id.dataproc_stage_bucket_prefix.hex}-dataproc-stage-bucket"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
}

