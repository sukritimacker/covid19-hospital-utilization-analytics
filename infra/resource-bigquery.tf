resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id                 = "raw_dataset"
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "raw_data" {
  dataset_id          = google_bigquery_dataset.raw_dataset.dataset_id
  table_id            = "raw_data"
  deletion_protection = false
#   schema = file("${local.schemas_dir}/raw_data.json")
}
