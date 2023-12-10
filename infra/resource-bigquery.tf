resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id                 = "raw_dataset"
  delete_contents_on_destroy = true
}


resource "google_bigquery_dataset" "prod_dataset" {
  dataset_id                 = "prod_dataset"
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "raw_data" {
  dataset_id          = google_bigquery_dataset.raw_dataset.dataset_id
  table_id            = "raw_data"
  deletion_protection = false
}

resource "google_bigquery_table" "raw_data_cleaned" {
  dataset_id          = google_bigquery_dataset.raw_dataset.dataset_id
  table_id            = "raw_data_cleaned"
  deletion_protection = false
}


resource "google_bigquery_table" "hospital_subtype_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "hospital_subtype_table"
  deletion_protection = false
}


resource "google_bigquery_table" "covid_tred_month_year_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "covid_tred_month_year_table"
  deletion_protection = false
}


resource "google_bigquery_table" "metro_micro_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "metro_micro_table"
  deletion_protection = false
}


resource "google_bigquery_table" "icu_bed_utilization_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "icu_bed_utilization_table"
  deletion_protection = false
}

resource "google_bigquery_table" "yearly_monthly_vaccination_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "yearly_monthly_vaccination_table"
  deletion_protection = false
}

resource "google_bigquery_table" "util_rates_adult_pediatric_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "util_rates_adult_pediatric_table"
  deletion_protection = false
}

resource "google_bigquery_table" "vaccination_doses_timeline_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "vaccination_doses_timeline_table"
  deletion_protection = false
}

resource "google_bigquery_table" "pediatric_admissions_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "pediatric_admissions_table"
  deletion_protection = false
}

resource "google_bigquery_table" "influenza_covid_comparison_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "influenza_covid_comparison_table"
  deletion_protection = false
}

resource "google_bigquery_table" "hospital_reporting_countrywide_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "hospital_reporting_countrywide_table"
  deletion_protection = false
}

resource "google_bigquery_table" "hospital_reporting_statewide_table" {
  dataset_id          = google_bigquery_dataset.prod_dataset.dataset_id
  table_id            = "hospital_reporting_statewide_table"
  deletion_protection = false
}