terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.61.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "3.1.0"
    }
  }

  # Remove this backend if you want to 
  # persist the terraform state locally
  backend "gcs" {
    bucket = "8969bc6a9192e160-tfstate-bucket"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

provider "tls" {}

locals {
  prefect_dir = "../prefect"
  pyspark_dir = "../pyspark"
  schemas_dir = "../schemas"
  scripts_dir = "../scripts"
  ssh_dir     = "../ssh"
}
