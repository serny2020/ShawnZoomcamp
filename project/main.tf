terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.27.0"
    }
  }
}

provider "google" {
  credentials = file("zoomcamp-hw4-shawn-b0edc39ea1d4.json")
#   project     = "zoomcamp-proj-shawn"
  project     = "zoomcamp-hw4-shawn"
  region      = "us-central1"
}

resource "google_storage_bucket" "project-bucket" {
  name          = "zoomcamp-proj-shawn-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 5
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}