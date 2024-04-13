provider "google" {
    project = var.project
    credentials = var.credentials_file
    region = var.region
    zone = var.zone
}


resource "google_storage_bucket" "client_data" {
  name     = "data-from-client-123123"  # TO CHANGE
  location = var.location
  storage_class = var.storage_class
}


resource "google_storage_bucket" "processed_data" {
  name     = "processed-data-123123"  # TO CHANGE
  location = var.location
  storage_class = var.storage_class
}