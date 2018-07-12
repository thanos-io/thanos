# Bucket for thanos sidecars to ship metrics.
variable "store_bucket_name" {}

# Location of the bucket holding metrics.
variable "location" {
  default = "EU"
}

# Region to use with provider.
variable "region" {
  default = "europe-west1"
}

# Zone to run GKE cluster running thanos.
variable "thanos_zone" {
  default = "europe-west1-b"
}

# Gcloud project to use.
variable "project" {}

# Storage of terraform state.
terraform {
  backend "gcs" {
    bucket  = "terraform-state"
    path    = "thanos-loadtest/terraform.tfstate"
    project = "alert-diode-174314"
  }
}

# Provider options.
provider "google" {
  project = "${var.project}"
  region  = "europe-west1"
  version = "~> 1.2"
}

# Service account kubernetes nodes will run as.
data "google_compute_default_service_account" "default" {}

# Bucket to hold metrics.
resource "google_storage_bucket" "bucket" {
  name          = "${var.store_bucket_name}"
  location      = "${var.location}"
  force_destroy = true
}

# Ensure service account has permissions for the storage bucket.
resource "google_storage_bucket_iam_member" "default" {
  bucket      = "${var.store_bucket_name}"
  role        = "roles/storage.admin"
  member      = "serviceAccount:${data.google_compute_default_service_account.default.id}"
}

# GKE cluster to run thanos.
resource "google_container_cluster" "cluster" {
  name               = "thanos-loadtest"
  zone               = "${var.thanos_zone}"
  initial_node_count = 1
  min_master_version = "1.9.7-gke.3"

  node_config {
    oauth_scopes = [
      "https://www.googleapis.com/auth/devstorage.read_write",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
    ]
    machine_type = "n1-standard-16"
  }
}
