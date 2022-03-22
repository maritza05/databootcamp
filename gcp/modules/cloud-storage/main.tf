// Raw layer bucket
resource "google_storage_bucket" "raw_layer_bucket" {
  name          = var.raw_layer_bucket
  location      = var.region
  force_destroy = true
  project       = var.project_id
}

// Upload files to raw bucket
resource "google_storage_bucket_object" "movie_reviews_csv" {
  name   = "movie_reviews.csv"
  source = "modules/cloud-storage/files/movie_reviews.csv"
  bucket = google_storage_bucket.raw_layer_bucket.name
}

resource "google_storage_bucket_object" "log_reviews_csv" {
  name   = "log_reviews.csv"
  source = "modules/cloud-storage/files/log_reviews.csv"
  bucket = google_storage_bucket.raw_layer_bucket.name
}

resource "google_storage_bucket_object" "user_purchase_csv" {
  name   = "user_purchase.csv"
  source = "modules/cloud-storage/files/user_purchase.csv"
  bucket = google_storage_bucket.raw_layer_bucket.name
}

// Staging layer bucket
resource "google_storage_bucket" "staging_layer_bucket" {
  name          = var.staging_layer_bucket
  location      = var.region
  force_destroy = true
  project       = var.project_id
}

// Dataproc scripts
resource "google_storage_bucket" "dataproc_scripts_bucket" {
  name          = var.dataproc_scripts
  location      = var.region
  force_destroy = true
  project       = var.project_id
}

resource "google_storage_bucket_object" "movies_processing_script" {
  name   = var.movies_reviews_script
  source = "modules/cloud-storage/files/movies_processing.py"
  bucket = google_storage_bucket.dataproc_scripts_bucket.name
}

resource "google_storage_bucket_object" "logs_processing_script" {
  name   = var.logs_reviews_script
  source = "modules/cloud-storage/files/logs_processing.py"
  bucket = google_storage_bucket.dataproc_scripts_bucket.name
}

// Temp bucket
resource "google_storage_bucket" "dataproc_temp_bucket" {
  name          = var.dataproc_temp
  location      = var.region
  force_destroy = true
  project       = var.project_id
}
