resource "google_service_account" "service_account" {
  account_id   = var.service_account_name
  display_name = var.service_account_name
  project      = var.project_id
}

resource "google_project_iam_binding" "service_account_iam" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

resource "google_project_iam_binding" "dataproc_admin_iam" {
  project = var.project_id
  role    = "roles/dataproc.admin"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

resource "google_project_iam_binding" "gcs_creator_iam" {
  project = var.project_id
  role    = "roles/storage.objectCreator"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

resource "google_project_iam_binding" "gcs_viewer_iam" {
  project = var.project_id
  role    = "roles/storage.objectViewer"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

resource "google_project_iam_binding" "bigquery_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}


resource "google_project_iam_binding" "bigquery_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}
