module "vpc" {
  source = "./modules/vpc"

  project_id = var.project_id

}

module "gke" {
  source = "./modules/gke"

  project_id    = var.project_id
  cluster_name  = "airflow-gke-data-bootcamp"
  location      = var.location
  vpc_id        = module.vpc.vpc
  subnet_id     = module.vpc.private_subnets[0]
  gke_num_nodes = var.gke_num_nodes
  machine_type  = var.machine_type

}

module "cloudsql" {
  source = "./modules/cloudsql"

  region           = var.region
  location         = var.location
  instance_name    = var.instance_name
  database_version = var.database_version
  instance_tier    = var.instance_tier
  disk_space       = var.disk_space
  database_name    = var.database_name
  db_username      = var.db_username
  db_password      = var.db_password
}

module "cloudstorage" {
  source     = "./modules/cloud-storage"
  region     = var.region
  project_id = var.project_id

  raw_layer_bucket      = var.raw_layer_bucket
  staging_layer_bucket  = var.staging_layer_bucket
  dataproc_scripts      = var.dataproc_scripts
  dataproc_temp         = var.dataproc_temp
  movies_reviews_script = var.movies_reviews_script
  logs_reviews_script   = var.logs_reviews_script
}

module "service-account" {
  source     = "./modules/service-account"
  project_id = var.project_id

  user_email           = var.user_email
  service_account_name = var.service_account_name
}

module "bigquery" {
  source = "./modules/bigquery"

  bigquery_dataset_id    = var.bigquery_dataset_id
  bigquery_friendly_name = var.bigquery_friendly_name
  bigquery_description   = var.bigquery_description
  bigquery_location      = var.bigquery_location
  user_email             = var.user_email
}
