variable "project_id" {
  description = "project id"
}
variable "location" {
  description = "location"
}

variable "region" {
  description = "region"
}


#GKE
variable "gke_num_nodes" {
  default     = 2
  description = "number of gke nodes"
}

variable "machine_type" {
  type    = string
  default = "n1-standard-1"
}

# CloudSQL
variable "instance_name" {
  description = "Name for the sql instance database"
  default     = "data-bootcamp"
}

variable "database_version" {
  description = "The MySQL, PostgreSQL or SQL Server (beta) version to use. "
  default     = "POSTGRES_12"
}

variable "instance_tier" {
  description = "Sql instance tier"
  default     = "db-f1-micro"
}

variable "disk_space" {
  description = "Size of the disk in the sql instance"
  default     = 10
}

variable "database_name" {
  description = "Name for the database to be created"
  default     = "dbname"
}

variable "db_username" {
  description = "Username credentials for root user"
  default     = "dbuser"
}
variable "db_password" {
  description = "Password credentials for root user"
  default     = "dbpassword"
}

# Cloud storage
variable "raw_layer_bucket" {}
variable "staging_layer_bucket" {}
variable "dataproc_scripts" {}
variable "movies_reviews_script" {}
variable "logs_reviews_script" {}
variable "dataproc_temp" {}

# User account 
variable "service_account_name" {}
variable "user_email" {}

# Bigquery
variable "bigquery_dataset_id" {}
variable "bigquery_friendly_name" {}
variable "bigquery_description" {}
variable "bigquery_location" {}
