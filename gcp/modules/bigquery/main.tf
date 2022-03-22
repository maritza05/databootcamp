resource "google_bigquery_dataset" "datawarehouse" {
  dataset_id    = var.bigquery_dataset_id
  friendly_name = var.bigquery_friendly_name
  description   = var.bigquery_description
  location      = var.bigquery_location

  labels = {
    env = "default"
  }

  access {
    role          = "OWNER"
    user_by_email = var.user_email
  }
}

resource "google_bigquery_routine" "getseason" {
  dataset_id      = google_bigquery_dataset.datawarehouse.dataset_id
  routine_id      = "getSeason"
  routine_type    = "SCALAR_FUNCTION"
  language        = "JAVASCRIPT"
  definition_body = <<-EOS
  if (month >= 3 && month < 6){
        return "Spring";
    }
    else if(month >= 6 && month < 9){
        return "Summer";
    }
    else if(month >= 9 && month < 12){
        return "Autum";
    }
    return "Winter";
  EOS
  arguments {
    name      = "month"
    data_type = "{\"typeKind\" :  \"INT64\"}"
  }

  return_type = "{\"typeKind\" :  \"STRING\"}"
}
