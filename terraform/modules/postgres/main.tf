variable "environment" {}
variable "db_host"     {}
variable "db_port"     {}
variable "db_user"     {}
variable "db_password" {}
variable "db_name"     {}

resource "null_resource" "ehr_schema" {
  triggers = {
    environment = var.environment
  }

  provisioner "local-exec" {
    command = <<-CMD
      docker exec postgres psql \
        -U ${var.db_user} \
        -d ${var.db_name} \
        -c "CREATE SCHEMA IF NOT EXISTS ehr_${var.environment};"

      docker exec postgres psql \
        -U ${var.db_user} \
        -d ${var.db_name} \
        -c "CREATE TABLE IF NOT EXISTS ehr_${var.environment}.patients (
              patient_id   SERIAL PRIMARY KEY,
              name         VARCHAR(200) NOT NULL,
              dob          DATE,
              admitted_at  TIMESTAMP DEFAULT NOW(),
              status       VARCHAR(50) DEFAULT 'active'
            );"
    CMD
  }
}
