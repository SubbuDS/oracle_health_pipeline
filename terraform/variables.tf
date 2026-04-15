variable "environment" {
  description = "Environment name: dev or prod"
  type        = string
}

variable "db_host"     { default = "localhost" }
variable "db_port"     { default = 5432 }
variable "db_user"     { default = "demo" }
variable "db_password" { default = "demo" }
variable "db_name"     { default = "ehr_db" }

variable "kafka_broker" { default = "localhost:29092" }

variable "minio_endpoint"   { default = "http://localhost:9000" }
variable "minio_access_key" { default = "minioadmin" }
variable "minio_secret_key" { default = "minioadmin" }
