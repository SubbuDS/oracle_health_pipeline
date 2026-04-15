terraform {
  required_providers {
    null = { source = "hashicorp/null" }
  }
}

module "postgres" {
  source      = "./modules/postgres"
  environment = var.environment
  db_host     = var.db_host
  db_port     = var.db_port
  db_user     = var.db_user
  db_password = var.db_password
  db_name     = var.db_name
}

module "kafka" {
  source       = "./modules/kafka"
  environment  = var.environment
  kafka_broker = var.kafka_broker
}

module "minio" {
  source           = "./modules/minio"
  environment      = var.environment
  minio_endpoint   = var.minio_endpoint
  minio_access_key = var.minio_access_key
  minio_secret_key = var.minio_secret_key
}
