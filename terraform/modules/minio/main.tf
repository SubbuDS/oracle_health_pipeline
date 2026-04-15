variable "environment" {}
variable "minio_endpoint"   {}
variable "minio_access_key" {}
variable "minio_secret_key" {}

resource "null_resource" "ehr_bucket" {
  triggers = {
    environment = var.environment
  }

  provisioner "local-exec" {
    command = "docker run --rm -e MC_HOST_minio=http://${var.minio_access_key}:${var.minio_secret_key}@host.docker.internal:9000 minio/mc mb --ignore-existing minio/ehr-${var.environment}"
  }
}
