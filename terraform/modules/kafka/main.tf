variable "environment" {}
variable "kafka_broker" {}

resource "null_resource" "ehr_topic" {
  triggers = {
    environment = var.environment
  }

  provisioner "local-exec" {
    command = <<-CMD
      docker exec kafka kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create \
        --if-not-exists \
        --topic ehr.${var.environment}.patients \
        --partitions 3 \
        --replication-factor 1
    CMD
  }
}
