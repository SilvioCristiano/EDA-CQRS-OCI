variable "tenancy_ocid" {
  type = string
}
variable "compartment_ocid" {
  type = string
}
variable "region" {
  type = string
}
variable "api_jwt_issuer" { type = string }
variable "api_jwt_audience" { type = string }
variable "api_jwks_uri" { type = string }
variable "project" {
  type    = string
  default = "edacqrs"
}
variable "environment" {
  type    = string
  default = "dev"
  validation {
    condition     = contains(["dev", "hml", "prd"], var.environment)
    error_message = "environment deve ser dev, hml ou prd."
  }
}
variable "adb_admin_password" {
  type      = string
  sensitive = true
}
variable "adb_cpu_core_count" {
  type    = number
  default = 1
}
variable "kafka_version" {
  type        = string
  description = "Versão Kafka disponível na região; obtenha-a com OCI Console/CLI antes do apply."
}
variable "kafka_coordination_type" {
  type        = string
  description = "Tipo aceito pela versão Kafka escolhida, por exemplo KRAFT."
}
variable "kafka_cluster_type" {
  type    = string
  default = "PRODUCTION"
  validation {
    condition     = contains(["DEVELOPMENT", "PRODUCTION"], var.kafka_cluster_type)
    error_message = "kafka_cluster_type deve ser DEVELOPMENT ou PRODUCTION."
  }
}
variable "kafka_broker_node_count" {
  type    = number
  default = 3
}
variable "kafka_broker_ocpu_count" {
  type    = number
  default = 1
}
variable "kafka_broker_storage_gbs" {
  type    = number
  default = 500
}
variable "kafka_broker_node_shape" {
  type    = string
  default = "VM.Standard.E5.Flex"
}
variable "kafka_topic_partitions" {
  type    = number
  default = 6
}
variable "kafka_tls_port" {
  type    = number
  default = 9092
}
variable "tags" {
  type    = map(string)
  default = {}
}
