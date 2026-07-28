resource "oci_kms_vault" "main" {
  compartment_id = var.compartment_ocid
  display_name   = "${local.name}-vault"
  vault_type     = "DEFAULT"
  freeform_tags  = local.tags
}

resource "oci_kms_key" "events" {
  management_endpoint = oci_kms_vault.main.management_endpoint
  display_name        = "${local.name}-events-key"
  protection_mode     = "HSM"
  key_shape { algorithm = "AES", length = 32 }
  freeform_tags = local.tags
}

resource "oci_managed_kafka_kafka_cluster_config" "events" {
  compartment_id = var.compartment_ocid
  display_name   = "${local.name}-kafka-config"
  latest_config {
    # Tópicos são criados pelo admin client; desabilitar criação acidental evita typo virar tópico de produção.
    properties = {
      "auto.create.topics.enable" = "false"
      "num.partitions"            = tostring(var.kafka_topic_partitions)
      "default.replication.factor" = tostring(var.kafka_cluster_type == "PRODUCTION" ? 3 : 1)
      "min.insync.replicas"        = tostring(var.kafka_cluster_type == "PRODUCTION" ? 2 : 1)
      "unclean.leader.election.enable" = "false"
    }
  }
  freeform_tags = local.tags
}

resource "oci_managed_kafka_kafka_cluster" "events" {
  compartment_id         = var.compartment_ocid
  display_name           = "${local.name}-kafka"
  cluster_type           = var.kafka_cluster_type
  kafka_version          = var.kafka_version
  coordination_type      = var.kafka_coordination_type
  cluster_config_id      = oci_managed_kafka_kafka_cluster_config.events.id
  cluster_config_version = oci_managed_kafka_kafka_cluster_config.events.latest_config[0].version_number
  access_subnets { subnets = [oci_core_subnet.kafka.id] }
  broker_shape {
    node_count           = var.kafka_broker_node_count
    node_shape           = var.kafka_broker_node_shape
    ocpu_count           = var.kafka_broker_ocpu_count
    storage_size_in_gbs  = var.kafka_broker_storage_gbs
  }
  freeform_tags = local.tags
}

resource "oci_database_autonomous_database" "write_model" {
  compartment_id           = var.compartment_ocid
  db_name                  = replace(substr(local.name, 0, 14), "-", "")
  display_name             = "${local.name}-write"
  admin_password           = var.adb_admin_password
  db_workload              = "OLTP"
  subnet_id                = oci_core_subnet.functions.id
  private_endpoint_label   = "${replace(local.name, "-", "")}-adb"
  is_free_tier             = false
  cpu_core_count           = var.adb_cpu_core_count
  data_storage_size_in_tbs = 1
  license_model            = "LICENSE_INCLUDED"
  freeform_tags            = local.tags
}

resource "oci_nosql_table" "read_model" {
  compartment_id    = var.compartment_ocid
  name              = "${replace(local.name, "-", "_")}_read_model"
  ddl_statement     = "CREATE TABLE ${replace(local.name, "-", "_")}_read_model (id STRING, entityType STRING, version LONG, eventId STRING, payload JSON, updatedAt STRING, PRIMARY KEY (SHARD(id), entityType))"
  is_multi_region   = false
  freeform_tags     = local.tags
}

resource "oci_objectstorage_bucket" "archive" {
  compartment_id = var.compartment_ocid
  namespace      = data.oci_objectstorage_namespace.current.namespace
  name           = "${local.name}-event-archive"
  access_type    = "NoPublicAccess"
  storage_tier   = "Standard"
  versioning     = "Enabled"
  freeform_tags  = local.tags
}

data "oci_objectstorage_namespace" "current" { compartment_id = var.compartment_ocid }

resource "oci_logging_log_group" "main" {
  compartment_id = var.compartment_ocid
  display_name   = "${local.name}-logs"
  freeform_tags  = local.tags
}

resource "oci_functions_application" "command" {
  compartment_id = var.compartment_ocid
  display_name   = "${local.name}-command"
  subnet_ids     = [oci_core_subnet.functions.id]
  network_security_group_ids = [oci_core_network_security_group.functions.id]
  config = { ENVIRONMENT = var.environment }
  freeform_tags = local.tags
}

resource "oci_functions_application" "projection" {
  compartment_id = var.compartment_ocid
  display_name   = "${local.name}-projection"
  subnet_ids     = [oci_core_subnet.functions.id]
  network_security_group_ids = [oci_core_network_security_group.functions.id]
  config = { ENVIRONMENT = var.environment }
  freeform_tags = local.tags
}
