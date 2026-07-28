output "kafka_cluster_ocid" { value = oci_managed_kafka_kafka_cluster.events.id }
output "kafka_bootstrap_servers" { value = [for listener in oci_managed_kafka_kafka_cluster.events.kafka_bootstrap_urls : listener.url] }
output "kafka_topic_replication_factor" { value = var.kafka_cluster_type == "PRODUCTION" ? 3 : 1 }
output "adb_ocid" { value = oci_database_autonomous_database.write_model.id }
output "adb_service_console_url" { value = oci_database_autonomous_database.write_model.service_console_url }
output "nosql_table" { value = oci_nosql_table.read_model.name }
output "command_application_id" { value = oci_functions_application.command.id }
output "projection_application_id" { value = oci_functions_application.projection.id }
output "gateway_id" { value = oci_apigateway_gateway.public.id }
output "command_application_name" { value = oci_functions_application.command.display_name }
output "projection_application_name" { value = oci_functions_application.projection.display_name }
output "kafka_topic" { value = "${var.project}.${var.environment}.orders.domain.v1" }
output "vault_id" { value = oci_kms_vault.main.id }
