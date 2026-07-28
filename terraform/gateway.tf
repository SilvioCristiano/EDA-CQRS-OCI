resource "oci_apigateway_gateway" "public" {
  compartment_id = var.compartment_ocid
  display_name   = "${local.name}-gateway"
  endpoint_type  = "PRIVATE"
  subnet_id      = oci_core_subnet.gateway.id
  network_security_group_ids = [oci_core_network_security_group.gateway.id]
  freeform_tags  = local.tags
}
