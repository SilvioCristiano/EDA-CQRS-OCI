data "oci_core_services" "all" {}

resource "oci_core_vcn" "main" {
  compartment_id = var.compartment_ocid
  display_name   = "${local.name}-vcn"
  cidr_blocks    = ["10.20.0.0/16"]
  dns_label      = replace(local.name, "-", "")
  freeform_tags  = local.tags
}

resource "oci_core_service_gateway" "main" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${local.name}-sgw"
  services { service_id = data.oci_core_services.all.services[0].id }
  freeform_tags = local.tags
}

resource "oci_core_route_table" "private" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${local.name}-private-rt"
  route_rules {
    network_entity_id = oci_core_service_gateway.main.id
    destination       = data.oci_core_services.all.services[0].cidr_block
    destination_type  = "SERVICE_CIDR_BLOCK"
  }
  freeform_tags = local.tags
}

resource "oci_core_network_security_group" "functions" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${local.name}-functions-nsg"
  freeform_tags  = local.tags
}

resource "oci_core_network_security_group_security_rule" "functions_egress" {
  network_security_group_id = oci_core_network_security_group.functions.id
  direction                 = "EGRESS"
  protocol                  = "6"
  destination               = "10.20.0.0/16"
  destination_type          = "CIDR_BLOCK"
  description               = "Somente workloads privados da VCN; portas são controladas por NSGs de destino."
}

resource "oci_core_network_security_group" "gateway" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${local.name}-gateway-nsg"
  freeform_tags  = local.tags
}

resource "oci_core_network_security_group_security_rule" "gateway_ingress_vcn" {
  network_security_group_id = oci_core_network_security_group.gateway.id
  direction                 = "INGRESS"
  protocol                  = "6"
  source                    = "10.20.0.0/16"
  source_type               = "CIDR_BLOCK"
  tcp_options { destination_port_range { min = 443, max = 443 } }
  description = "API privada somente a partir da VCN/VPN conectada."
}

resource "oci_core_subnet" "functions" {
  compartment_id             = var.compartment_ocid
  vcn_id                     = oci_core_vcn.main.id
  display_name               = "${local.name}-functions-private"
  cidr_block                 = "10.20.10.0/24"
  prohibit_public_ip_on_vnic = true
  route_table_id             = oci_core_route_table.private.id
  security_list_ids          = []
  freeform_tags              = local.tags
}

resource "oci_core_subnet" "kafka" {
  compartment_id             = var.compartment_ocid
  vcn_id                     = oci_core_vcn.main.id
  display_name               = "${local.name}-kafka-private"
  cidr_block                 = "10.20.20.0/24"
  prohibit_public_ip_on_vnic = true
  route_table_id             = oci_core_route_table.private.id
  security_list_ids          = []
  freeform_tags              = local.tags
}

resource "oci_core_subnet" "gateway" {
  compartment_id             = var.compartment_ocid
  vcn_id                     = oci_core_vcn.main.id
  display_name               = "${local.name}-gateway-private"
  cidr_block                 = "10.20.30.0/24"
  prohibit_public_ip_on_vnic = true
  route_table_id             = oci_core_route_table.private.id
  security_list_ids          = []
  freeform_tags              = local.tags
}
