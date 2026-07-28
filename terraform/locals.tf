locals {
  name = "${var.project}-${var.environment}"
  tags = merge(var.tags, {
    ManagedBy   = "Terraform"
    Architecture = "EDA-CQRS"
    Environment = var.environment
  })
}
