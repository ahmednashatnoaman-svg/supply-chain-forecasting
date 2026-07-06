# ============================================================================
# COST WARNING: This Terraform is a MANIFEST-ONLY stub. Do NOT `terraform apply`
# it against a billed cloud provider — that would violate the project's
# zero-cost rule (docs/reference/cost-and-licensing.md). It documents an
# equivalent managed topology for portability only. For a free cluster, use
# kind/minikube locally and the Helm chart in ../helm.
# ============================================================================

terraform {
  required_version = ">= 1.5.0"
  # No provider is configured on purpose, so `apply` cannot provision paid resources by accident.
}

# Example (commented) of what a managed layout WOULD look like — intentionally inert.
# module "kafka" {
#   source   = "./modules/kafka"
#   brokers  = var.kafka_brokers
# }
# module "spark" {
#   source   = "./modules/spark"
#   workers  = var.spark_workers
# }

output "note" {
  value = "Manifests only. Deploy locally with Helm on kind/minikube to stay at $0."
}
