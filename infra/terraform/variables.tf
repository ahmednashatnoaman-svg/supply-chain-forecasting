# Variables for the manifest-only stub. Defaults are minimal/free-tier sized.
variable "kafka_brokers" {
  type        = number
  default     = 1
  description = "Broker count (1 for local/free)."
}

variable "spark_workers" {
  type        = number
  default     = 2
  description = "Spark worker count."
}

variable "environment" {
  type        = string
  default     = "local"
  description = "Deployment environment. Keep 'local' to stay zero-cost."
}
