variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Prefix for every resource."
  type        = string
  default     = "vigil"
}

variable "localstack_endpoint" {
  description = <<-EOT
    Point the provider at LocalStack instead of AWS, e.g. http://localhost:4566.
    Empty means real AWS.
  EOT
  type        = string
  default     = ""
}

variable "lambda_binary" {
  description = "Path to the compiled handler. Build it with `task lambda:build`."
  type        = string
  default     = "../../dist/bootstrap"
}

variable "kafka_brokers" {
  description = "Seed brokers for the self-managed Kafka event source."
  type        = list(string)
  default     = []
}

variable "kafka_user" {
  description = "SASL/SCRAM username for the event source mapping."
  type        = string
  default     = ""
}

variable "kafka_password" {
  description = "SASL/SCRAM password for the event source mapping."
  type        = string
  default     = ""
  sensitive   = true
}

variable "reserved_concurrency" {
  description = <<-EOT
    Hard ceiling on concurrent executions.

    This is a cost control, not a performance tuning knob. Lambda's free
    allowance is a million requests a month, which is 0.39 invocations per
    second sustained; an unbounded function fed by a surging Kafka topic can
    cross that in an afternoon. Five is generous for this workload and bounds
    the damage from a redelivery loop.
  EOT
  type        = number
  default     = 5
}

variable "log_retention_days" {
  description = "CloudWatch retention. Short on purpose: logs are the line item that surprises people."
  type        = number
  default     = 3
}

variable "monthly_budget_usd" {
  description = "Budget alarm threshold."
  type        = number
  default     = 1
}

variable "budget_alert_email" {
  description = "Where the budget alarm goes. Empty disables the budget."
  type        = string
  default     = ""
}
