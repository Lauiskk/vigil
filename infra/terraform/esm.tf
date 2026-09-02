# The self-managed Kafka event source mapping.
#
# This is the real delivery path and the reason the pipeline needs no bridge in
# AWS: Lambda polls the alert topic itself, batches records, and invokes the
# function. The forwarder in cmd/forwarder exists only because LocalStack's
# free tier does not implement this event source — the handler accepts both
# shapes so the same code serves both.
#
# Nothing here is created unless kafka_brokers is set.

resource "aws_secretsmanager_secret" "kafka" {
  count = local.kafka_enabled ? 1 : 0
  name  = "${var.name}/kafka-scram"
}

resource "aws_secretsmanager_secret_version" "kafka" {
  count     = local.kafka_enabled ? 1 : 0
  secret_id = aws_secretsmanager_secret.kafka[0].id
  secret_string = jsonencode({
    username = var.kafka_user
    password = var.kafka_password
  })
}

resource "aws_lambda_event_source_mapping" "alerts" {
  count = local.kafka_enabled ? 1 : 0

  function_name     = aws_lambda_function.alerts.arn
  topics            = ["vigil.alerts"]
  starting_position = "TRIM_HORIZON"

  self_managed_event_source {
    endpoints = {
      KAFKA_BOOTSTRAP_SERVERS = join(",", var.kafka_brokers)
    }
  }

  source_access_configuration {
    type = "SASL_SCRAM_512_AUTH"
    uri  = aws_secretsmanager_secret.kafka[0].arn
  }

  # Batching is not an optimisation here, it is the cost control that makes
  # the free tier reachable. One invocation per alert at a few alerts a second
  # would exceed a million requests a month; at fifty per batch it is
  # comfortably inside it.
  batch_size                         = 50
  maximum_batching_window_in_seconds = 5
}
