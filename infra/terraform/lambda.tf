locals {
  kafka_enabled = length(var.kafka_brokers) > 0
}

# The handler is a static Go binary named `bootstrap`, which is what the
# provided.al2023 runtime executes. Built by `task lambda:build`.
data "archive_file" "alerts" {
  type        = "zip"
  source_file = var.lambda_binary
  output_path = "${path.module}/.terraform-artifacts/alerts.zip"
}

# Created explicitly rather than left to Lambda, so retention is set from the
# start. A log group Lambda creates for itself retains forever, and on a free
# tier that is the line item that eventually costs money.
resource "aws_cloudwatch_log_group" "alerts" {
  name              = "/aws/lambda/${var.name}-alerts"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "alerts" {
  function_name = "${var.name}-alerts"
  role          = aws_iam_role.alerts.arn

  filename         = data.archive_file.alerts.output_path
  source_code_hash = data.archive_file.alerts.output_base64sha256

  runtime       = "provided.al2023"
  handler       = "bootstrap"
  architectures = ["arm64"] # cheaper per millisecond than x86_64

  memory_size = 128
  timeout     = 20

  reserved_concurrent_executions = var.reserved_concurrency

  environment {
    variables = {
      ALERTS_TABLE = aws_dynamodb_table.alerts.name
    }
  }

  depends_on = [
    aws_iam_role_policy.alerts,
    aws_cloudwatch_log_group.alerts,
  ]
}
