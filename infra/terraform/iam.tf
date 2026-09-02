data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "alerts" {
  name               = "${var.name}-alerts"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

# Scoped to the single table and the single log group this function uses.
# AWSLambdaBasicExecutionRole would have been one line and would have granted
# log access across the account.
data "aws_iam_policy_document" "alerts" {
  statement {
    sid       = "WriteAlerts"
    actions   = ["dynamodb:PutItem"]
    resources = [aws_dynamodb_table.alerts.arn]
  }

  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.alerts.arn}:*"]
  }

  # Only needed by the self-managed Kafka event source, and only when one is
  # configured — the mapping reads the broker credentials as the function's
  # role, not as the caller's.
  dynamic "statement" {
    for_each = local.kafka_enabled ? [1] : []
    content {
      sid       = "ReadKafkaCredentials"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = [aws_secretsmanager_secret.kafka[0].arn]
    }
  }
}

resource "aws_iam_role_policy" "alerts" {
  name   = "${var.name}-alerts"
  role   = aws_iam_role.alerts.id
  policy = data.aws_iam_policy_document.alerts.json
}
