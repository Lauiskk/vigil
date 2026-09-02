output "function_name" {
  description = "Lambda function name, for `aws lambda invoke`."
  value       = aws_lambda_function.alerts.function_name
}

output "table_name" {
  description = "DynamoDB table holding the alerts."
  value       = aws_dynamodb_table.alerts.name
}

output "log_group" {
  description = "CloudWatch log group for the handler."
  value       = aws_cloudwatch_log_group.alerts.name
}

output "event_source_enabled" {
  description = "Whether Lambda polls Kafka directly, or the forwarder bridges it."
  value       = local.kafka_enabled
}
