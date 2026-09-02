# Alerts, keyed so that "everything that happened to this entity, newest
# first" is a query rather than a scan.
resource "aws_dynamodb_table" "alerts" {
  name         = "${var.name}-alerts"
  billing_mode = "PAY_PER_REQUEST" # no idle cost, and no capacity to forecast

  hash_key  = "pk"
  range_key = "sk"

  attribute {
    name = "pk" # ALERT#<stream>#<entity>
    type = "S"
  }

  attribute {
    name = "sk" # <detectedAt>#<alertId>
    type = "S"
  }

  # Rows expire themselves. A scheduled cleanup would consume read and write
  # capacity to do what DynamoDB does for nothing.
  ttl {
    attribute_name = "expiresAt"
    enabled        = true
  }

  point_in_time_recovery {
    # Off deliberately: these rows are derived from a Kafka topic and can be
    # rebuilt by replaying it. Paying for continuous backups of reproducible
    # data is a cost with no matching risk.
    enabled = false
  }
}
