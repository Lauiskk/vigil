terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.6"
    }
  }
}

# One configuration, two targets.
#
# When localstack_endpoint is set the provider is pointed at it with dummy
# credentials and every account-side validation is skipped, so the same files
# that would provision real infrastructure can be applied against a container
# on a laptop. Keeping one configuration is the point: a separate "local"
# variant would drift from the real one and stop proving anything.
provider "aws" {
  region = var.region

  access_key = var.localstack_endpoint == "" ? null : "test"
  secret_key = var.localstack_endpoint == "" ? null : "test"

  skip_credentials_validation = var.localstack_endpoint != ""
  skip_metadata_api_check     = var.localstack_endpoint != ""
  skip_requesting_account_id  = var.localstack_endpoint != ""

  dynamic "endpoints" {
    for_each = var.localstack_endpoint == "" ? [] : [var.localstack_endpoint]
    content {
      dynamodb       = endpoints.value
      lambda         = endpoints.value
      iam            = endpoints.value
      logs           = endpoints.value
      sts            = endpoints.value
      secretsmanager = endpoints.value
    }
  }

  default_tags {
    tags = {
      Project   = "vigil"
      ManagedBy = "terraform"
    }
  }
}
