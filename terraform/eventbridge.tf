module "eventbridge" {
  source = "terraform-aws-modules/eventbridge/aws"

  bus_name = "my-bus"

  tags = {
    Name = "my-bus"
  }
}

module "eventbridge" {
  source = "terraform-aws-modules/eventbridge/aws"

  bus_name = "my-bus"

  create_targets = false

  rules = {
    logs = {
      description   = "Capture log data"
      event_pattern = jsonencode({ "source" : ["my.app.logs"] })
    }
  }
}

module "eventbridge" {
  source = "terraform-aws-modules/eventbridge/aws"

  bus_name = "my-bus"

  rules = {
    logs = {
      description   = "Capture log data"
      event_pattern = jsonencode({ "source" : ["my.app.logs"] })
    }
  }

  targets = {
    logs = [
      {
        name = "send-logs-to-cloudwatch"
        arn  = aws_iam_policy_document.cw_document.arn
      }
    ]
  }
}

module "eventbridge" {
  source = "terraform-aws-modules/eventbridge/aws"

  create_bus = false

  rules = {
    crons = {
      description         = "Trigger for a Lambda"
      schedule_expression = "rate(15 minutes)"
    }
  }

  targets = {
    crons = [
      {
        name  = "lambda-loves-cron"
        arn   = "${aws_iam_role.lambda_1_role.arn}"
        input = jsonencode({"job": "cron-by-rate"})
      }
    ]
  }
}