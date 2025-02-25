module "eventbridge_bus" {
  source  = "terraform-aws-modules/eventbridge/aws"
  bus_name = "my-bus"
}

module "eventbridge_rules" {
  source  = "terraform-aws-modules/eventbridge/aws"
  
  bus_name = module.eventbridge_bus.eventbridge_bus_name
  create_bus = false  # Bus is created by `eventbridge_bus`

  rules = {
    logs = {
      description   = "Capture log data"
      event_pattern = jsonencode({ "source" : ["my.app.logs"] })
    }
    crons = {
      description         = "Trigger for a Lambda"
      schedule_expression = "rate(15 minutes)"
    }
  }
}

module "eventbridge_targets" {
  source  = "terraform-aws-modules/eventbridge/aws"
  
  bus_name = module.eventbridge_bus.eventbridge_bus_name
  create_bus = false  

  targets = {
    logs = [
      {
        name = "send-logs-to-cloudwatch"
        arn  = aws_cloudwatch_log_group.log_group.arn 
      }
    ]
    crons = [
      {
        name  = "lambda-loves-cron"
        arn   = aws_lambda_function.my_lambda.arn  
        input = jsonencode({"job": "cron-by-rate"})
      }
    ]
  }
}
